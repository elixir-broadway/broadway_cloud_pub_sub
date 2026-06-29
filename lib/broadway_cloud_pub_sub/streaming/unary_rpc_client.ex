defmodule BroadwayCloudPubSub.Streaming.UnaryRpcClient do
  @moduledoc false

  # GenServer that owns a dedicated gRPC channel for unary Acknowledge and
  # ModifyAckDeadline RPCs.
  #
  # Acks and modacks are delivered independently of the StreamingPull stream state —
  # if the stream is reconnecting, acks can still be sent and will succeed. A
  # separate channel avoids HOL-blocking the message stream with ack traffic.
  #
  # ## Error handling
  #
  # Each RPC is attempted exactly once on the current channel:
  #
  #   - Success            → return :ok, emit telemetry
  #   - Retryable error    → schedule async channel reconnect via send(self(), :reconnect),
  #                          return {:error, reason}. The caller (AckBatcher) retains the
  #                          ack_ids and retries on the next flush timer tick.
  #   - Terminal error     → stop the GenServer via {:stop, reason, reply, state} so the
  #                          supervisor restarts it fresh. The caller receives {:error, reason}
  #                          first (from the reply in the stop tuple) so it can retain ack_ids.
  #
  # Retry timing and back-pressure belong to the caller (AckBatcher), not here.
  #
  # ## API
  #
  # acknowledge/2 and modify_ack_deadline/3 are synchronous calls that return
  # {:ok, remaining_ack_ids} on completion. remaining_ack_ids is the list of ack_ids
  # that were NOT successfully delivered (empty list on full success). On a hard
  # process error (noproc, timeout) they return {:error, reason}.

  use GenServer

  alias BroadwayCloudPubSub.Backoff
  alias BroadwayCloudPubSub.Streaming.{AckResult, ErrorClassifier, Options, Telemetry}
  alias Google.Pubsub.V1.{AcknowledgeRequest, ModifyAckDeadlineRequest}

  require Logger

  @max_ack_ids_per_request 2_500

  defstruct [
    :config,
    :grpc_client,
    :grpc_client_config,
    :channel,
    :backoff,
    # True when a :reconnect message is already queued in the mailbox.
    # Prevents multiple concurrent reconnect attempts from stacking up.
    reconnect_pending: false
  ]

  # --- Public API ---

  @all_keys [
    :subscription,
    :grpc_client,
    :grpc_client_config,
    :backoff_type,
    :backoff_min,
    :backoff_max,
    :broadway_name,
    :telemetry_metadata
  ]

  @required_keys [
    :subscription,
    :grpc_client,
    :grpc_client_config,
    :backoff_type,
    :backoff_min,
    :backoff_max,
    :broadway_name
  ]

  @doc false
  @spec child_opts(keyword()) :: keyword()
  def child_opts(opts) do
    Options.validate_child_opts(opts, @all_keys, @required_keys)
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)

    if name do
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      GenServer.start_link(__MODULE__, opts)
    end
  end

  @doc """
  Sends acknowledge requests for the given ack_ids.

  Large lists are chunked to @max_ack_ids_per_request. Returns
  {:ok, remaining_ack_ids} where remaining_ack_ids is the list of ack_ids
  that could not be delivered (empty on full success). Returns {:error, reason}
  only on a hard process failure (noproc, timeout, etc.).
  """
  @spec acknowledge(GenServer.server(), [String.t()]) :: {:ok, [String.t()]} | {:error, term()}
  def acknowledge(pid, ack_ids) when is_list(ack_ids) do
    chunked_rpc(pid, ack_ids, fn chunk ->
      GenServer.call(pid, {:acknowledge, chunk}, 30_000)
    end)
  end

  @doc """
  Sends modifyAckDeadline requests for the given ack_ids and deadline.

  Same chunking and return semantics as acknowledge/2.
  """
  @spec modify_ack_deadline(GenServer.server(), [String.t()], non_neg_integer()) ::
          {:ok, [String.t()]} | {:error, term()}
  def modify_ack_deadline(pid, ack_ids, deadline_seconds) when is_list(ack_ids) do
    chunked_rpc(pid, ack_ids, fn chunk ->
      GenServer.call(pid, {:modify_ack_deadline, chunk, deadline_seconds}, 30_000)
    end)
  end

  # --- GenServer callbacks ---

  @impl GenServer
  def init(opts) do
    # Trap exits so that when the Mint/Gun ConnectionProcess linked by
    # GRPC.Stub.connect exits normally (e.g. on disconnect/shutdown), the
    # {:EXIT, pid, :normal} signal is delivered as a handle_info message
    # instead of killing this GenServer.
    Process.flag(:trap_exit, true)
    config = Map.new(opts)

    backoff =
      Backoff.new(
        type: config.backoff_type,
        min: config.backoff_min,
        max: config.backoff_max
      )

    state = %__MODULE__{
      config: config,
      grpc_client: config.grpc_client,
      grpc_client_config: config.grpc_client_config,
      backoff: backoff
    }

    # Open initial channel immediately.
    case state.grpc_client.connect(state.grpc_client_config) do
      {:ok, channel} ->
        {:ok, %{state | channel: channel}}

      {:error, reason} ->
        emit_telemetry(:connection_failure, %{}, config, %{reason: reason})
        {:ok, state}
    end
  end

  @impl GenServer
  def handle_call({:acknowledge, ack_ids}, _from, state) do
    request = %AcknowledgeRequest{
      subscription: state.config.subscription,
      ack_ids: ack_ids
    }

    grpc_client = state.grpc_client
    do_rpc(state, ack_ids, request, &grpc_client.acknowledge/3, "acknowledge messages")
  end

  def handle_call({:modify_ack_deadline, ack_ids, deadline_seconds}, _from, state) do
    request = %ModifyAckDeadlineRequest{
      subscription: state.config.subscription,
      ack_ids: ack_ids,
      ack_deadline_seconds: deadline_seconds
    }

    grpc_client = state.grpc_client

    do_rpc(
      state,
      ack_ids,
      request,
      &grpc_client.modify_ack_deadline/3,
      "modify ack deadline for messages"
    )
  end

  @impl GenServer
  def handle_info(:reconnect, state) do
    # Clear the pending flag before attempting so new errors during reconnect
    # can queue a fresh :reconnect if needed.
    state = %{state | reconnect_pending: false}
    state = disconnect_channel(state)

    case state.grpc_client.connect(state.grpc_client_config) do
      {:ok, channel} ->
        emit_telemetry(:connect, %{}, state.config)
        backoff = Backoff.reset(state.backoff)
        {:noreply, %{state | channel: channel, backoff: backoff}}

      {:error, reason} ->
        emit_telemetry(:connection_failure, %{}, state.config, %{reason: reason})
        {delay, new_backoff} = Backoff.backoff(state.backoff)
        Process.send_after(self(), :reconnect, delay || state.config.backoff_min)
        {:noreply, %{state | channel: nil, backoff: new_backoff, reconnect_pending: true}}
    end
  end

  # The Mint/Gun ConnectionProcess is linked to this GenServer (trap_exit in init/1).
  # :normal = clean disconnect; tear down the channel so ensure_channel/1 reopens it.
  # other   = unexpected crash; schedule a reconnect.
  def handle_info({:EXIT, pid, :normal}, state) do
    if channel_conn_pid(state) == pid do
      {:noreply, disconnect_channel(state)}
    else
      {:noreply, state}
    end
  end

  def handle_info({:EXIT, pid, _reason}, state) do
    if channel_conn_pid(state) == pid do
      state = state |> disconnect_channel() |> schedule_reconnect()
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Resolves the held channel's owning pid via the configured grpc_client.
  # Returns nil if no channel is held (so an EXIT signal can never match) or
  # the client reports no pid for this channel.
  defp channel_conn_pid(%{channel: nil}), do: nil

  defp channel_conn_pid(%{channel: channel, grpc_client: grpc_client}) do
    grpc_client.connection_pid(channel)
  end

  @impl GenServer
  def terminate(_reason, %{channel: channel} = state) when not is_nil(channel) do
    state.grpc_client.disconnect(channel, state.grpc_client_config)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  # --- Private ---

  # Chunks ack_ids into batches of @max_ack_ids_per_request and calls `call_fn`
  # for each chunk. Accumulates failed chunks. Stops on hard process errors.
  defp chunked_rpc(_pid, ack_ids, call_fn) do
    result =
      ack_ids
      |> Enum.chunk_every(@max_ack_ids_per_request)
      |> Enum.reduce({:ok, []}, fn
        chunk, {:ok, failed_so_far} ->
          case call_fn.(chunk) do
            :ok -> {:ok, failed_so_far}
            {:error, _reason} -> {:ok, [chunk | failed_so_far]}
          end

        _chunk, {:error, _} = err ->
          # Hard process error — don't attempt remaining chunks
          err
      end)

    case result do
      {:ok, failed} -> {:ok, List.flatten(failed)}
      error -> error
    end
  catch
    :exit, reason ->
      {:error, {:call_failed, reason}}
  end

  # Shared RPC execution: ensure channel, call the given rpc_fn, classify errors.
  # `rpc_fn` is a 3-arity function (channel, request, grpc_client_config).
  # `operation` is a human-readable string for Logger.error messages.
  defp do_rpc(state, ack_ids, request, rpc_fn, operation) do
    state = ensure_channel(state)

    case state.channel do
      nil ->
        {:reply, {:error, :no_channel}, state}

      channel ->
        case rpc_fn.(channel, request, state.grpc_client_config) do
          {:ok, _} ->
            {:reply, :ok, state}

          {:error, error} ->
            handle_rpc_error(state, ack_ids, error, operation)
        end
    end
  end

  defp handle_rpc_error(state, ack_ids, error, operation) do
    case ErrorClassifier.classify(error) do
      :retryable ->
        # For exactly-once subscriptions, retryable RPC errors may embed
        # per-ack-ID permanent failures in error details. Permanent ids
        # are dropped; transient ones are returned to AckBatcher for retry.
        per_ack_errors = AckResult.parse_error_details(error_details(error))
        {transient_ids, permanent_ids} = split_by_ack_result(ack_ids, per_ack_errors)

        if permanent_ids != [] do
          emit_telemetry(:permanent_failure, %{count: length(permanent_ids)}, state.config)
        end

        state = schedule_reconnect(state)
        {:reply, {:error, {error, transient_ids}}, state}

      :terminal ->
        Logger.error(
          "Unable to #{operation} with Cloud Pub/Sub via gRPC - reason: #{inspect(error)}"
        )

        # Reply first so caller can retain ack_ids, then stop so supervisor restarts fresh.
        {:stop, {:terminal_error, error}, {:error, error}, state}
    end
  end

  defp ensure_channel(%{channel: nil} = state) do
    case state.grpc_client.connect(state.grpc_client_config) do
      {:ok, channel} -> %{state | channel: channel}
      {:error, _} -> state
    end
  end

  defp ensure_channel(state), do: state

  defp schedule_reconnect(%{channel: nil} = state), do: state
  # Skip if a :reconnect is already queued — prevents churn when multiple
  # retryable RPC errors arrive before :reconnect is processed.
  defp schedule_reconnect(%{reconnect_pending: true} = state), do: state

  defp schedule_reconnect(state) do
    send(self(), :reconnect)
    %{state | reconnect_pending: true}
  end

  defp disconnect_channel(%{channel: nil} = state), do: state

  defp disconnect_channel(%{channel: channel} = state) do
    state.grpc_client.disconnect(channel, state.grpc_client_config)
    %{state | channel: nil}
  end

  defp emit_telemetry(event, measurements, config, extra_metadata \\ %{}) do
    metadata =
      Map.merge(
        %{
          name: config.broadway_name,
          subscription: config.subscription
        },
        extra_metadata
      )

    Telemetry.execute(:unary, event, measurements, metadata, Map.get(config, :telemetry_metadata))
  end

  # Extract per-ack-ID details only when the error is a structured map-like
  # value (GRPC.RPCError, Google Status, etc.). String/atom/tuple errors from
  # transport-level failures (e.g. Mint's "the connection is closed") have no
  # per-ID details to recover; treat them as a single transient failure for
  # the entire batch.
  defp error_details(%{details: details}), do: details
  defp error_details(_), do: nil

  # Splits ack_ids into {transient, permanent} based on per-ack-ID error details
  # parsed from the gRPC error. If there are no per-ack-ID details (the common
  # case), all ids are treated as transient so the caller retries them all.
  defp split_by_ack_result(ack_ids, per_ack_errors) when map_size(per_ack_errors) == 0 do
    {ack_ids, []}
  end

  defp split_by_ack_result(ack_ids, per_ack_errors) do
    {transient, permanent} =
      Enum.reduce(ack_ids, {[], []}, fn ack_id, {t, p} ->
        case Map.get(per_ack_errors, ack_id) do
          {:permanent, _reason} -> {t, [ack_id | p]}
          _ -> {[ack_id | t], p}
        end
      end)

    {Enum.reverse(transient), Enum.reverse(permanent)}
  end
end
