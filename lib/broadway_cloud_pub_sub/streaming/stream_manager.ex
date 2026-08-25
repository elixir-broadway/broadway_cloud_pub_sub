defmodule BroadwayCloudPubSub.Streaming.StreamManager do
  @moduledoc false

  # GenServer that owns the gRPC bidirectional StreamingPull connection.
  # Manages connection lifecycle, message dispatch with demand-based backpressure,
  # lease extension, keep-alive pings, and graceful drain on shutdown.
  # See decisions.md for design rationale.

  use GenServer
  require Logger

  alias BroadwayCloudPubSub.Backoff

  alias BroadwayCloudPubSub.Streaming.{
    AckBatcher,
    AckTimeDistribution,
    ErrorClassifier,
    LeaseManager,
    MessageDispatch,
    StreamReader,
    Telemetry
  }

  alias BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullRequest

  # The server's inactivity timeout is ~60s; pinging at half that prevents closure.
  @default_keepalive_ms 30_000

  @default_drain_timeout_ms 30_000

  # Exactly-once delivery requires a longer retry window to handle server-side transient failures.
  @exactly_once_retry_deadline_ms 600_000

  defstruct [
    :producer_pid,
    :config,
    :grpc_client,
    :grpc_client_config,
    :channel,
    :grpc_stream,
    :conn_pid,
    # Pid of the linked StreamReader process.
    :reader_pid,
    :backoff,
    :lease_timer,
    # Tracks message processing times for the adaptive p99 ack deadline.
    :ack_time_dist,
    # Non-nil when a reconnect is already scheduled — prevents double-scheduling.
    :reconnect_ref,
    :keepalive_timer,
    # Registered name (not PID) so we survive UnaryAckSupervisor restarts.
    :ack_batcher,
    draining: false,
    drain_timer: nil,
    drain_started_at: nil,
    ordering_enabled: false,
    # Updated from StreamingPullResponse.subscription_properties.
    exactly_once_enabled: false,
    # ack_id => %{received_at: monotonic_ms, max_expiry: monotonic_ms}
    outstanding: %{},
    # Buffered messages waiting for producer demand. Bounded by max_outstanding_messages.
    message_buffer: :queue.new(),
    pending_demand: 0,
    # In-flight receipt modack RPCs for exactly-once delivery.
    # ref => %{broadway_messages, ack_ids, received_at}. See decisions.md.
    pending_receipt_modacks: %{}
  ]

  # --- Public API ---

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
  Acknowledge (ack) a list of ack_ids. Called by StreamingAcknowledger.
  """
  @spec acknowledge(pid(), [String.t()]) :: :ok
  def acknowledge(pid, ack_ids) when is_list(ack_ids) do
    GenServer.cast(pid, {:acknowledge, ack_ids})
  end

  @doc """
  Modify ack deadline for a list of ack_ids. Used for nack and lease extension.
  """
  @spec modify_deadline(pid(), [String.t()], non_neg_integer()) :: :ok
  def modify_deadline(pid, ack_ids, deadline_seconds) when is_list(ack_ids) do
    GenServer.cast(pid, {:modify_deadline, ack_ids, deadline_seconds})
  end

  @doc """
  Prepares the StreamManager for graceful shutdown. Called from the
  producer's `prepare_for_draining/1`. Atomically:

    1. Closes the reader to stop new messages from arriving.
    2. Nacks pending receipt modacks (exactly-once) so the server redelivers.
    3. Nacks and clears all buffered messages per the `on_shutdown` config.
    4. Removes those buffered ack_ids from `outstanding` so the drain
       phase only waits for messages already dispatched to processors.
    5. Sets `draining: true` and starts the drain timer.
    6. Checks if drain is already complete (outstanding may now be empty).

  Returns `{:ok, nacked_count}` where `nacked_count` is the number of
  buffered messages that were nacked and removed.
  """
  @spec prepare_for_draining(pid()) :: {:ok, non_neg_integer()}
  def prepare_for_draining(pid) do
    GenServer.call(pid, :prepare_for_draining)
  end

  @doc """
  Returns all currently outstanding ack_ids (received but not yet acked/nacked).
  Used in `terminate/2` to nack unprocessed messages per the `on_shutdown` option.
  """
  @spec get_outstanding(pid()) :: [String.t()]
  def get_outstanding(pid) do
    GenServer.call(pid, :get_outstanding)
  end

  @doc """
  Flushes any buffered acks and closes the gRPC stream gracefully.
  Called from the producer's `terminate/2`.
  """
  @spec close(pid()) :: :ok
  def close(pid) do
    GenServer.call(pid, :close, 10_000)
  end

  @doc """
  Signals additional demand from the producer. The `amount` is a delta (the
  `incoming_demand` from the latest `GenStage.handle_demand/2` callback).
  StreamManager accumulates it into `pending_demand` and flushes up to the
  new total from the message buffer.

  Called by `BroadwayCloudPubSub.Producer.handle_demand/2`.
  """
  @spec notify_demand(pid(), non_neg_integer()) :: :ok
  def notify_demand(pid, amount) when is_integer(amount) and amount >= 0 do
    GenServer.cast(pid, {:demand_available, amount})
  end

  # --- GenServer callbacks ---

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)
    config = Map.new(opts)

    backoff =
      Backoff.new(
        type: config.backoff_type,
        min: config.backoff_min,
        max: config.backoff_max
      )

    ack_batcher = Module.concat(config.broadway_name, AckBatcher)

    state = %__MODULE__{
      producer_pid: Map.fetch!(config, :producer_pid),
      config: config,
      grpc_client: config.grpc_client,
      grpc_client_config: config.grpc_client_config,
      backoff: backoff,
      ack_time_dist: AckTimeDistribution.new(config.stream_ack_deadline_seconds),
      ack_batcher: ack_batcher,
      pending_demand: 0
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl GenServer
  def handle_continue(:connect, state), do: do_connect(state)

  @impl GenServer
  # During draining, ignore reconnect attempts — we don't want new messages.
  def handle_info(:connect, %{draining: true} = state) do
    {:noreply, %{state | reconnect_ref: nil}}
  end

  def handle_info(:connect, state) do
    %{state | reconnect_ref: nil}
    |> do_connect()
  end

  # The StreamReader successfully opened the gRPC stream and sends us the
  # stream struct so we can call send_request for acks and lease extensions.
  def handle_info({:stream_opened, reader_pid, grpc_stream}, %{reader_pid: reader_pid} = state) do
    state =
      %{
        state
        | grpc_stream: grpc_stream,
          conn_pid: grpc_stream.channel.adapter_payload.conn_pid,
          backoff: Backoff.reset(state.backoff)
      }
      |> schedule_lease_timer()
      |> schedule_keepalive_timer()

    emit_telemetry(:connect, %{}, state.config)
    {:noreply, state}
  end

  # Stale :stream_opened from a previous reader (race during reconnect) — ignore.
  def handle_info({:stream_opened, _pid, _stream}, state) do
    {:noreply, state}
  end

  # Decoded messages forwarded from the StreamReader.
  def handle_info({:stream_messages, []}, state) do
    {:noreply, state}
  end

  def handle_info({:stream_messages, messages}, %{draining: true} = state) do
    nack_per_on_shutdown(state, Enum.map(messages, & &1.ack_id))
    {:noreply, state}
  end

  def handle_info({:stream_messages, messages}, state) do
    broadway_messages =
      Enum.map(messages, &MessageDispatch.build_broadway_message(&1, state.config.ack_ref))

    ack_ids = Enum.map(messages, & &1.ack_id)
    now = now_ms()

    adaptive_deadline =
      LeaseManager.effective_deadline(state.ack_time_dist, state.exactly_once_enabled)

    if state.exactly_once_enabled do
      # Exactly-once receipt modack gate: hold messages until the receipt modack
      # RPC confirms success. Messages whose modack fails are dropped (server redelivers).
      ref = make_ref()
      AckBatcher.receipt_modack(state.ack_batcher, ref, self(), ack_ids, adaptive_deadline)

      pending =
        Map.put(state.pending_receipt_modacks, ref, %{
          broadway_messages: broadway_messages,
          ack_ids: ack_ids,
          received_at: now
        })

      {:noreply, %{state | pending_receipt_modacks: pending}}
    else
      # Standard delivery: fire-and-forget receipt modack, dispatch immediately.
      new_outstanding =
        MessageDispatch.add_to_outstanding(
          state.outstanding,
          ack_ids,
          now,
          state.config.max_extension_ms
        )

      AckBatcher.modack(state.ack_batcher, ack_ids, adaptive_deadline)
      emit_telemetry(:receive_messages, %{count: length(broadway_messages)}, state.config)

      {:noreply, enqueue_and_flush(%{state | outstanding: new_outstanding}, broadway_messages)}
    end
  end

  # Result of an exactly-once receipt modack RPC sent via AckBatcher.receipt_modack/5.
  # Messages are delivered only if the receipt modack succeeded; otherwise dropped
  # (the server will redeliver them).
  #
  # During draining, any surviving pending entries are nacked rather than delivered.
  # Normally prepare_for_draining clears pending_receipt_modacks, but a result
  # could arrive in the mailbox between the receipt_modack call and the drain.
  def handle_info({:receipt_modack_result, ref, result}, state) do
    case Map.pop(state.pending_receipt_modacks, ref) do
      {nil, _} ->
        # Stale or unknown ref (e.g. cleared during drain) — ignore.
        {:noreply, state}

      {pending, rest} ->
        state = %{state | pending_receipt_modacks: rest}

        if state.draining do
          # Nack rather than deliver — server will redeliver.
          nack_per_on_shutdown(state, pending.ack_ids)
          {:noreply, state}
        else
          handle_receipt_modack_success(state, pending, result)
        end
    end
  end

  # Subscription properties forwarded from the StreamReader.
  # Sent by the server on any response when subscription settings change.
  def handle_info(
        {:subscription_properties,
         %{
           message_ordering_enabled: ordering_enabled,
           exactly_once_delivery_enabled: exactly_once_enabled
         } = _props},
        state
      ) do
    # Propagate retry deadline change to AckBatcher when exactly-once status changes.
    if exactly_once_enabled != state.exactly_once_enabled do
      new_deadline =
        if exactly_once_enabled,
          do: @exactly_once_retry_deadline_ms,
          else: Map.get(state.config, :retry_deadline_ms, 60_000)

      AckBatcher.update_retry_deadline(state.ack_batcher, new_deadline)
    end

    {:noreply,
     %{state | ordering_enabled: ordering_enabled, exactly_once_enabled: exactly_once_enabled}}
  end

  # Stream-level gRPC error reported by the StreamReader.
  # Retryable errors trigger reconnect; terminal errors stop the GenServer.
  # During draining, don't reconnect on retryable errors — just clean up.
  def handle_info({:stream_error, error}, %{draining: true} = state) do
    case ErrorClassifier.classify(error) do
      :terminal ->
        Logger.error(
          "Terminal gRPC stream error during drain on subscription #{state.config.subscription} - reason: #{inspect(error)}."
        )

        emit_telemetry(:terminal_error, %{}, state.config, %{reason: error})
        {:noreply, reset_connection(state)}

      :retryable ->
        emit_telemetry(:disconnect, %{}, state.config, %{reason: error})
        {:noreply, reset_connection(state)}
    end
  end

  def handle_info({:stream_error, error}, state) do
    case ErrorClassifier.classify(error) do
      :terminal ->
        Logger.error(
          "Terminal gRPC stream error on subscription #{state.config.subscription} - reason: #{inspect(error)}. Stopping StreamManager."
        )

        emit_telemetry(:terminal_error, %{}, state.config, %{reason: error})
        {:stop, {:terminal_error, error}, close_stream(state)}

      :retryable ->
        emit_telemetry(:disconnect, %{}, state.config, %{reason: error})

        state =
          state
          |> reset_connection()
          |> schedule_reconnect()

        {:noreply, state}
    end
  end

  # Server closed the stream normally (StreamReader enumeration exhausted).
  def handle_info({:stream_closed}, state) do
    # Stream ended naturally; nil out grpc_stream to skip cancel in close_stream/1.
    # See decisions.md for why cancelling after a server-initiated close crashes the Mint ConnectionProcess.
    handle_disconnect(%{state | grpc_stream: nil}, :stream_closed)
  end

  # StreamReader exited normally — {:stream_closed} should arrive first.
  # Only reconnect if grpc_stream is still set (stream_closed not yet processed).
  def handle_info({:EXIT, pid, :normal}, %{reader_pid: pid} = state) do
    if state.grpc_stream do
      # Same rationale as {:stream_closed}: skip cancel on natural close.
      handle_disconnect(%{state | grpc_stream: nil}, :stream_closed)
    else
      # Already handled by {:stream_closed} — just clear the reader_pid.
      {:noreply, %{state | reader_pid: nil}}
    end
  end

  # StreamReader crashed — reconnect (unless draining).
  def handle_info({:EXIT, pid, reason}, %{reader_pid: pid} = state) do
    handle_disconnect(state, reason)
  end

  # Catch-all for other EXIT signals (e.g. from the supervisor during shutdown).
  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, state}
  end

  def handle_info(:extend_leases, state) do
    {:noreply, do_extend_leases(state)}
  end

  # Periodic keep-alive ping to prevent the server from closing an idle stream.
  def handle_info(:send_keepalive, %{grpc_stream: nil} = state) do
    {:noreply, state}
  end

  def handle_info(:send_keepalive, state) do
    adaptive_deadline =
      LeaseManager.effective_deadline(state.ack_time_dist, state.exactly_once_enabled)

    keepalive_request = %StreamingPullRequest{stream_ack_deadline_seconds: adaptive_deadline}

    case send_on_stream(state.grpc_stream, keepalive_request, state) do
      {:ok, stream} ->
        emit_telemetry(:keepalive, %{deadline: adaptive_deadline}, state.config)
        timer = schedule_keepalive_after(state.config)
        {:noreply, %{state | grpc_stream: stream, keepalive_timer: timer}}

      {:error, _reason} ->
        state =
          state
          |> reset_connection()
          |> schedule_reconnect()

        {:noreply, state}
    end
  end

  # Mint adapter signals connection loss.
  # During draining, don't reconnect — the stream is intentionally closing.
  def handle_info({:elixir_grpc, :connection_down, conn_pid}, %{conn_pid: conn_pid} = state) do
    handle_disconnect(state, :connection_down)
  end

  # Gun adapter signals connection loss.
  def handle_info(
        {:gun_down, conn_pid, _protocol, _reason, _killed_streams},
        %{conn_pid: conn_pid} = state
      ) do
    handle_disconnect(state, :connection_down)
  end

  def handle_info(:drain_timeout, state) do
    outstanding_ids = Map.keys(state.outstanding)

    Telemetry.emit_span_exception(
      :stream,
      :drain,
      state.drain_started_at,
      Map.merge(span_metadata(state.config), %{kind: :timeout, reason: :drain_timeout}),
      %{remaining_count: length(outstanding_ids)},
      Map.get(state.config, :telemetry_metadata)
    )

    # Nack all remaining outstanding messages so they're redelivered promptly
    # instead of waiting for their ack deadlines to expire naturally. This also
    # empties the outstanding map so the producer's terminate/2 becomes a no-op.
    nack_per_on_shutdown(state, outstanding_ids)

    # Flush the batcher to ensure the nacks above are sent to the server
    # before the connection is torn down by close_stream.
    flush_batcher_if_alive(state.ack_batcher)

    state =
      %{state | drain_timer: nil, drain_started_at: nil, outstanding: %{}}
      |> close_stream()

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:acknowledge, ack_ids}, state) do
    {new_outstanding, new_dist} =
      MessageDispatch.record_and_remove(state.outstanding, state.ack_time_dist, ack_ids, now_ms())

    state = %{state | outstanding: new_outstanding, ack_time_dist: new_dist}

    AckBatcher.ack(state.ack_batcher, ack_ids)
    emit_telemetry(:ack, %{count: length(ack_ids)}, state.config)

    {:noreply, maybe_complete_drain(state)}
  end

  def handle_cast({:modify_deadline, ack_ids, deadline_seconds}, state) do
    # Record processing times and remove from outstanding for all deadline
    # modifications (both nack with deadline=0 and nack with deadline>0).
    # Once a message has been nacked, it must not be lease-extended further —
    # otherwise the periodic extend_leases cycle would override the requested
    # deadline, and the drain phase could never complete because outstanding
    # would never become empty.
    {new_outstanding, new_dist} =
      MessageDispatch.record_and_remove(state.outstanding, state.ack_time_dist, ack_ids, now_ms())

    state = %{state | outstanding: new_outstanding, ack_time_dist: new_dist}

    AckBatcher.modack(state.ack_batcher, ack_ids, deadline_seconds)

    {:noreply, maybe_complete_drain(state)}
  end

  # The producer signals a demand delta. Accumulate it and flush up to the new
  # total from the message buffer.
  def handle_cast({:demand_available, amount}, state) do
    state =
      %{state | pending_demand: state.pending_demand + amount}
      |> do_flush_demand()

    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:prepare_for_draining, _from, state) do
    drain_meta = span_metadata(state.config)

    drain_started_at =
      Telemetry.emit_span_start(
        :stream,
        :drain,
        drain_meta,
        %{
          buffered_count: :queue.len(state.message_buffer),
          outstanding_count: map_size(state.outstanding),
          pending_receipt_modack_count: map_size(state.pending_receipt_modacks)
        },
        Map.get(state.config, :telemetry_metadata)
      )

    try do
      # 1. Close the reader FIRST to stop new messages from arriving.
      # 2. Nack pending receipt modacks so the server redelivers them quickly.
      state =
        state
        |> close_reader()
        |> nack_pending_receipt_modacks()

      # 3. Extract ack_ids from buffered messages and nack them.
      buffered_ack_ids = MessageDispatch.extract_buffered_ack_ids(state.message_buffer)
      nacked_count = length(buffered_ack_ids)
      nack_per_on_shutdown(state, buffered_ack_ids)

      # 4. Remove buffered ack_ids from outstanding and clear the buffer.
      new_outstanding =
        Enum.reduce(buffered_ack_ids, state.outstanding, &Map.delete(&2, &1))

      # 5–6. Set draining state, start drain timer, check if already complete.
      state =
        %{
          state
          | message_buffer: :queue.new(),
            outstanding: new_outstanding,
            draining: true,
            drain_started_at: drain_started_at
        }
        |> start_drain_timer()
        |> maybe_complete_drain()

      {:reply, {:ok, nacked_count}, state}
    rescue
      e ->
        Telemetry.emit_span_exception(
          :stream,
          :drain,
          drain_started_at,
          Map.merge(drain_meta, %{kind: :error, reason: Exception.message(e)}),
          %{},
          Map.get(state.config, :telemetry_metadata)
        )

        reraise e, __STACKTRACE__
    end
  end

  def handle_call(:get_outstanding, _from, state) do
    {:reply, Map.keys(state.outstanding), state}
  end

  def handle_call(:close, _from, state) do
    # Best-effort flush; AckBatcher may already be down during pipeline shutdown.
    flush_batcher_if_alive(state.ack_batcher)
    {:reply, :ok, close_stream(state)}
  end

  @impl GenServer
  def terminate(reason, state) do
    if state.draining and state.drain_started_at != nil do
      remaining_count =
        map_size(state.outstanding) + map_size(state.pending_receipt_modacks)

      Telemetry.emit_span_exception(
        :stream,
        :drain,
        state.drain_started_at,
        Map.merge(span_metadata(state.config), %{kind: :terminate, reason: reason}),
        %{remaining_count: remaining_count},
        Map.get(state.config, :telemetry_metadata)
      )
    end

    state
    |> cancel_lease_timer()
    |> cancel_keepalive_timer()
    |> cancel_drain_timer()
    |> close_stream()

    :ok
  end

  # --- Private: connection ---

  # Shared handler for disconnect events (stream_closed, connection_down,
  # reader crash). Emits telemetry, resets the connection, and schedules
  # a reconnect unless draining.
  defp handle_disconnect(state, reason) do
    emit_telemetry(:disconnect, %{}, state.config, %{reason: reason})

    if state.draining do
      {:noreply, reset_connection(state)}
    else
      state =
        state
        |> reset_connection()
        |> schedule_reconnect()

      {:noreply, state}
    end
  end

  defp do_connect(state) do
    case connect(state) do
      {:ok, new_state} ->
        {:noreply, new_state}

      {:error, reason, new_state} ->
        emit_telemetry(:connection_failure, %{}, state.config, %{reason: reason})
        {:noreply, schedule_reconnect(new_state)}
    end
  end

  defp connect(state) do
    case state.grpc_client.connect(state.grpc_client_config) do
      {:ok, channel} ->
        connect_stream(channel, state)

      {:error, reason} ->
        {:error, reason, state}
    end
  rescue
    e ->
      {:error, {:connect_failed, Exception.message(e)}, state}
  end

  # Opens the gRPC channel and spawns the StreamReader, which will open the
  # stream and send {:stream_opened, reader_pid, grpc_stream} back to us.
  defp connect_stream(channel, state) do
    with {:ok, reader_pid} <- StreamReader.start_link(self(), channel, state.config) do
      {:ok,
       %{
         state
         | channel: channel,
           reader_pid: reader_pid,
           grpc_stream: nil,
           conn_pid: nil
       }}
    end
  rescue
    e ->
      state.grpc_client.disconnect(channel, state.grpc_client_config)
      {:error, {:connect_failed, Exception.message(e)}, state}
  end

  defp send_on_stream(grpc_stream, request, state) do
    state.grpc_client.send_request(grpc_stream, request, state.grpc_client_config)
  end

  defp reset_connection(state) do
    # Drop buffered messages on disconnect and nack them so they become
    # immediately available for redelivery to any consumer in the subscription.
    # Without the nack, redelivery depends on either this client reconnecting
    # (same client_id) or the ack deadline expiring naturally (up to 600s).
    buffered_ack_ids = MessageDispatch.extract_buffered_ack_ids(state.message_buffer)
    AckBatcher.modack(state.ack_batcher, buffered_ack_ids, 0)

    new_outstanding = Enum.reduce(buffered_ack_ids, state.outstanding, &Map.delete(&2, &1))

    # Dispatched ack_ids (sent to the producer but not yet acked) are intentionally
    # kept in `outstanding`. Since the same `client_id` is used on reconnection,
    # Pub/Sub associates the new stream with the same logical subscriber and won't
    # redeliver those messages. They remain in outstanding until acked/nacked by
    # the processor, or until `max_extension_ms` expiry in the lease extension cycle.
    #
    # Preserve pending_demand across reconnection to avoid a demand deadlock.
    # See decisions.md.
    %{state | message_buffer: :queue.new(), outstanding: new_outstanding}
    |> close_stream()
  end

  defp close_stream(%{reader_pid: nil, grpc_stream: nil} = state), do: state

  defp close_stream(state) do
    state =
      state
      |> stop_reader()
      |> cancel_grpc_stream()
      |> disconnect_channel()
      |> cancel_keepalive_timer()

    %{state | reader_pid: nil, grpc_stream: nil, channel: nil, conn_pid: nil}
  end

  defp stop_reader(%{reader_pid: nil} = state), do: state

  defp stop_reader(%{reader_pid: reader_pid} = state) do
    # Unlink before killing to prevent the EXIT signal from triggering reconnect.
    Process.unlink(reader_pid)
    Process.exit(reader_pid, :kill)
    state
  end

  defp cancel_grpc_stream(%{grpc_stream: nil} = state), do: state

  defp cancel_grpc_stream(%{grpc_stream: grpc_stream} = state) do
    # Skip cancel if the Mint StreamResponseProcess is already dead — calling
    # cancel would crash the ConnectionProcess. See decisions.md.
    srp_alive? =
      case grpc_stream do
        %{payload: %{stream_response_pid: pid}} when is_pid(pid) -> Process.alive?(pid)
        _ -> true
      end

    if srp_alive? do
      state.grpc_client.cancel(grpc_stream, state.grpc_client_config)
    end

    state
  end

  defp disconnect_channel(%{channel: nil} = state), do: state

  defp disconnect_channel(%{channel: channel} = state) do
    # Only disconnect if the connection process is alive; a dead channel causes
    # a FunctionClauseError inside the gRPC GenServer. See decisions.md.
    conn_alive? =
      case state.conn_pid do
        pid when is_pid(pid) -> Process.alive?(pid)
        _ -> true
      end

    if conn_alive? do
      state.grpc_client.disconnect(channel, state.grpc_client_config)
    end

    state
  end

  # --- Private: backoff ---

  defp schedule_reconnect(%{backoff: nil} = _state) do
    raise "StreamManager failed to connect and backoff is :stop — crashing"
  end

  # Deduplication: skip if a :connect is already pending to prevent the
  # double-reconnect race where {:stream_error} and {:stream_closed} (or {:EXIT})
  # both arrive within a single disconnect.
  defp schedule_reconnect(%{reconnect_ref: ref} = state) when not is_nil(ref) do
    state
  end

  defp schedule_reconnect(%{backoff: backoff} = state) do
    {timeout, new_backoff} = Backoff.backoff(backoff)
    emit_telemetry(:reconnect, %{delay: timeout}, state.config)
    ref = Process.send_after(self(), :connect, timeout)
    %{state | backoff: new_backoff, reconnect_ref: ref}
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  # --- Private: lease extension ---

  defp do_extend_leases(state) do
    now = now_ms()

    %{
      valid: valid,
      expired_count: expired_count,
      modack_ids: modack_ids,
      modack_deadline: modack_deadline,
      next_timer_ms: next_timer_ms
    } =
      LeaseManager.extend_leases(
        state.outstanding,
        state.ack_time_dist,
        state.exactly_once_enabled,
        now
      )

    if expired_count > 0 do
      emit_telemetry(:lease_expired, %{count: expired_count}, state.config)
    end

    emit_telemetry(
      :extend_leases,
      %{count: map_size(valid), deadline: modack_deadline},
      state.config
    )

    emit_telemetry(
      :pressure_snapshot,
      %{
        outstanding_count: map_size(valid),
        buffered_count: :queue.len(state.message_buffer),
        pending_demand: state.pending_demand
      },
      state.config
    )

    if modack_ids != [] do
      AckBatcher.modack(state.ack_batcher, modack_ids, modack_deadline)
    end

    timer = Process.send_after(self(), :extend_leases, next_timer_ms)

    # Sweep stale pending receipt modacks.
    %{fresh: fresh, stale_ack_ids: stale_ack_ids} =
      LeaseManager.sweep_stale_pending_modacks(state.pending_receipt_modacks, now)

    if stale_ack_ids != [] do
      AckBatcher.modack(state.ack_batcher, stale_ack_ids, 0)
      emit_telemetry(:receipt_modack_stale, %{count: length(stale_ack_ids)}, state.config)
    end

    %{state | outstanding: valid, lease_timer: timer, pending_receipt_modacks: fresh}
  end

  # --- Private: lease timer management ---

  defp schedule_lease_timer(state) do
    state = cancel_lease_timer(state)
    interval = LeaseManager.initial_timer_ms(state.config.stream_ack_deadline_seconds)
    timer = Process.send_after(self(), :extend_leases, interval)
    %{state | lease_timer: timer}
  end

  defp cancel_lease_timer(%{lease_timer: nil} = state), do: state

  defp cancel_lease_timer(%{lease_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | lease_timer: nil}
  end

  # --- Private: keep-alive ---

  defp schedule_keepalive_timer(state) do
    state = cancel_keepalive_timer(state)
    timer = schedule_keepalive_after(state.config)
    %{state | keepalive_timer: timer}
  end

  defp schedule_keepalive_after(config) do
    interval = Map.get(config, :keepalive_interval_ms, @default_keepalive_ms)
    Process.send_after(self(), :send_keepalive, interval)
  end

  defp cancel_keepalive_timer(%{keepalive_timer: nil} = state), do: state

  defp cancel_keepalive_timer(%{keepalive_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | keepalive_timer: nil}
  end

  # --- Private: drain ---

  # Kill the reader so no new messages arrive; keep the channel open for AckBatcher.
  defp close_reader(%{reader_pid: nil} = state), do: state

  defp close_reader(%{reader_pid: reader_pid} = state) do
    Process.unlink(reader_pid)
    Process.exit(reader_pid, :kill)
    %{state | reader_pid: nil}
  end

  defp start_drain_timer(state) do
    timeout = Map.get(state.config, :drain_timeout_ms, @default_drain_timeout_ms)
    timer = Process.send_after(self(), :drain_timeout, timeout)
    %{state | drain_timer: timer}
  end

  defp cancel_drain_timer(%{drain_timer: nil} = state), do: state

  defp cancel_drain_timer(%{drain_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | drain_timer: nil}
  end

  # After each ack/nack, check if we are draining and all outstanding messages
  # have been resolved. If so, cancel the drain timer and close the stream.
  defp maybe_complete_drain(
         %{draining: true, outstanding: outstanding, pending_receipt_modacks: pending} = state
       )
       when map_size(outstanding) == 0 and map_size(pending) == 0 do
    # AckBatcher may already be down during pipeline shutdown.
    flush_batcher_if_alive(state.ack_batcher)

    Telemetry.emit_span_stop(
      :stream,
      :drain,
      state.drain_started_at,
      span_metadata(state.config),
      Map.get(state.config, :telemetry_metadata)
    )

    state = cancel_drain_timer(state)
    close_stream(%{state | drain_started_at: nil})
  end

  defp maybe_complete_drain(state), do: state

  # --- Private: message dispatch ---

  # Enqueues messages into the buffer and flushes up to pending_demand.
  defp enqueue_and_flush(state, messages) do
    new_buffer = Enum.reduce(messages, state.message_buffer, &:queue.in(&1, &2))

    %{state | message_buffer: new_buffer}
    |> do_flush_demand()
  end

  # Flushes up to pending_demand messages from the buffer to the producer.
  # No-op when draining, demand is zero, or the buffer is empty.
  defp do_flush_demand(%{draining: true} = state), do: state
  defp do_flush_demand(%{pending_demand: 0} = state), do: state

  defp do_flush_demand(state) do
    %{to_send: to_send, remaining_buffer: remaining_buffer, remaining_demand: remaining_demand} =
      MessageDispatch.flush_demand(state.message_buffer, state.pending_demand)

    if to_send != [] do
      send(state.producer_pid, {:stream_messages, to_send})
    end

    %{state | message_buffer: remaining_buffer, pending_demand: remaining_demand}
  end

  # Handle the result of an exactly-once receipt modack RPC (non-draining path).
  defp handle_receipt_modack_success(state, pending, result) do
    case result do
      {:ok, []} ->
        new_outstanding =
          MessageDispatch.add_to_outstanding(
            state.outstanding,
            pending.ack_ids,
            pending.received_at,
            state.config.max_extension_ms
          )

        emit_telemetry(
          :receive_messages,
          %{count: length(pending.broadway_messages)},
          state.config
        )

        {:noreply,
         enqueue_and_flush(%{state | outstanding: new_outstanding}, pending.broadway_messages)}

      {:ok, failed_ids} ->
        # Partial success — deliver only messages whose modack succeeded.
        {ok_msgs, ok_ids} =
          MessageDispatch.partition_succeeded(
            pending.broadway_messages,
            pending.ack_ids,
            failed_ids
          )

        new_outstanding =
          MessageDispatch.add_to_outstanding(
            state.outstanding,
            ok_ids,
            pending.received_at,
            state.config.max_extension_ms
          )

        if ok_msgs != [] do
          emit_telemetry(:receive_messages, %{count: length(ok_msgs)}, state.config)
          {:noreply, enqueue_and_flush(%{state | outstanding: new_outstanding}, ok_msgs)}
        else
          {:noreply, %{state | outstanding: new_outstanding}}
        end

      {:error, _reason} ->
        # Total failure — drop all messages (server will redeliver).
        {:noreply, state}
    end
  end

  # Nack a list of ack_ids per the on_shutdown config. With :noop, messages are
  # simply dropped (server redelivers after ack deadline expires naturally).
  defp nack_per_on_shutdown(_state, []), do: :ok
  defp nack_per_on_shutdown(%{config: %{on_shutdown: :noop}}, _ack_ids), do: :ok

  defp nack_per_on_shutdown(
         %{ack_batcher: ack_batcher, config: %{on_shutdown: {:nack, deadline}}},
         ack_ids
       ) do
    AckBatcher.modack(ack_batcher, ack_ids, deadline)
  end

  # Nack all messages held in pending_receipt_modacks so the server redelivers
  # them quickly. Used during drain/shutdown.
  defp nack_pending_receipt_modacks(%{pending_receipt_modacks: pending} = state)
       when map_size(pending) == 0,
       do: state

  defp nack_pending_receipt_modacks(state) do
    pending_ids =
      state.pending_receipt_modacks
      |> Map.values()
      |> Enum.flat_map(& &1.ack_ids)

    nack_per_on_shutdown(state, pending_ids)
    %{state | pending_receipt_modacks: %{}}
  end

  # Flush AckBatcher if its process is currently alive. Guards against the
  # batcher being down during pipeline shutdown (Broadway stops children in
  # reverse start order).
  defp flush_batcher_if_alive(nil), do: :ok

  defp flush_batcher_if_alive(batcher) do
    case GenServer.whereis(batcher) do
      nil -> :ok
      pid -> AckBatcher.flush(pid)
    end
  end

  # --- Private: telemetry ---

  defp span_metadata(config) do
    %{
      name: config.broadway[:name],
      subscription: config.subscription
    }
  end

  defp emit_telemetry(event, measurements, config, extra_metadata \\ %{}) do
    metadata =
      Map.merge(
        span_metadata(config),
        extra_metadata
      )

    Telemetry.execute(
      :stream,
      event,
      measurements,
      metadata,
      Map.get(config, :telemetry_metadata)
    )
  end
end
