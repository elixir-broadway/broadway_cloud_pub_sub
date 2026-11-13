defmodule BroadwayCloudPubSub.Streaming.AckBatcher do
  @moduledoc false

  # GenServer that accumulates ack and modifyAckDeadline requests and flushes
  # them to UnaryRpcClient on a timer, size threshold, or explicit flush.
  # Modacks are grouped by deadline value: one unary RPC per unique deadline per flush.

  use GenServer

  alias BroadwayCloudPubSub.Streaming.{Options, RetryTracker, Telemetry, UnaryRpcClient}

  @max_modack_attempts 3

  defstruct [
    :rpc_client,
    :broadway_name,
    :subscription,
    :telemetry_metadata,
    :batch_interval_ms,
    :batch_max_size,
    :timer_ref,
    # Registered name of the Task.Supervisor for receipt modack tasks.
    :task_supervisor,
    ack_ids: [],
    ack_count: 0,
    # %{deadline_seconds => [ack_id]}
    modack_ids: %{},
    # RetryTracker for ack retry state (deadline-only, no attempt limit).
    ack_tracker: nil,
    # RetryTracker for modack retry state (deadline + 3-attempt limit).
    modack_tracker: nil
  ]

  @all_keys [
    :subscription,
    :ack_batch_interval_ms,
    :ack_batch_max_size,
    :retry_deadline_ms,
    :broadway_name,
    :telemetry_metadata,
    :rpc_client,
    :task_supervisor
  ]

  @required_keys [
    :subscription,
    :ack_batch_interval_ms,
    :ack_batch_max_size,
    :broadway_name,
    :rpc_client,
    :task_supervisor
  ]

  @doc false
  @spec child_opts(keyword()) :: keyword()
  def child_opts(opts) do
    Options.validate_child_opts(opts, @all_keys, @required_keys)
  end

  @doc """
  Updates the retry deadline at runtime. Called by StreamManager when it detects
  a change in exactly-once delivery status from subscription_properties.
  """
  @spec update_retry_deadline(GenServer.server(), pos_integer()) :: :ok
  def update_retry_deadline(pid, retry_deadline_ms) do
    GenServer.cast(pid, {:update_retry_deadline, retry_deadline_ms})
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
  Queues ack_ids for acknowledgement. Fire-and-forget.
  """
  @spec ack(GenServer.server(), [String.t()]) :: :ok
  def ack(pid, ack_ids) when is_list(ack_ids) and ack_ids != [] do
    GenServer.cast(pid, {:ack, ack_ids})
  end

  def ack(_pid, []), do: :ok

  @doc """
  Queues ack_ids for a modifyAckDeadline request. Fire-and-forget.
  """
  @spec modack(GenServer.server(), [String.t()], non_neg_integer()) :: :ok
  def modack(pid, ack_ids, deadline_seconds) when is_list(ack_ids) and ack_ids != [] do
    GenServer.cast(pid, {:modack, ack_ids, deadline_seconds})
  end

  def modack(_pid, [], _deadline), do: :ok

  @doc """
  Sends a receipt modack for exactly-once delivery. Spawns a Task that calls
  UnaryRpcClient.modify_ack_deadline/3 and sends the result to `reply_to`
  as `{:receipt_modack_result, ref, result}`.

  Unlike modack/3, this is NOT batched — it runs immediately because
  exactly-once delivery requires confirmation before dispatching messages.
  """
  @spec receipt_modack(GenServer.server(), reference(), pid(), [String.t()], non_neg_integer()) ::
          :ok
  def receipt_modack(pid, ref, reply_to, ack_ids, deadline_seconds) do
    GenServer.cast(pid, {:receipt_modack, ref, reply_to, ack_ids, deadline_seconds})
  end

  @doc """
  Flushes all pending batches synchronously. Used during graceful shutdown to
  ensure no acks are dropped before the process terminates.
  """
  @spec flush(GenServer.server()) :: :ok
  def flush(pid) do
    GenServer.call(pid, :flush, 15_000)
  end

  # --- GenServer callbacks ---

  @impl GenServer
  def init(opts) do
    config = Map.new(opts)

    state = %__MODULE__{
      rpc_client: config.rpc_client,
      broadway_name: config[:broadway_name],
      subscription: config[:subscription],
      telemetry_metadata: config[:telemetry_metadata],
      batch_interval_ms: config.ack_batch_interval_ms,
      batch_max_size: config.ack_batch_max_size,
      task_supervisor: config[:task_supervisor],
      ack_tracker: RetryTracker.new(retry_deadline_ms: config[:retry_deadline_ms]),
      modack_tracker:
        RetryTracker.new(
          retry_deadline_ms: config[:retry_deadline_ms],
          max_attempts: @max_modack_attempts
        )
    }

    {:ok, schedule_flush(state)}
  end

  @impl GenServer
  def handle_cast({:ack, ack_ids}, state) do
    now = now_ms()
    new_ids = ack_ids ++ state.ack_ids
    new_count = state.ack_count + length(ack_ids)
    ack_tracker = RetryTracker.track(state.ack_tracker, ack_ids, now)
    state = %{state | ack_ids: new_ids, ack_count: new_count, ack_tracker: ack_tracker}

    state =
      if new_count >= state.batch_max_size do
        do_flush(state)
      else
        state
      end

    {:noreply, state}
  end

  def handle_cast({:modack, ack_ids, deadline_seconds}, state) do
    now = now_ms()

    new_modack_ids =
      Map.update(state.modack_ids, deadline_seconds, ack_ids, &(ack_ids ++ &1))

    total_modack_count = new_modack_ids |> Map.values() |> Enum.map(&length/1) |> Enum.sum()
    modack_tracker = RetryTracker.track(state.modack_tracker, ack_ids, now)

    state = %{state | modack_ids: new_modack_ids, modack_tracker: modack_tracker}

    state =
      if state.ack_count + total_modack_count >= state.batch_max_size do
        do_flush(state)
      else
        state
      end

    {:noreply, state}
  end

  def handle_cast({:update_retry_deadline, retry_deadline_ms}, state) do
    {:noreply,
     %{
       state
       | ack_tracker: RetryTracker.update_retry_deadline(state.ack_tracker, retry_deadline_ms),
         modack_tracker:
           RetryTracker.update_retry_deadline(state.modack_tracker, retry_deadline_ms)
     }}
  end

  # Receipt modack for exactly-once delivery. Spawns a supervised Task that
  # calls UnaryRpcClient directly (bypassing batching) and sends the result
  # back to the caller. The Task is fire-and-forget from AckBatcher's
  # perspective, but is supervised so it's cleaned up on pipeline shutdown.
  def handle_cast({:receipt_modack, ref, reply_to, ack_ids, deadline_seconds}, state) do
    rpc_client = state.rpc_client

    Task.Supervisor.start_child(state.task_supervisor, fn ->
      do_receipt_modack(rpc_client, ref, reply_to, ack_ids, deadline_seconds)
    end)

    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:flush, _from, state) do
    state = do_flush(state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info(:flush_timer, state) do
    state = do_flush(state)
    {:noreply, schedule_flush(state)}
  end

  defp do_flush(state) do
    state = cancel_timer(state)

    # Check if RPC client is available. If not, keep all state and retry on the
    # next timer tick to avoid a noproc crash while UnaryRpcClient is restarting.
    case GenServer.whereis(state.rpc_client) do
      nil ->
        emit_telemetry(
          :flush_deferred,
          %{ack_count: state.ack_count, modack_groups: map_size(state.modack_ids)},
          state
        )

        schedule_flush(state)

      _pid ->
        # Each step runs independently; a flush_acks failure does not block flush_modacks.
        state
        |> flush_acks()
        |> flush_modacks()
        |> schedule_flush()
    end
  end

  defp flush_acks(%{ack_count: 0} = state), do: state

  defp flush_acks(state) do
    rpc_result = UnaryRpcClient.acknowledge(state.rpc_client, state.ack_ids)
    result = classify_ack_result(state.ack_ids, rpc_result, state.ack_tracker, now_ms())

    if result.expired_count > 0 do
      emit_telemetry(:ack_retry_expired, %{count: result.expired_count}, state)
    end

    %{state | ack_ids: result.live, ack_count: length(result.live), ack_tracker: result.tracker}
  end

  defp flush_modacks(%{modack_ids: modacks} = state) when map_size(modacks) == 0, do: state

  defp flush_modacks(state) do
    all_ids = state.modack_ids |> Map.values() |> List.flatten()
    tracker = RetryTracker.record_attempt(state.modack_tracker, all_ids)

    # Call each deadline group's RPC and collect results.
    rpc_results =
      Enum.map(state.modack_ids, fn {deadline, ids} ->
        rpc_result = UnaryRpcClient.modify_ack_deadline(state.rpc_client, ids, deadline)
        {deadline, ids, rpc_result}
      end)

    result = classify_modack_results(rpc_results, tracker, now_ms())

    if result.exhausted_count > 0 do
      emit_telemetry(:modack_retry_exhausted, %{count: result.exhausted_count}, state)
    end

    if result.expired_count > 0 do
      emit_telemetry(:modack_retry_expired, %{count: result.expired_count}, state)
    end

    %{state | modack_ids: result.remaining, modack_tracker: result.tracker}
  end

  # --- Pure classification functions ---

  @typedoc "RPC result from UnaryRpcClient.acknowledge/2 or modify_ack_deadline/3."
  @type rpc_result :: {:ok, [String.t()]} | {:error, term()}

  @doc false
  @spec classify_ack_result([String.t()], rpc_result(), RetryTracker.t(), integer()) ::
          %{live: [String.t()], expired_count: non_neg_integer(), tracker: RetryTracker.t()}
  def classify_ack_result(sent_ack_ids, rpc_result, ack_tracker, now_ms) do
    retained_ids = extract_retained_ids(sent_ack_ids, rpc_result)

    retained_set = MapSet.new(retained_ids)
    tracker = RetryTracker.retain_only(ack_tracker, retained_set)
    {live, expired, tracker} = RetryTracker.expire_stale(tracker, retained_ids, now_ms)

    %{live: live, expired_count: length(expired), tracker: tracker}
  end

  @doc false
  @spec classify_modack_results(
          [{non_neg_integer(), [String.t()], rpc_result()}],
          RetryTracker.t(),
          integer()
        ) ::
          %{
            remaining: %{non_neg_integer() => [String.t()]},
            exhausted_count: non_neg_integer(),
            expired_count: non_neg_integer(),
            tracker: RetryTracker.t()
          }
  def classify_modack_results(rpc_results, modack_tracker, now_ms) do
    # Each deadline group is processed independently. Thread the tracker
    # through each group so attempt-limit checks use the latest counts.
    {remaining_modacks, tracker, total_exhausted} =
      Enum.reduce(rpc_results, {%{}, modack_tracker, 0}, fn
        {deadline, sent_ids, rpc_result}, {remaining, trk, exhausted_acc} ->
          result_ids = extract_retained_ids(sent_ids, rpc_result)

          if result_ids == [] do
            {remaining, trk, exhausted_acc}
          else
            {keep, drop, trk} = RetryTracker.check_attempts(trk, result_ids)

            remaining =
              if keep == [], do: remaining, else: Map.put(remaining, deadline, keep)

            {remaining, trk, exhausted_acc + length(drop)}
          end
      end)

    # Cleanup sweep: bound tracking maps to currently-pending ids only.
    still_pending = remaining_modacks |> Map.values() |> List.flatten() |> MapSet.new()
    tracker = RetryTracker.retain_only(tracker, still_pending)

    # Expire stale modack ids that have exceeded the retry deadline.
    still_pending_list = MapSet.to_list(still_pending)

    {_live_ids, expired_ids, tracker} =
      RetryTracker.expire_stale(tracker, still_pending_list, now_ms)

    # Remove expired ids from remaining_modacks.
    remaining_modacks =
      if expired_ids == [] do
        remaining_modacks
      else
        expired_set = MapSet.new(expired_ids)

        remaining_modacks
        |> Enum.map(fn {d, ids} -> {d, Enum.reject(ids, &MapSet.member?(expired_set, &1))} end)
        |> Enum.reject(fn {_, ids} -> ids == [] end)
        |> Map.new()
      end

    %{
      remaining: remaining_modacks,
      exhausted_count: total_exhausted,
      expired_count: length(expired_ids),
      tracker: tracker
    }
  end

  # Extracts the list of ack_ids that should be retained from an RPC result.
  defp extract_retained_ids(_sent_ids, {:ok, []}), do: []
  defp extract_retained_ids(_sent_ids, {:ok, remaining_ids}), do: remaining_ids

  defp extract_retained_ids(_sent_ids, {:error, {_rpc_error, transient_ids}})
       when is_list(transient_ids),
       do: transient_ids

  defp extract_retained_ids(sent_ids, {:error, _reason}), do: sent_ids

  defp schedule_flush(state) do
    state = cancel_timer(state)
    ref = Process.send_after(self(), :flush_timer, state.batch_interval_ms)
    %{state | timer_ref: ref}
  end

  defp cancel_timer(%{timer_ref: nil} = state), do: state

  defp cancel_timer(%{timer_ref: ref} = state) do
    Process.cancel_timer(ref)
    # Drain any :flush_timer already in the mailbox to prevent a double flush.
    receive do
      :flush_timer -> :ok
    after
      0 -> :ok
    end

    %{state | timer_ref: nil}
  end

  # Executes a receipt modack RPC and always sends the result to `reply_to`,
  # even if the RPC raises or the process it calls is dead. This prevents
  # pending_receipt_modacks entries from being orphaned in StreamManager.
  defp do_receipt_modack(rpc_client, ref, reply_to, ack_ids, deadline_seconds) do
    result =
      try do
        UnaryRpcClient.modify_ack_deadline(rpc_client, ack_ids, deadline_seconds)
      rescue
        e -> {:error, {:receipt_modack_crashed, Exception.message(e)}}
      catch
        kind, reason -> {:error, {:receipt_modack_crashed, {kind, reason}}}
      end

    send(reply_to, {:receipt_modack_result, ref, result})
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp emit_telemetry(event, measurements, state) do
    metadata = %{
      name: state.broadway_name,
      subscription: state.subscription
    }

    Telemetry.execute(:ack_batcher, event, measurements, metadata, state.telemetry_metadata)
  end
end
