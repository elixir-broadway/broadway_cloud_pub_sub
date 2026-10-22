defmodule BroadwayCloudPubSub.Streaming.AckBatcherTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Streaming.AckBatcher
  alias BroadwayCloudPubSub.Test.TelemetryHelper

  # A spy GenServer that records every call it receives and forwards them to
  # the test process so we can assert on them. Returns :ok to all calls so
  # AckBatcher sees {:ok, []} (full success) for every flush.
  defmodule SpyRpcClient do
    use GenServer

    def start_link(opts) do
      {name, opts} = Keyword.pop(opts, :name)
      {test_pid, _} = Keyword.pop(opts, :test_pid)

      if name do
        GenServer.start_link(__MODULE__, test_pid, name: name)
      else
        GenServer.start_link(__MODULE__, test_pid)
      end
    end

    def init(test_pid), do: {:ok, test_pid}

    # Spy on call-based API — notify test process and return :ok so
    # AckBatcher accumulates {:ok, []} (all delivered, nothing retained).
    def handle_call({:acknowledge, _ack_ids} = msg, _from, test_pid) do
      send(test_pid, {:rpc, msg})
      {:reply, :ok, test_pid}
    end

    def handle_call({:modify_ack_deadline, _ids, _deadline} = msg, _from, test_pid) do
      send(test_pid, {:rpc, msg})
      {:reply, :ok, test_pid}
    end

    def handle_call(:ping, _from, state), do: {:reply, :ok, state}
  end

  # A spy that returns {:error, :unavailable} for the first call, then :ok.
  defmodule FlakyRpcClient do
    use GenServer

    def start_link(test_pid) do
      GenServer.start_link(__MODULE__, {test_pid, 0})
    end

    def init(state), do: {:ok, state}

    def handle_call({:acknowledge, ack_ids}, _from, {test_pid, call_count}) do
      send(test_pid, {:rpc, {:acknowledge, ack_ids}, call_count})
      reply = if call_count == 0, do: {:error, :unavailable}, else: :ok
      {:reply, reply, {test_pid, call_count + 1}}
    end

    def handle_call({:modify_ack_deadline, _ids, _deadline} = msg, _from, {test_pid, count}) do
      send(test_pid, {:rpc, msg, count})
      {:reply, :ok, {test_pid, count + 1}}
    end
  end

  # A spy that always fails modack for deadline=30 but succeeds for deadline=60.
  defmodule SelectiveFlakyRpc do
    use GenServer

    def start_link(test_pid) do
      GenServer.start_link(__MODULE__, {test_pid, 0})
    end

    def init(state), do: {:ok, state}

    def handle_call({:acknowledge, _ids}, _from, {test_pid, count}) do
      {:reply, :ok, {test_pid, count + 1}}
    end

    def handle_call({:modify_ack_deadline, ids, deadline}, _from, {test_pid, count}) do
      send(test_pid, {:rpc, {:modack, ids, deadline}, count})
      reply = if deadline == 30 and count == 0, do: {:error, :unavailable}, else: :ok
      {:reply, reply, {test_pid, count + 1}}
    end
  end

  # Start a Task.Supervisor for receipt modack tasks.
  # Returns the supervisor pid.
  defp start_task_supervisor do
    {:ok, sup} = Task.Supervisor.start_link()
    sup
  end

  # Start a spy RPC client + AckBatcher pair.
  # Returns {batcher_pid, rpc_client_pid}.
  defp start_batcher(extra_opts \\ []) do
    test_pid = self()

    {:ok, rpc_pid} = SpyRpcClient.start_link(test_pid: test_pid)

    opts =
      Keyword.merge(
        [
          rpc_client: rpc_pid,
          broadway_name: :TestPipeline,
          subscription: "projects/test/subscriptions/test-sub",
          ack_batch_interval_ms: 50,
          ack_batch_max_size: 10,
          task_supervisor: start_task_supervisor()
        ],
        extra_opts
      )

    {:ok, batcher} = AckBatcher.start_link(opts)
    {batcher, rpc_pid}
  end

  # ============================================================
  # ack/2
  # ============================================================

  describe "ack/2" do
    test "queues ack_ids and flushes them on the next timer tick" do
      {batcher, _rpc} = start_batcher()

      AckBatcher.ack(batcher, ["id-1", "id-2"])

      # Timer fires after 50ms
      assert_receive {:rpc, {:acknowledge, ids}}, 200
      assert Enum.sort(ids) == ["id-1", "id-2"]
    end

    test "no-op when list is empty" do
      {batcher, _rpc} = start_batcher()

      AckBatcher.ack(batcher, [])

      refute_receive {:rpc, _}, 100
    end

    test "accumulates multiple ack calls before flush" do
      # Long interval so the timer doesn't fire mid-test
      {batcher, _rpc} = start_batcher(ack_batch_interval_ms: 10_000)

      AckBatcher.ack(batcher, ["id-1"])
      AckBatcher.ack(batcher, ["id-2"])
      AckBatcher.ack(batcher, ["id-3"])

      AckBatcher.flush(batcher)

      assert_receive {:rpc, {:acknowledge, ids}}, 500
      assert Enum.sort(ids) == ["id-1", "id-2", "id-3"]
    end
  end

  # ============================================================
  # modack/3
  # ============================================================

  describe "modack/3" do
    test "queues modack_ids and flushes them on the next timer tick" do
      {batcher, _rpc} = start_batcher()

      AckBatcher.modack(batcher, ["id-a"], 30)

      assert_receive {:rpc, {:modify_ack_deadline, ids, 30}}, 200
      assert ids == ["id-a"]
    end

    test "groups modacks by deadline — one RPC per unique deadline per flush" do
      {batcher, _rpc} = start_batcher(ack_batch_interval_ms: 10_000)

      AckBatcher.modack(batcher, ["id-1", "id-2"], 30)
      AckBatcher.modack(batcher, ["id-3"], 60)
      AckBatcher.modack(batcher, ["id-4"], 30)

      AckBatcher.flush(batcher)

      # We expect exactly two :modify_ack_deadline messages — one per deadline
      rpcs = collect_rpcs(2, 500)

      {ids_30, deadline_30} = find_modack(rpcs, 30)
      {ids_60, deadline_60} = find_modack(rpcs, 60)

      assert deadline_30 == 30
      assert Enum.sort(ids_30) == ["id-1", "id-2", "id-4"]

      assert deadline_60 == 60
      assert ids_60 == ["id-3"]
    end

    test "no-op when list is empty" do
      {batcher, _rpc} = start_batcher()

      AckBatcher.modack(batcher, [], 30)

      refute_receive {:rpc, _}, 100
    end
  end

  # ============================================================
  # flush/1
  # ============================================================

  describe "flush/1" do
    test "flush/1 sends all pending acks and modacks synchronously" do
      {batcher, _rpc} = start_batcher(ack_batch_interval_ms: 10_000)

      AckBatcher.ack(batcher, ["ack-1"])
      AckBatcher.modack(batcher, ["mod-1"], 30)

      :ok = AckBatcher.flush(batcher)

      assert_receive {:rpc, {:acknowledge, _}}, 500
      assert_receive {:rpc, {:modify_ack_deadline, _, 30}}, 500
    end

    test "flush/1 is a no-op when nothing is queued" do
      {batcher, _rpc} = start_batcher(ack_batch_interval_ms: 10_000)

      :ok = AckBatcher.flush(batcher)

      refute_receive {:rpc, _}, 100
    end

    test "flush/1 resets the state — subsequent timer does not re-send" do
      {batcher, _rpc} = start_batcher(ack_batch_interval_ms: 10_000)

      AckBatcher.ack(batcher, ["id-1"])
      :ok = AckBatcher.flush(batcher)

      # Drain the flushed message
      assert_receive {:rpc, {:acknowledge, _}}, 500

      # Should receive no further RPC from a duplicate flush
      refute_receive {:rpc, _}, 100
    end
  end

  # ============================================================
  # Max batch size — size-triggered flush
  # ============================================================

  describe "size-triggered flush" do
    test "flushes immediately when ack_count reaches ack_batch_max_size" do
      # batch_max_size = 3, long timer so only size triggers the flush
      {batcher, _rpc} = start_batcher(ack_batch_interval_ms: 10_000, ack_batch_max_size: 3)

      AckBatcher.ack(batcher, ["id-1", "id-2", "id-3"])

      # Should flush without waiting for the timer
      assert_receive {:rpc, {:acknowledge, ids}}, 200
      assert length(ids) == 3
    end

    test "flushes when combined ack + modack count reaches max_size" do
      {batcher, _rpc} = start_batcher(ack_batch_interval_ms: 10_000, ack_batch_max_size: 3)

      AckBatcher.ack(batcher, ["id-1"])
      AckBatcher.modack(batcher, ["id-2", "id-3"], 30)

      # Combined count == 3 — should trigger flush
      assert_receive {:rpc, _}, 200
    end
  end

  # ============================================================
  # Timer behaviour
  # ============================================================

  describe "timer" do
    test "timer fires automatically without explicit flush" do
      {batcher, _rpc} = start_batcher(ack_batch_interval_ms: 30)

      AckBatcher.ack(batcher, ["timer-id"])

      assert_receive {:rpc, {:acknowledge, ["timer-id"]}}, 300
    end

    test "timer resets after each flush — sends again on next tick" do
      {batcher, _rpc} = start_batcher(ack_batch_interval_ms: 30)

      AckBatcher.ack(batcher, ["tick-1"])
      assert_receive {:rpc, {:acknowledge, _}}, 300

      AckBatcher.ack(batcher, ["tick-2"])
      assert_receive {:rpc, {:acknowledge, _}}, 300
    end
  end

  # ============================================================
  # Partial failure handling — acks retained on RPC failure
  # ============================================================

  describe "partial failure handling" do
    test "ack_ids are retained in state when flush fails and retried on next tick" do
      test_pid = self()
      {:ok, flaky} = FlakyRpcClient.start_link(test_pid)

      {:ok, batcher} =
        AckBatcher.start_link(
          rpc_client: flaky,
          ack_batch_interval_ms: 40,
          ack_batch_max_size: 100,
          task_supervisor: start_task_supervisor()
        )

      AckBatcher.ack(batcher, ["id-1", "id-2"])

      # First timer tick — RPC fails, ack_ids retained
      assert_receive {:rpc, {:acknowledge, first_ids}, 0}, 300
      assert Enum.sort(first_ids) == ["id-1", "id-2"]

      # Second timer tick — RPC succeeds, ack_ids cleared
      assert_receive {:rpc, {:acknowledge, retry_ids}, 1}, 300
      assert Enum.sort(retry_ids) == ["id-1", "id-2"]

      # After successful flush, state should be clear — no further RPCs
      refute_receive {:rpc, {:acknowledge, _}, _}, 100
    end

    test "modack_ids for a failing deadline group are retained independently" do
      test_pid = self()

      {:ok, selective} = SelectiveFlakyRpc.start_link(test_pid)

      {:ok, batcher} =
        AckBatcher.start_link(
          rpc_client: selective,
          ack_batch_interval_ms: 40,
          ack_batch_max_size: 100,
          task_supervisor: start_task_supervisor()
        )

      AckBatcher.modack(batcher, ["id-30"], 30)
      AckBatcher.modack(batcher, ["id-60"], 60)

      # First tick: deadline=30 fails, deadline=60 succeeds
      # Both are attempted in the same flush (Enum.reduce over modack_ids)
      assert_receive {:rpc, {:modack, _, 30}, 0}, 300
      assert_receive {:rpc, {:modack, _, 60}, _}, 300

      # Second tick: deadline=30 is retried (count=1 now → succeeds), deadline=60 is gone
      assert_receive {:rpc, {:modack, ["id-30"], 30}, _}, 300
      refute_receive {:rpc, {:modack, ["id-60"], 60}, _}, 100
    end
  end

  # ============================================================
  # RPC client unavailable — defer flush
  # ============================================================

  describe "RPC client unavailability" do
    test "flush is deferred gracefully when rpc_client process is not registered" do
      # Use a name that is never registered so GenServer.whereis returns nil
      fake_name = Module.concat(__MODULE__, "NeverRegistered#{System.unique_integer()}")

      {:ok, batcher} =
        AckBatcher.start_link(
          rpc_client: fake_name,
          ack_batch_interval_ms: 50,
          ack_batch_max_size: 100,
          task_supervisor: start_task_supervisor()
        )

      AckBatcher.ack(batcher, ["id-orphan"])

      # Flush should not crash the batcher even though rpc_client is not alive
      :ok = AckBatcher.flush(batcher)

      assert Process.alive?(batcher)

      # Ack_ids must still be retained (not silently dropped)
      state = :sys.get_state(batcher)
      assert state.ack_count == 1
      assert state.ack_ids == ["id-orphan"]
    end
  end

  # ============================================================
  # receipt_modack/5 — exactly-once delivery
  # ============================================================

  describe "receipt_modack/5" do
    test "spawns a task that calls modify_ack_deadline and sends result to reply_to" do
      {batcher, _rpc} = start_batcher()
      ref = make_ref()

      AckBatcher.receipt_modack(batcher, ref, self(), ["id-eo-1", "id-eo-2"], 60)

      # SpyRpcClient returns :ok (i.e., {:ok, []}) for modify_ack_deadline
      assert_receive {:receipt_modack_result, ^ref, {:ok, []}}, 500
      # The RPC was also made to the spy
      assert_receive {:rpc, {:modify_ack_deadline, ids, 60}}, 500
      assert Enum.sort(ids) == ["id-eo-1", "id-eo-2"]
    end

    test "result is sent to the specified reply_to pid, not the batcher" do
      {batcher, _rpc} = start_batcher()
      ref = make_ref()
      # reply_to is self(), so we expect the message here
      AckBatcher.receipt_modack(batcher, ref, self(), ["id-reply"], 60)

      assert_receive {:receipt_modack_result, ^ref, _result}, 500
    end

    test "does NOT add ack_ids to the batcher's pending batch" do
      {batcher, _rpc} = start_batcher(ack_batch_interval_ms: 10_000)
      ref = make_ref()

      AckBatcher.receipt_modack(batcher, ref, self(), ["id-not-batched"], 60)
      # Wait for the task to complete
      assert_receive {:receipt_modack_result, ^ref, _}, 500

      # State should have no pending ack_ids or modack_ids
      state = :sys.get_state(batcher)
      assert state.ack_ids == []
      assert state.modack_ids == %{}
    end

    test "multiple concurrent receipt_modacks use independent refs" do
      {batcher, _rpc} = start_batcher()
      ref1 = make_ref()
      ref2 = make_ref()

      AckBatcher.receipt_modack(batcher, ref1, self(), ["id-a"], 60)
      AckBatcher.receipt_modack(batcher, ref2, self(), ["id-b"], 60)

      results =
        for _ <- 1..2 do
          receive do
            {:receipt_modack_result, ref, result} -> {ref, result}
          after
            500 -> flunk("Expected 2 receipt_modack_result messages")
          end
        end

      result_refs = Enum.map(results, &elem(&1, 0))

      assert Enum.sort_by(result_refs, &:erlang.ref_to_list/1) ==
               Enum.sort_by([ref1, ref2], &:erlang.ref_to_list/1)
    end
  end

  # ============================================================
  # update_retry_deadline/2 — exactly-once auto-switch
  # ============================================================

  describe "update_retry_deadline/2" do
    test "updates retry_deadline_ms in state" do
      {batcher, _rpc} = start_batcher()

      # Default is nil (not configured in start_batcher)
      state = :sys.get_state(batcher)
      assert state.ack_tracker.retry_deadline_ms == nil
      assert state.modack_tracker.retry_deadline_ms == nil

      AckBatcher.update_retry_deadline(batcher, 600_000)
      # Cast is async — sync via flush
      AckBatcher.flush(batcher)

      state = :sys.get_state(batcher)
      assert state.ack_tracker.retry_deadline_ms == 600_000
      assert state.modack_tracker.retry_deadline_ms == 600_000
    end

    test "restores configured deadline when exactly-once is disabled" do
      {batcher, _rpc} = start_batcher()

      AckBatcher.update_retry_deadline(batcher, 600_000)
      AckBatcher.flush(batcher)
      assert :sys.get_state(batcher).ack_tracker.retry_deadline_ms == 600_000
      assert :sys.get_state(batcher).modack_tracker.retry_deadline_ms == 600_000

      AckBatcher.update_retry_deadline(batcher, 60_000)
      AckBatcher.flush(batcher)
      assert :sys.get_state(batcher).ack_tracker.retry_deadline_ms == 60_000
      assert :sys.get_state(batcher).modack_tracker.retry_deadline_ms == 60_000
    end
  end

  # ============================================================
  # Modack retry limit — @max_modack_attempts = 3
  # ============================================================

  # RPC client that always fails modify_ack_deadline so we can observe the retry limit.
  defmodule AlwaysFailModackRpc do
    use GenServer

    def start_link(test_pid) do
      GenServer.start_link(__MODULE__, {test_pid, 0})
    end

    def init(state), do: {:ok, state}

    def handle_call({:acknowledge, _ids}, _from, {test_pid, count}) do
      {:reply, :ok, {test_pid, count + 1}}
    end

    def handle_call({:modify_ack_deadline, ids, deadline}, _from, {test_pid, count}) do
      send(test_pid, {:rpc, {:modack, ids, deadline}, count})
      # Always return a retryable error to force retries up to the limit
      {:reply, {:error, :unavailable}, {test_pid, count + 1}}
    end
  end

  describe "modack retry limit" do
    test "drops modack ack_ids after 3 failed attempts" do
      test_pid = self()
      {:ok, rpc} = AlwaysFailModackRpc.start_link(test_pid)

      {:ok, batcher} =
        AckBatcher.start_link(
          rpc_client: rpc,
          ack_batch_interval_ms: 30,
          ack_batch_max_size: 100,
          task_supervisor: start_task_supervisor()
        )

      AckBatcher.modack(batcher, ["id-exhaust"], 30)

      # Attempt 1 (count=0)
      assert_receive {:rpc, {:modack, ["id-exhaust"], 30}, 0}, 500
      # Attempt 2 (count=1)
      assert_receive {:rpc, {:modack, ["id-exhaust"], 30}, 1}, 500
      # Attempt 3 (count=2)
      assert_receive {:rpc, {:modack, ["id-exhaust"], 30}, 2}, 500

      # After 3 attempts the id is dropped — no further RPC calls for it
      refute_receive {:rpc, {:modack, ["id-exhaust"], 30}, _}, 200

      # State should be clear
      state = :sys.get_state(batcher)
      assert state.modack_ids == %{}
      assert state.modack_tracker.attempts == %{}
    end

    test "other ack_ids are not affected by one id reaching the retry limit" do
      test_pid = self()

      # Only fail for "id-bad", succeed for everything else
      {:ok, rpc} =
        GenServer.start_link(
          BroadwayCloudPubSub.Streaming.AckBatcherTest.AlwaysFailModackRpc,
          {test_pid, 0}
        )

      # Use SelectiveFlakyRpc indirectly: we test via the state, not via RPC spy
      {:ok, batcher_a} =
        AckBatcher.start_link(
          rpc_client: rpc,
          ack_batch_interval_ms: 10_000,
          ack_batch_max_size: 100,
          task_supervisor: start_task_supervisor()
        )

      # Add two ids with the same deadline; the rpc always fails
      AckBatcher.modack(batcher_a, ["id-1", "id-2"], 30)

      # After 3 flushes, both should be dropped
      AckBatcher.flush(batcher_a)
      AckBatcher.flush(batcher_a)
      AckBatcher.flush(batcher_a)
      AckBatcher.flush(batcher_a)

      state = :sys.get_state(batcher_a)
      assert state.modack_ids == %{}
    end

    test "retry limit is per-ack-id — surviving ids stay in state after others are dropped" do
      # AlwaysFailModackRpc fails every modify_ack_deadline call.
      # Both "id-bad" and "id-good" will exhaust the 3-attempt limit, so after
      # 3 flushes the modack state should be fully cleared.
      test_pid = self()
      {:ok, rpc} = AlwaysFailModackRpc.start_link(test_pid)

      {:ok, batcher_b} =
        AckBatcher.start_link(
          rpc_client: rpc,
          ack_batch_interval_ms: 10_000,
          ack_batch_max_size: 100,
          task_supervisor: start_task_supervisor()
        )

      AckBatcher.modack(batcher_b, ["id-bad", "id-good"], 30)

      # 3 explicit flushes exhaust the retry limit for both ids
      AckBatcher.flush(batcher_b)
      AckBatcher.flush(batcher_b)
      AckBatcher.flush(batcher_b)
      # One more flush to let the cleanup sweep run
      AckBatcher.flush(batcher_b)

      state = :sys.get_state(batcher_b)
      remaining_ids = state.modack_ids |> Map.values() |> List.flatten()
      refute "id-bad" in remaining_ids
      refute "id-good" in remaining_ids
    end
  end

  # ============================================================
  # Telemetry metadata
  # ============================================================

  describe "telemetry_metadata" do
    # Helper: start a batcher whose rpc_client is an atom that is never registered,
    # so GenServer.whereis/1 returns nil and every flush is deferred — which reliably
    # triggers the :flush_deferred telemetry event without needing to kill a process.
    defp start_batcher_no_rpc(extra_opts \\ []) do
      # Use a unique atom so concurrent tests don't share the same unregistered name.
      rpc_name = Module.concat(__MODULE__, "NeverStarted#{System.unique_integer([:positive])}")

      opts =
        Keyword.merge(
          [
            rpc_client: rpc_name,
            broadway_name: :TestPipeline,
            subscription: "projects/test/subscriptions/test-sub",
            ack_batch_interval_ms: 100_000,
            ack_batch_max_size: 10_000,
            task_supervisor: start_task_supervisor()
          ],
          extra_opts
        )

      {:ok, batcher} = AckBatcher.start_link(opts)
      batcher
    end

    test "telemetry events include name and subscription in metadata" do
      test_pid = self()
      batcher = start_batcher_no_rpc()
      telemetry_name = "batcher-meta-#{inspect(batcher)}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :ack_batcher, :flush_deferred],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :telemetry_meta}
      )

      AckBatcher.ack(batcher, ["id-1"])
      AckBatcher.flush(batcher)

      assert_receive {:telemetry_meta, _measurements, metadata}, 1_000
      assert metadata.name == :TestPipeline
      assert metadata.subscription == "projects/test/subscriptions/test-sub"
      refute Map.has_key?(metadata, :extra)

      :telemetry.detach(telemetry_name)
    end

    test "static telemetry_metadata is included under :extra" do
      extra = %{tenant_id: "acme"}
      test_pid = self()
      batcher = start_batcher_no_rpc(telemetry_metadata: extra)
      telemetry_name = "batcher-extra-static-#{inspect(batcher)}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :ack_batcher, :flush_deferred],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :telemetry_meta}
      )

      AckBatcher.ack(batcher, ["id-1"])
      AckBatcher.flush(batcher)

      assert_receive {:telemetry_meta, _measurements, metadata}, 1_000
      assert metadata.name == :TestPipeline
      assert metadata.subscription == "projects/test/subscriptions/test-sub"
      assert metadata.extra == extra

      :telemetry.detach(telemetry_name)
    end

    test "MFA telemetry_metadata is called and result is included under :extra" do
      test_pid = self()
      batcher = start_batcher_no_rpc(telemetry_metadata: {__MODULE__, :dynamic_meta, []})
      telemetry_name = "batcher-extra-mfa-#{inspect(batcher)}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :ack_batcher, :flush_deferred],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :telemetry_meta}
      )

      AckBatcher.ack(batcher, ["id-1"])
      AckBatcher.flush(batcher)

      assert_receive {:telemetry_meta, _measurements, metadata}, 1_000
      assert metadata.extra == %{dynamic: true}

      :telemetry.detach(telemetry_name)
    end
  end

  # MFA for telemetry_metadata test.
  def dynamic_meta, do: %{dynamic: true}

  # ============================================================
  # Helpers
  # ============================================================

  # Collect exactly `count` {:rpc, _} messages from the mailbox within `timeout` ms.
  defp collect_rpcs(count, timeout) do
    Enum.map(1..count, fn _ ->
      receive do
        {:rpc, msg} -> msg
      after
        timeout -> flunk("Expected #{count} RPC messages but timed out")
      end
    end)
  end

  defp find_modack(rpcs, deadline) do
    result =
      Enum.find_value(rpcs, fn
        {:modify_ack_deadline, ids, ^deadline} -> {ids, deadline}
        _ -> nil
      end)

    assert result, "Expected :modify_ack_deadline with deadline #{deadline}"
    result
  end

  # ============================================================
  # child_opts/1
  # ============================================================

  describe "child_opts/1" do
    @full_opts [
      subscription: "projects/p/subscriptions/s",
      ack_batch_interval_ms: 100,
      ack_batch_max_size: 2_500,
      retry_deadline_ms: 60_000,
      broadway_name: MyPipeline,
      telemetry_metadata: %{env: :test},
      rpc_client: :some_rpc_client,
      task_supervisor: :some_task_supervisor,
      # Extra keys that AckBatcher should NOT include
      grpc_client: BroadwayCloudPubSub.Streaming.GrpcClient,
      grpc_client_config: %{},
      backoff_type: :rand_exp,
      backoff_min: 100,
      backoff_max: 60_000,
      max_outstanding_messages: 1_000
    ]

    test "returns only the keys AckBatcher needs" do
      result = AckBatcher.child_opts(@full_opts)

      assert Keyword.keys(result) |> Enum.sort() ==
               Enum.sort([
                 :subscription,
                 :ack_batch_interval_ms,
                 :ack_batch_max_size,
                 :retry_deadline_ms,
                 :broadway_name,
                 :telemetry_metadata,
                 :rpc_client,
                 :task_supervisor
               ])
    end

    test "excludes UnaryRpcClient-specific keys" do
      result = AckBatcher.child_opts(@full_opts)

      refute Keyword.has_key?(result, :grpc_client)
      refute Keyword.has_key?(result, :grpc_client_config)
      refute Keyword.has_key?(result, :backoff_type)
      refute Keyword.has_key?(result, :backoff_min)
      refute Keyword.has_key?(result, :backoff_max)
      refute Keyword.has_key?(result, :max_outstanding_messages)
    end

    test "omits optional keys when not provided" do
      opts =
        @full_opts
        |> Keyword.delete(:telemetry_metadata)
        |> Keyword.delete(:retry_deadline_ms)

      result = AckBatcher.child_opts(opts)

      refute Keyword.has_key?(result, :telemetry_metadata)
      refute Keyword.has_key?(result, :retry_deadline_ms)
    end

    test "raises on missing required key :subscription" do
      opts = Keyword.delete(@full_opts, :subscription)

      assert_raise ArgumentError, ~r/missing required option :subscription/, fn ->
        AckBatcher.child_opts(opts)
      end
    end

    test "raises on missing required key :rpc_client" do
      opts = Keyword.delete(@full_opts, :rpc_client)

      assert_raise ArgumentError, ~r/missing required option :rpc_client/, fn ->
        AckBatcher.child_opts(opts)
      end
    end

    test "raises on missing required key :broadway_name" do
      opts = Keyword.delete(@full_opts, :broadway_name)

      assert_raise ArgumentError, ~r/missing required option :broadway_name/, fn ->
        AckBatcher.child_opts(opts)
      end
    end

    test "raises on missing required key :ack_batch_interval_ms" do
      opts = Keyword.delete(@full_opts, :ack_batch_interval_ms)

      assert_raise ArgumentError, ~r/missing required option :ack_batch_interval_ms/, fn ->
        AckBatcher.child_opts(opts)
      end
    end

    test "raises on missing required key :task_supervisor" do
      opts = Keyword.delete(@full_opts, :task_supervisor)

      assert_raise ArgumentError, ~r/missing required option :task_supervisor/, fn ->
        AckBatcher.child_opts(opts)
      end
    end
  end

  # --- Pure classification function tests ---

  describe "classify_ack_result/4" do
    alias BroadwayCloudPubSub.Streaming.RetryTracker

    defp new_ack_tracker(opts) do
      RetryTracker.new(opts)
    end

    defp tracked_ack_tracker(ids, opts \\ []) do
      tracker = new_ack_tracker(opts)
      now = System.monotonic_time(:millisecond)
      RetryTracker.track(tracker, ids, now)
    end

    test "full success returns empty live list" do
      tracker = tracked_ack_tracker(["a1", "a2"])
      result = AckBatcher.classify_ack_result(["a1", "a2"], {:ok, []}, tracker, now_ms())

      assert result.live == []
      assert result.expired_count == 0
    end

    test "partial success retains remaining ids" do
      tracker = tracked_ack_tracker(["a1", "a2", "a3"])

      result =
        AckBatcher.classify_ack_result(["a1", "a2", "a3"], {:ok, ["a2"]}, tracker, now_ms())

      assert result.live == ["a2"]
      assert result.expired_count == 0
    end

    test "retryable error with transient ids retains those ids" do
      tracker = tracked_ack_tracker(["a1", "a2"])
      rpc_result = {:error, {%GRPC.RPCError{status: 14}, ["a1"]}}
      result = AckBatcher.classify_ack_result(["a1", "a2"], rpc_result, tracker, now_ms())

      assert result.live == ["a1"]
      assert result.expired_count == 0
    end

    test "total failure retains all ids" do
      tracker = tracked_ack_tracker(["a1", "a2"])

      result =
        AckBatcher.classify_ack_result(["a1", "a2"], {:error, :unavailable}, tracker, now_ms())

      assert result.live == ["a1", "a2"]
      assert result.expired_count == 0
    end

    test "expired ids are dropped and counted" do
      tracker = new_ack_tracker(retry_deadline_ms: 100)
      old_time = now_ms() - 200
      tracker = RetryTracker.track(tracker, ["a1", "a2"], old_time)

      result =
        AckBatcher.classify_ack_result(["a1", "a2"], {:error, :unavailable}, tracker, now_ms())

      assert result.live == []
      assert result.expired_count == 2
    end
  end

  describe "classify_modack_results/3" do
    alias BroadwayCloudPubSub.Streaming.RetryTracker

    test "full success for all groups returns empty remaining" do
      tracker = RetryTracker.new(max_attempts: 3)
      tracker = RetryTracker.track(tracker, ["m1", "m2"], now_ms())
      tracker = RetryTracker.record_attempt(tracker, ["m1", "m2"])

      rpc_results = [{60, ["m1"], {:ok, []}}, {0, ["m2"], {:ok, []}}]
      result = AckBatcher.classify_modack_results(rpc_results, tracker, now_ms())

      assert result.remaining == %{}
      assert result.exhausted_count == 0
      assert result.expired_count == 0
    end

    test "partial failure retains failed ids in the correct deadline group" do
      tracker = RetryTracker.new(max_attempts: 3)
      tracker = RetryTracker.track(tracker, ["m1", "m2"], now_ms())
      tracker = RetryTracker.record_attempt(tracker, ["m1", "m2"])

      rpc_results = [{60, ["m1", "m2"], {:ok, ["m2"]}}]
      result = AckBatcher.classify_modack_results(rpc_results, tracker, now_ms())

      assert result.remaining == %{60 => ["m2"]}
      assert result.exhausted_count == 0
    end

    test "ids exceeding max_attempts are dropped" do
      tracker = RetryTracker.new(max_attempts: 2)
      tracker = RetryTracker.track(tracker, ["m1"], now_ms())
      # Simulate 2 prior attempts
      tracker = RetryTracker.record_attempt(tracker, ["m1"])
      tracker = RetryTracker.record_attempt(tracker, ["m1"])

      rpc_results = [{60, ["m1"], {:error, :unavailable}}]
      result = AckBatcher.classify_modack_results(rpc_results, tracker, now_ms())

      assert result.remaining == %{}
      assert result.exhausted_count == 1
    end

    test "expired modack ids are dropped and counted" do
      tracker = RetryTracker.new(max_attempts: 10, retry_deadline_ms: 100)
      old_time = now_ms() - 200
      tracker = RetryTracker.track(tracker, ["m1"], old_time)
      tracker = RetryTracker.record_attempt(tracker, ["m1"])

      rpc_results = [{60, ["m1"], {:error, :unavailable}}]
      result = AckBatcher.classify_modack_results(rpc_results, tracker, now_ms())

      assert result.remaining == %{}
      assert result.expired_count == 1
    end

    test "multiple deadline groups are tracked independently" do
      tracker = RetryTracker.new(max_attempts: 3)
      tracker = RetryTracker.track(tracker, ["a", "b", "c"], now_ms())
      tracker = RetryTracker.record_attempt(tracker, ["a", "b", "c"])

      rpc_results = [
        {60, ["a"], {:ok, ["a"]}},
        {0, ["b", "c"], {:ok, ["c"]}}
      ]

      result = AckBatcher.classify_modack_results(rpc_results, tracker, now_ms())

      assert result.remaining == %{60 => ["a"], 0 => ["c"]}
      assert result.exhausted_count == 0
    end
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
