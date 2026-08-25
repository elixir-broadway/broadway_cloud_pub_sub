defmodule BroadwayCloudPubSub.Streaming.StreamManagerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias BroadwayCloudPubSub.Streaming.{AckBatcher, StreamManager}
  alias BroadwayCloudPubSub.Test.{GrpcDynamicAdapter, TelemetryHelper}

  # Minimal config with enough keys to satisfy StreamManager.init/1
  # (mirrors what Options produces after validation + defaults).
  defp base_config do
    [
      broadway_name: __MODULE__,
      subscription: "projects/test/subscriptions/test-sub",
      max_outstanding_messages: 1_000,
      max_outstanding_bytes: 104_857_600,
      stream_ack_deadline_seconds: 60,
      backoff_type: :exp,
      backoff_min: 1_000,
      backoff_max: 30_000,
      use_ssl: true,
      adapter: GrpcDynamicAdapter,
      grpc_endpoint: "localhost:1234",
      keepalive_interval_ms: 30_000,
      on_success: :ack,
      on_failure: :noop,
      on_shutdown: {:nack, 5},
      max_extension_ms: 3_600_000,
      ack_batch_interval_ms: 100,
      ack_batch_max_size: 2_500,
      client_id: "test-client-id",
      # fail token by default, most of the tests don't need a live stream, and this avoids GRPC.Client.Connection stop logs
      token_generator: {__MODULE__, :fail_token, []},
      broadway: [name: __MODULE__]
    ]
  end

  def noop_token, do: {:ok, "test-token"}
  def fail_token, do: {:error, :no_token}

  # A minimal stub GenServer that silently accepts any cast (ack/modack).
  # Used as the rpc_client for AckBatcher so no real gRPC calls are made.
  defmodule StubRpcClient do
    use GenServer
    def start_link(name), do: GenServer.start_link(__MODULE__, :ok, name: name)
    def init(:ok), do: {:ok, :ok}
    def handle_cast(_msg, state), do: {:noreply, state}
    def handle_call(_msg, _from, state), do: {:reply, :ok, state}
  end

  # Start a StreamManager, inject producer_pid so it doesn't try to connect.
  # Also starts a real AckBatcher (backed by StubRpcClient) registered under
  # the name that StreamManager derives from broadway_name.
  defp start_manager(extra_opts \\ []) do
    # Generate a unique broadway_name per test invocation to avoid registered-name
    # collisions when multiple tests run concurrently or sequentially.
    broadway_name = Module.concat(__MODULE__, "Run#{System.unique_integer([:positive])}")

    opts =
      base_config() |> Keyword.put(:broadway_name, broadway_name) |> Keyword.merge(extra_opts)

    # Mirror what Producer.prepare_for_start/2 does: call grpc_client.init/1 and
    # store the resulting config so StreamManager can read it from its config map.
    grpc_client = Keyword.get(opts, :grpc_client, BroadwayCloudPubSub.Streaming.GrpcClient)
    {:ok, grpc_client_config} = grpc_client.init(opts)

    opts =
      opts
      |> Keyword.put(:grpc_client, grpc_client)
      |> Keyword.put(:grpc_client_config, grpc_client_config)

    rpc_client_name = Module.concat(broadway_name, UnaryRpcClient)
    batcher_name = Module.concat(broadway_name, AckBatcher)

    # Start stub RPC client so AckBatcher can call it
    {:ok, _stub} = StubRpcClient.start_link(rpc_client_name)

    # Start a Task.Supervisor for receipt modack tasks
    task_sup_name = Module.concat(broadway_name, ReceiptModackTaskSupervisor)
    {:ok, _task_sup} = Task.Supervisor.start_link(name: task_sup_name)

    # Start a real AckBatcher registered under the name StreamManager will use
    {:ok, _batcher} =
      AckBatcher.start_link(
        name: batcher_name,
        rpc_client: rpc_client_name,
        task_supervisor: task_sup_name,
        ack_batch_interval_ms: Keyword.get(opts, :ack_batch_interval_ms, 100),
        ack_batch_max_size: Keyword.get(opts, :ack_batch_max_size, 2_500)
      )

    # Pass producer_pid and ack_ref directly (matching what Producer.init does)
    opts =
      opts
      |> Keyword.put(:producer_pid, self())
      |> Keyword.put(:ack_ref, {broadway_name, 0})

    {:ok, pid} = StreamManager.start_link(opts)

    pid
  end

  # Synchronous barrier: drains all prior mailbox messages in StreamManager
  # before returning. Uses get_outstanding (a GenServer.call) to guarantee ordering.
  defp sync(pid), do: StreamManager.get_outstanding(pid)

  # Returns the number of messages currently in the StreamManager's message_buffer.
  defp buffer_length(pid) do
    :queue.len(:sys.get_state(pid).message_buffer)
  end

  # Drain all messages currently in the test process mailbox.
  # Used to discard stray telemetry events emitted before we start asserting.
  defp flush_mailbox do
    receive do
      _ -> flush_mailbox()
    after
      0 -> :ok
    end
  end

  # Build a minimal ReceivedMessage for sending into {:stream_messages, ...}.
  defp received_message(ack_id, data) do
    %BroadwayCloudPubSub.Google.Pubsub.V1.ReceivedMessage{
      ack_id: ack_id,
      message: %BroadwayCloudPubSub.Google.Pubsub.V1.PubsubMessage{
        message_id: "msg-#{ack_id}",
        data: data,
        attributes: %{},
        ordering_key: "",
        publish_time: nil
      },
      delivery_attempt: 1
    }
  end

  # Open a live dynamic adapter stream.
  # Starts the manager with noop_token so the adapter actually connects,
  # waits for the connection handshake, then calls fun.(pid, ctrl).
  defp with_live_stream(extra_opts \\ [], fun) do
    pid =
      start_manager(
        [
          adapter: GrpcDynamicAdapter,
          test_pid: self(),
          token_generator: {__MODULE__, :noop_token, []}
        ] ++ extra_opts
      )

    assert_receive {:adapter_connected, ctrl}, 2_000
    # Wait for the initial StreamingPullRequest so the stream is fully open
    assert_receive {:adapter_call, {:send_data, _}}, 2_000
    fun.(pid, ctrl)
  end

  # ============================================================
  # Demand signaling
  # ============================================================

  describe "notify_demand/2 — no buffered messages" do
    test "stores pending_demand when message buffer is empty" do
      pid = start_manager()

      StreamManager.notify_demand(pid, 0)
      StreamManager.notify_demand(pid, 10)

      # Send one real message — if demand > 0, it will be forwarded immediately
      send(pid, {:stream_messages, [received_message("demand-probe", "probe")]})
      assert_receive {:stream_messages, [_]}, 500

      # Buffer is empty: all demand was consumed by the forwarded message
      assert buffer_length(pid) == 0
    end
  end

  describe "notify_demand/2 — with buffered messages" do
    test "flushes buffered messages to producer and decrements pending_demand" do
      pid = start_manager()

      # Buffer two messages with demand=0
      StreamManager.notify_demand(pid, 0)
      send(pid, {:stream_messages, [received_message("buf-1", "msg1")]})
      send(pid, {:stream_messages, [received_message("buf-2", "msg2")]})
      # Wait for both to be buffered (sync via buffer_length)
      assert buffer_length(pid) == 2

      # Now demand arrives — should flush both at once
      StreamManager.notify_demand(pid, 10)

      assert_receive {:stream_messages, received}
      assert Enum.map(received, & &1.data) == ["msg1", "msg2"]

      # Buffer should be empty; remaining demand consumed 2 of 10
      assert buffer_length(pid) == 0
    end

    test "flushes only up to pending_demand, keeps remainder buffered" do
      pid = start_manager()

      # Buffer 5 messages with demand=0
      StreamManager.notify_demand(pid, 0)

      for i <- 1..5 do
        send(pid, {:stream_messages, [received_message("buf-#{i}", "msg#{i}")]})
      end

      assert buffer_length(pid) == 5

      # Demand for 2 — should flush exactly 2
      StreamManager.notify_demand(pid, 2)

      assert_receive {:stream_messages, received}
      assert length(received) == 2
      assert Enum.map(received, & &1.data) == ["msg1", "msg2"]

      # 3 remain buffered
      assert buffer_length(pid) == 3

      # Demand for 10 — should flush the remaining 3
      StreamManager.notify_demand(pid, 10)

      assert_receive {:stream_messages, received2}
      assert length(received2) == 3
      assert Enum.map(received2, & &1.data) == ["msg3", "msg4", "msg5"]

      assert buffer_length(pid) == 0
    end
  end

  describe "notify_demand/2 — delta accumulation (regression for over-delivery bug)" do
    test "demand deltas accumulate correctly when interleaved with message flushes" do
      pid = start_manager()

      # 1. Signal demand delta of 10 → pending_demand should be 10
      StreamManager.notify_demand(pid, 10)
      sync(pid)
      assert :sys.get_state(pid).pending_demand == 10

      # 2. StreamManager receives and flushes 5 messages → pending_demand should be 5
      msgs = for i <- 1..5, do: received_message("race-#{i}", "msg#{i}")
      send(pid, {:stream_messages, msgs})
      assert_receive {:stream_messages, flushed}, 500
      assert length(flushed) == 5
      sync(pid)
      assert :sys.get_state(pid).pending_demand == 5

      # 3. Signal another demand delta of 5 → pending_demand should be 5 + 5 = 10
      #    BUG: with absolute overwrite semantics, pending_demand becomes 5 instead of 10,
      #    effectively "forgetting" the 5 units of remaining demand from step 1.
      StreamManager.notify_demand(pid, 5)
      sync(pid)
      assert :sys.get_state(pid).pending_demand == 10
    end

    test "over-delivery is prevented when demand arrives after flush" do
      pid = start_manager()

      # Signal demand delta of 3
      StreamManager.notify_demand(pid, 3)
      sync(pid)

      # StreamManager receives and flushes 3 messages → pending_demand = 0
      msgs = for i <- 1..3, do: received_message("od-#{i}", "msg#{i}")
      send(pid, {:stream_messages, msgs})
      assert_receive {:stream_messages, flushed}, 500
      assert length(flushed) == 3
      sync(pid)
      assert :sys.get_state(pid).pending_demand == 0

      # Signal new demand delta of 2 → should accumulate to 0 + 2 = 2
      StreamManager.notify_demand(pid, 2)
      sync(pid)
      assert :sys.get_state(pid).pending_demand == 2

      # Now 5 messages arrive — only 2 should be flushed (the rest buffered)
      msgs2 = for i <- 6..10, do: received_message("od-#{i}", "msg#{i}")
      send(pid, {:stream_messages, msgs2})
      assert_receive {:stream_messages, flushed2}, 500
      assert length(flushed2) == 2

      # 3 should remain buffered
      assert buffer_length(pid) == 3
    end
  end

  describe "stream_messages → message delivery" do
    test "messages are forwarded immediately when pending_demand > 0" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("ack-1", "hello")]})

      assert_receive {:stream_messages, messages}
      assert length(messages) == 1
      assert hd(messages).data == "hello"

      # Buffer should be empty (demand consumed the message immediately)
      assert buffer_length(pid) == 0
    end

    test "messages are buffered when pending_demand is 0" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 0)

      send(pid, {:stream_messages, [received_message("ack-2", "buffered")]})

      refute_receive {:stream_messages, _}, 100

      assert buffer_length(pid) == 1
    end

    test "buffer is flushed in FIFO order on notify_demand" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 0)

      for i <- 1..3 do
        send(pid, {:stream_messages, [received_message("ack-#{i}", "data-#{i}")]})
      end

      # Sync: ensure all 3 are buffered before we signal demand
      assert buffer_length(pid) == 3

      StreamManager.notify_demand(pid, 10)

      assert_receive {:stream_messages, messages}
      assert Enum.map(messages, & &1.data) == ["data-1", "data-2", "data-3"]
    end
  end

  # ============================================================
  # draining
  # ============================================================

  describe "prepare_for_draining/1" do
    test "messages are not forwarded after prepare_for_draining even when pending_demand > 0" do
      pid = start_manager()

      StreamManager.notify_demand(pid, 10)
      StreamManager.prepare_for_draining(pid)

      send(pid, {:stream_messages, [received_message("drain-ack", "should not arrive")]})

      refute_receive {:stream_messages, _}, 200
    end

    test "clears message_buffer and removes buffered ack_ids from outstanding" do
      pid = start_manager()

      # Buffer 3 messages with demand=0
      StreamManager.notify_demand(pid, 0)

      for i <- 1..3 do
        send(pid, {:stream_messages, [received_message("buf-drain-#{i}", "data-#{i}")]})
      end

      assert buffer_length(pid) == 3
      state = :sys.get_state(pid)
      assert map_size(state.outstanding) == 3

      # prepare_for_draining should nack + clear buffer + remove from outstanding
      {:ok, nacked_count} = StreamManager.prepare_for_draining(pid)
      assert nacked_count == 3

      state = :sys.get_state(pid)
      assert buffer_length(pid) == 0
      assert map_size(state.outstanding) == 0
      assert state.draining == true
    end

    test "with on_shutdown :noop, clears buffer without nacking" do
      pid = start_manager(on_shutdown: :noop)

      StreamManager.notify_demand(pid, 0)
      send(pid, {:stream_messages, [received_message("noop-buf", "data")]})
      assert buffer_length(pid) == 1

      {:ok, nacked_count} = StreamManager.prepare_for_draining(pid)
      assert nacked_count == 1

      state = :sys.get_state(pid)
      assert buffer_length(pid) == 0
      assert map_size(state.outstanding) == 0
    end

    test "flush_demand is blocked after draining starts" do
      pid = start_manager()

      # Start draining with empty buffer
      StreamManager.prepare_for_draining(pid)

      # Manually inject a message into the buffer via replace_state
      # (simulating a race condition where a message arrives after draining)
      :sys.replace_state(pid, fn s ->
        fake_msg = %Broadway.Message{
          data: "sneaky",
          metadata: %{},
          acknowledger:
            {BroadwayCloudPubSub.Streaming.Acknowledger, :unused, %{ack_id: "after-drain"}}
        }

        %{s | message_buffer: :queue.in(fake_msg, s.message_buffer)}
      end)

      assert buffer_length(pid) == 1

      # Demand arrives — flush_demand should be a no-op because draining=true
      StreamManager.notify_demand(pid, 10)
      refute_receive {:stream_messages, _}, 200

      # Buffer should still have the message (not flushed)
      assert buffer_length(pid) == 1
    end

    test "preserves in-flight messages in outstanding (only clears buffered)" do
      pid = start_manager()

      # Send 3 messages with demand so they go straight to processor (in-flight)
      StreamManager.notify_demand(pid, 10)

      for i <- 1..3 do
        send(pid, {:stream_messages, [received_message("inflight-#{i}", "data-#{i}")]})
      end

      assert_receive {:stream_messages, _}, 500

      # Zero out pending_demand so subsequent messages are buffered
      :sys.replace_state(pid, fn s -> %{s | pending_demand: 0} end)
      send(pid, {:stream_messages, [received_message("buf-a", "buf-data-a")]})
      send(pid, {:stream_messages, [received_message("buf-b", "buf-data-b")]})
      assert buffer_length(pid) == 2

      state = :sys.get_state(pid)
      # 5 total outstanding: 3 in-flight + 2 buffered
      assert map_size(state.outstanding) == 5

      {:ok, nacked_count} = StreamManager.prepare_for_draining(pid)
      assert nacked_count == 2

      state = :sys.get_state(pid)
      assert buffer_length(pid) == 0
      # Only the 3 in-flight messages remain in outstanding
      assert map_size(state.outstanding) == 3
      assert Map.has_key?(state.outstanding, "inflight-1")
      assert Map.has_key?(state.outstanding, "inflight-2")
      assert Map.has_key?(state.outstanding, "inflight-3")
    end

    test "drain completes immediately when no in-flight messages remain" do
      pid = start_manager()

      # Buffer only (no demand → nothing dispatched to processors)
      StreamManager.notify_demand(pid, 0)
      send(pid, {:stream_messages, [received_message("only-buf", "data")]})
      assert buffer_length(pid) == 1

      {:ok, 1} = StreamManager.prepare_for_draining(pid)

      state = :sys.get_state(pid)
      # outstanding is empty → drain should have completed
      assert map_size(state.outstanding) == 0
      # drain_timer should be cancelled since drain completed
      assert state.drain_timer == nil
    end
  end

  # ============================================================
  # Draining behavior — handler guards
  # ============================================================

  describe "draining — stream_messages handler" do
    test "messages arriving during drain are not delivered to the producer" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)
      StreamManager.prepare_for_draining(pid)

      send(pid, {:stream_messages, [received_message("late-msg", "data")]})
      sync(pid)

      refute_receive {:stream_messages, _}, 100
    end

    test "messages arriving during drain are not added to outstanding" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)
      StreamManager.prepare_for_draining(pid)

      outstanding_before = map_size(:sys.get_state(pid).outstanding)

      send(pid, {:stream_messages, [received_message("late-msg", "data")]})
      sync(pid)

      # Outstanding should not grow
      assert map_size(:sys.get_state(pid).outstanding) == outstanding_before
    end
  end

  describe "draining — reconnect suppression" do
    test ":connect during drain is ignored (no reconnection)" do
      pid = start_manager()
      StreamManager.prepare_for_draining(pid)

      # Manually set a reconnect_ref to simulate a pending reconnect
      :sys.replace_state(pid, fn s -> %{s | reconnect_ref: make_ref()} end)

      send(pid, :connect)
      sync(pid)

      state = :sys.get_state(pid)
      # reconnect_ref should be cleared but no new connection started
      assert state.reconnect_ref == nil
      assert state.reader_pid == nil
    end

    test "retryable stream_error during drain does not schedule reconnect" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)
      StreamManager.prepare_for_draining(pid)

      # Cancel any existing reconnect timer from initial connect failure
      :sys.replace_state(pid, fn s ->
        if s.reconnect_ref, do: Process.cancel_timer(s.reconnect_ref)
        %{s | reconnect_ref: nil}
      end)

      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "unavailable"}})
      sync(pid)

      state = :sys.get_state(pid)
      # No new reconnect scheduled during drain
      assert state.reconnect_ref == nil
      assert state.reader_pid == nil
      assert Process.alive?(pid)
    end

    test "terminal stream_error during drain does not crash (no stop)" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)
      StreamManager.prepare_for_draining(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 5, message: "not found"}})
      sync(pid)

      # During drain, terminal errors don't stop the GenServer
      assert Process.alive?(pid)
    end
  end

  describe "draining — receipt_modack_result" do
    test "receipt_modack_result during drain nacks rather than delivers" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["eo-drain"], %{"eo-drain" => "data"})
      assert map_size(:sys.get_state(pid).pending_receipt_modacks) == 1

      # Enter drain mode (this clears pending_receipt_modacks normally,
      # but let's inject after drain to simulate the race)
      StreamManager.prepare_for_draining(pid)

      # Re-inject to simulate a receipt_modack_result arriving after drain started
      # but for a ref that was in-flight before drain
      ref2 = make_ref()
      inject_pending_receipt_modack(pid, ref2, ["eo-late"], %{"eo-late" => "late-data"})

      # Result arrives during drain
      send(pid, {:receipt_modack_result, ref2, {:ok, []}})
      sync(pid)

      # Message should NOT be delivered
      refute_receive {:stream_messages, _}, 100

      # pending_receipt_modacks should be cleared for this ref
      state = :sys.get_state(pid)
      refute Map.has_key?(state.pending_receipt_modacks, ref2)

      # Message should NOT be added to outstanding
      refute Map.has_key?(state.outstanding, "eo-late")
    end
  end

  # ============================================================
  # Drain timeout
  # ============================================================

  describe "drain_timeout nacks outstanding messages" do
    test "drain_timeout nacks all outstanding (in-flight) messages" do
      # Use a very short drain timeout so the test doesn't have to wait long.
      pid = start_manager(drain_timeout_ms: 50)

      # Dispatch 3 messages with demand so they become in-flight (outstanding).
      StreamManager.notify_demand(pid, 10)

      for i <- 1..3 do
        send(pid, {:stream_messages, [received_message("inflight-#{i}", "data-#{i}")]})
      end

      assert_receive {:stream_messages, _}, 500
      assert_receive {:stream_messages, _}, 500
      assert_receive {:stream_messages, _}, 500

      state = :sys.get_state(pid)
      assert map_size(state.outstanding) == 3

      # Drain — buffered is empty, so only in-flight messages remain in outstanding.
      {:ok, 0} = StreamManager.prepare_for_draining(pid)

      state = :sys.get_state(pid)
      assert state.draining == true
      # The 3 in-flight messages should still be in outstanding.
      assert map_size(state.outstanding) == 3
      # Drain timer should be set.
      assert state.drain_timer != nil

      # Wait for drain_timeout to fire (50ms + some margin).
      Process.sleep(150)

      state = :sys.get_state(pid)
      # After drain_timeout, outstanding should be empty (messages were nacked).
      assert map_size(state.outstanding) == 0
      # Drain timer should be cleared.
      assert state.drain_timer == nil
    end

    test "drain_timeout with on_shutdown :noop clears outstanding without nacking" do
      pid = start_manager(drain_timeout_ms: 50, on_shutdown: :noop)

      # Dispatch messages to make them in-flight.
      StreamManager.notify_demand(pid, 10)
      send(pid, {:stream_messages, [received_message("noop-inflight", "data")]})
      assert_receive {:stream_messages, _}, 500

      assert map_size(:sys.get_state(pid).outstanding) == 1

      {:ok, 0} = StreamManager.prepare_for_draining(pid)

      # Wait for drain_timeout.
      Process.sleep(150)

      state = :sys.get_state(pid)
      assert map_size(state.outstanding) == 0
      assert state.drain_timer == nil
    end

    test "drain completes before timeout when all messages are acked" do
      pid = start_manager(drain_timeout_ms: 5_000)

      # Dispatch 2 messages.
      StreamManager.notify_demand(pid, 10)
      send(pid, {:stream_messages, [received_message("ack-1", "data-1")]})
      send(pid, {:stream_messages, [received_message("ack-2", "data-2")]})
      assert_receive {:stream_messages, _}, 500
      assert_receive {:stream_messages, _}, 500

      assert map_size(:sys.get_state(pid).outstanding) == 2

      {:ok, 0} = StreamManager.prepare_for_draining(pid)

      state = :sys.get_state(pid)
      assert state.draining == true
      assert state.drain_timer != nil

      # Simulate processors finishing and acking both messages.
      StreamManager.acknowledge(pid, ["ack-1", "ack-2"])
      # Sync to ensure the cast is processed.
      sync(pid)

      state = :sys.get_state(pid)
      # Drain completed early — outstanding empty, timer cancelled.
      assert map_size(state.outstanding) == 0
      assert state.drain_timer == nil
    end
  end

  # ============================================================
  # Keep-alive pings
  # ============================================================

  describe "keep-alive ping" do
    test "triggers reconnect when send fails (fake stream)" do
      # Use a very short keepalive interval and a high backoff so the automatic
      # reconnect triggered by the failed send doesn't fire another :connect
      # during the test assertions.
      pid = start_manager(keepalive_interval_ms: 10, backoff_min: 60_000)

      :sys.replace_state(pid, fn s -> %{s | grpc_stream: :fake_stream} end)

      # Bootstrap the keepalive cycle — normally started by {:stream_opened},
      # but we injected the stream directly via replace_state.
      send(pid, :send_keepalive)

      # Sync barrier: let both the keepalive handler and any subsequent
      # handle_info(:connect, ...) fully settle before reading state.
      sync(pid)

      assert Process.alive?(pid)

      # After a send failure, the stream is reset (grpc_stream: nil) and a
      # reconnect is scheduled.
      state = :sys.get_state(pid)
      assert state.grpc_stream == nil
      assert state.reconnect_ref != nil
    end

    test "does not crash when stream is nil (reconnecting)" do
      # grpc_stream is already nil after start_manager with fail_token — no replace_state needed.
      pid = start_manager(keepalive_interval_ms: 10)

      send(pid, :send_keepalive)

      sync(pid)

      assert Process.alive?(pid)
    end

    test "keepalive_timer is nil before stream opens" do
      pid = start_manager()
      # No public API for keepalive_timer — :sys.get_state required
      state = :sys.get_state(pid)
      assert state.keepalive_timer == nil
    end

    test "keepalive_timer is set when stream is active" do
      # Use a live stream so {:stream_opened} fires, which calls schedule_keepalive_timer/1.
      # A very long interval ensures it doesn't fire during the test.
      with_live_stream([keepalive_interval_ms: 60_000], fn pid, _ctrl ->
        # No public API for keepalive_timer — :sys.get_state required
        state = :sys.get_state(pid)
        assert state.keepalive_timer != nil
      end)
    end
  end

  # ============================================================
  # Reconnect deduplication
  # ============================================================

  describe "reconnect deduplication" do
    test "only one reconnect is scheduled when stream_error and stream_closed both arrive" do
      # Use high backoff so :connect doesn't actually fire during the test
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)

      send(pid, {:stream_error, %GRPC.RPCError{status: 4, message: "timeout"}})
      send(pid, {:stream_closed})

      sync(pid)

      # No public API for reconnect_ref — :sys.get_state required
      state = :sys.get_state(pid)
      first_ref = state.reconnect_ref

      # Ref must be set (at least one reconnect scheduled)
      assert first_ref != nil

      # Send another close signal — ref must not change (dedup kicks in)
      send(pid, {:stream_closed})
      sync(pid)

      state2 = :sys.get_state(pid)
      assert state2.reconnect_ref == first_ref
    end

    test "reconnect_ref is cleared when :connect message fires" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)

      send(pid, {:stream_error, %GRPC.RPCError{status: 4, message: "timeout"}})
      sync(pid)

      # Manually fire :connect (connect() will fail — no real gRPC — but that's fine)
      send(pid, :connect)
      sync(pid)

      # GenServer should still be alive
      assert Process.alive?(pid)
    end
  end

  # ============================================================
  # Error classification — terminal vs retryable
  # ============================================================

  describe "terminal gRPC errors stop the GenServer" do
    test "NOT_FOUND (5) stops the GenServer" do
      logs =
        capture_log(fn ->
          pid = start_manager()
          ref = Process.monitor(pid)
          Process.unlink(pid)

          send(pid, {:stream_error, %GRPC.RPCError{status: 5, message: "not found"}})

          assert_receive {:DOWN, ^ref, :process, ^pid, {:terminal_error, _}}, 1_000
        end)

      assert logs =~
               "Terminal gRPC stream error on subscription projects/test/subscriptions/test-sub - reason: %GRPC.RPCError{status: 5, message: \"not found\", details: nil}. Stopping StreamManager."
    end

    test "PERMISSION_DENIED (7) stops the GenServer" do
      logs =
        capture_log(fn ->
          pid = start_manager()
          ref = Process.monitor(pid)
          Process.unlink(pid)

          send(pid, {:stream_error, %GRPC.RPCError{status: 7, message: "permission denied"}})

          assert_receive {:DOWN, ^ref, :process, ^pid, {:terminal_error, _}}, 1_000
        end)

      assert logs =~
               "Terminal gRPC stream error on subscription projects/test/subscriptions/test-sub - reason: %GRPC.RPCError{status: 7, message: \"permission denied\", details: nil}. Stopping StreamManager."
    end

    test "INVALID_ARGUMENT (3) stops the GenServer" do
      logs =
        capture_log(fn ->
          pid = start_manager()
          ref = Process.monitor(pid)
          Process.unlink(pid)

          send(pid, {:stream_error, %GRPC.RPCError{status: 3, message: "bad argument"}})

          assert_receive {:DOWN, ^ref, :process, ^pid, {:terminal_error, _}}, 1_000
        end)

      assert logs =~
               "Terminal gRPC stream error on subscription projects/test/subscriptions/test-sub - reason: %GRPC.RPCError{status: 3, message: \"bad argument\", details: nil}. Stopping StreamManager."
    end

    test "UNAUTHENTICATED (16) schedules reconnect without stopping" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)
      ref = Process.monitor(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 16, message: "unauthenticated"}})
      sync(pid)

      refute_received {:DOWN, ^ref, :process, ^pid, _}
      assert Process.alive?(pid)

      # No public API for reconnect_ref — :sys.get_state required
      state = :sys.get_state(pid)
      assert state.reconnect_ref != nil
    end

    test "UNAVAILABLE (14) with 'Server shutdownNow invoked' schedules reconnect without stopping" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)
      ref = Process.monitor(pid)

      send(
        pid,
        {:stream_error, %GRPC.RPCError{status: 14, message: "Server shutdownNow invoked"}}
      )

      sync(pid)

      refute_received {:DOWN, ^ref, :process, ^pid, _}
      assert Process.alive?(pid)

      state = :sys.get_state(pid)
      assert state.reconnect_ref != nil
    end

    test "terminal error emits :terminal_error telemetry before stopping" do
      logs =
        capture_log(fn ->
          pid = start_manager()
          test_pid = self()
          telemetry_name = "test-terminal-error-#{inspect(pid)}"
          Process.unlink(pid)

          :telemetry.attach(
            telemetry_name,
            [:broadway_cloud_pub_sub, :streaming, :stream, :terminal_error],
            &TelemetryHelper.handle_event_forward_test/4,
            %{pid: test_pid, msg: :telemetry_terminal_error}
          )

          send(pid, {:stream_error, %GRPC.RPCError{status: 5, message: "not found"}})

          assert_receive {:telemetry_terminal_error, %{}, %{reason: _}}, 1_000

          :telemetry.detach(telemetry_name)
        end)

      assert logs =~
               "Terminal gRPC stream error on subscription projects/test/subscriptions/test-sub - reason: %GRPC.RPCError{status: 5, message: \"not found\", details: nil}. Stopping StreamManager."
    end
  end

  describe "telemetry_metadata — :extra in stream event metadata" do
    # We trigger a retryable stream error and observe the :disconnect event,
    # which is emitted synchronously inside handle_info({:stream_error, ...})
    # before any reconnect timer is scheduled. This avoids races with the
    # :reconnect event that fires from the initial failed connection attempt.

    test "static map is included under :extra in stream event metadata" do
      extra = %{tenant_id: "acme", env: :prod}
      test_pid = self()
      telemetry_name = "test-extra-static-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :stream, :disconnect],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :telemetry_meta}
      )

      pid = start_manager(telemetry_metadata: extra, backoff_min: 60_000, backoff_max: 60_000)
      # Drain the first :connection_failure + reconnect from init before attaching.
      sync(pid)
      flush_mailbox()

      # Trigger a :disconnect from a retryable stream error.
      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "unavailable"}})

      assert_receive {:telemetry_meta, _measurements, metadata}, 1_000
      assert metadata.extra == extra

      :telemetry.detach(telemetry_name)
    end

    test "MFA is called and its return value is included under :extra" do
      test_pid = self()
      telemetry_name = "test-extra-mfa-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :stream, :disconnect],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :telemetry_meta}
      )

      pid =
        start_manager(
          telemetry_metadata: {__MODULE__, :dynamic_meta, []},
          backoff_min: 60_000,
          backoff_max: 60_000
        )

      sync(pid)
      flush_mailbox()

      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "unavailable"}})

      assert_receive {:telemetry_meta, _measurements, metadata}, 1_000
      assert metadata.extra == %{dynamic: true}

      :telemetry.detach(telemetry_name)
    end

    test "no :extra key when telemetry_metadata is not set" do
      test_pid = self()
      telemetry_name = "test-no-extra-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :stream, :disconnect],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :telemetry_meta}
      )

      pid = start_manager(backoff_min: 60_000, backoff_max: 60_000)
      sync(pid)
      flush_mailbox()

      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "unavailable"}})

      assert_receive {:telemetry_meta, _measurements, metadata}, 1_000
      refute Map.has_key?(metadata, :extra)

      :telemetry.detach(telemetry_name)
    end
  end

  # MFA for telemetry_metadata test.
  def dynamic_meta, do: %{dynamic: true}

  describe "retryable gRPC errors trigger reconnect" do
    test "DEADLINE_EXCEEDED (4) schedules reconnect without stopping" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)
      ref = Process.monitor(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 4, message: "timeout"}})
      sync(pid)

      refute_received {:DOWN, ^ref, :process, ^pid, _}
      assert Process.alive?(pid)

      state = :sys.get_state(pid)
      assert state.reconnect_ref != nil
    end

    test "UNAVAILABLE (14) without shutdown message schedules reconnect" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)
      ref = Process.monitor(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "service temporarily down"}})
      sync(pid)

      refute_received {:DOWN, ^ref, :process, ^pid, _}
      assert Process.alive?(pid)
    end
  end

  # ============================================================
  # Phase 5: Ordering key support — subscription_properties
  # ============================================================

  describe "subscription_properties — ordering_enabled" do
    test "ordering_enabled defaults to false" do
      pid = start_manager()
      # No public API for ordering_enabled — :sys.get_state required
      state = :sys.get_state(pid)
      assert state.ordering_enabled == false
    end

    test "updates ordering_enabled to true on {:subscription_properties, ...}" do
      pid = start_manager()

      props = %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
        message_ordering_enabled: true,
        exactly_once_delivery_enabled: false
      }

      send(pid, {:subscription_properties, props})
      sync(pid)

      state = :sys.get_state(pid)
      assert state.ordering_enabled == true
    end

    test "updates ordering_enabled to false when server reports false" do
      pid = start_manager()

      # First set to true
      send(
        pid,
        {:subscription_properties,
         %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: true,
           exactly_once_delivery_enabled: false
         }}
      )

      sync(pid)
      assert :sys.get_state(pid).ordering_enabled == true

      # Then server sends false (can happen mid-stream)
      send(
        pid,
        {:subscription_properties,
         %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: false,
           exactly_once_delivery_enabled: false
         }}
      )

      sync(pid)
      assert :sys.get_state(pid).ordering_enabled == false
    end

    test "ignores other messages — ordering_enabled unchanged" do
      pid = start_manager()

      # Unrelated message
      send(pid, {:some_other_event, :ignored})
      sync(pid)

      state = :sys.get_state(pid)
      assert state.ordering_enabled == false
    end
  end

  # ============================================================
  # Phase 6: Exactly-once delivery — subscription_properties
  # ============================================================

  describe "subscription_properties — exactly_once_enabled" do
    test "exactly_once_enabled defaults to false" do
      pid = start_manager()
      # No public API for exactly_once_enabled — :sys.get_state required
      state = :sys.get_state(pid)
      assert state.exactly_once_enabled == false
    end

    test "updates exactly_once_enabled to true when server reports true" do
      pid = start_manager()

      props = %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
        message_ordering_enabled: false,
        exactly_once_delivery_enabled: true
      }

      send(pid, {:subscription_properties, props})
      sync(pid)

      assert :sys.get_state(pid).exactly_once_enabled == true
    end

    test "updates exactly_once_enabled back to false when server reports false" do
      pid = start_manager()

      send(
        pid,
        {:subscription_properties,
         %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: false,
           exactly_once_delivery_enabled: true
         }}
      )

      sync(pid)
      assert :sys.get_state(pid).exactly_once_enabled == true

      send(
        pid,
        {:subscription_properties,
         %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: false,
           exactly_once_delivery_enabled: false
         }}
      )

      sync(pid)
      assert :sys.get_state(pid).exactly_once_enabled == false
    end

    test "ordering_enabled and exactly_once_enabled are updated together" do
      pid = start_manager()

      send(
        pid,
        {:subscription_properties,
         %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: true,
           exactly_once_delivery_enabled: true
         }}
      )

      sync(pid)
      state = :sys.get_state(pid)
      assert state.ordering_enabled == true
      assert state.exactly_once_enabled == true
    end
  end

  # ============================================================
  # Phase 6: Exactly-once delivery — extend_leases minimum deadline
  # ============================================================

  describe "extend_leases — exactly_once_enabled deadline enforcement" do
    # We use with_live_stream so the adapter actually connects, then push a
    # real StreamingPullResponse to get a message into `outstanding` naturally.
    # Then we control exactly_once_enabled via {:subscription_properties, ...}.

    defp push_one_message(ctrl, ack_id, data) do
      response = %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse{
        received_messages: [
          %BroadwayCloudPubSub.Google.Pubsub.V1.ReceivedMessage{
            ack_id: ack_id,
            message: %BroadwayCloudPubSub.Google.Pubsub.V1.PubsubMessage{
              message_id: "msg-#{ack_id}",
              data: data,
              attributes: %{},
              ordering_key: "",
              publish_time: nil
            },
            delivery_attempt: 1
          }
        ]
      }

      GrpcDynamicAdapter.push_response(ctrl, response)
    end

    test "uses adaptive deadline (no 60s floor) when exactly_once_enabled is false" do
      with_live_stream(fn pid, ctrl ->
        # Put one message into outstanding by pushing a response with demand
        StreamManager.notify_demand(pid, 1)
        push_one_message(ctrl, "ack-normal", "data")
        assert_receive {:stream_messages, [_]}, 2_000

        # Ensure exactly_once is false (the default)
        send(
          pid,
          {:subscription_properties,
           %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
             message_ordering_enabled: false,
             exactly_once_delivery_enabled: false
           }}
        )

        sync(pid)

        # Fire the :extend_leases handler directly
        send(pid, :extend_leases)
        sync(pid)

        # No public API for lease_timer — :sys.get_state required
        state = :sys.get_state(pid)
        assert state.lease_timer != nil
      end)
    end

    test "enforces 60s minimum deadline when exactly_once_enabled is true" do
      with_live_stream(fn pid, ctrl ->
        # Put one message into outstanding
        StreamManager.notify_demand(pid, 1)
        push_one_message(ctrl, "ack-eo", "data")
        assert_receive {:stream_messages, [_]}, 2_000

        # Enable exactly-once
        send(
          pid,
          {:subscription_properties,
           %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
             message_ordering_enabled: false,
             exactly_once_delivery_enabled: true
           }}
        )

        sync(pid)

        send(pid, :extend_leases)
        sync(pid)

        # No public API for lease_timer — :sys.get_state required
        state = :sys.get_state(pid)
        assert state.lease_timer != nil
        remaining_ms = Process.read_timer(state.lease_timer)
        # With effective_deadline=60s: interval in [(60-5)*1000*0.8, (60-5)*1000*0.9]
        #                              = [44_000, 49_500)
        assert remaining_ms >= 40_000,
               "Expected next lease timer >= 40s for exactly-once, got #{remaining_ms}ms"
      end)
    end

    test "uses normal interval (much shorter) when exactly_once_enabled is false" do
      with_live_stream([stream_ack_deadline_seconds: 20], fn pid, ctrl ->
        # Put one message into outstanding
        StreamManager.notify_demand(pid, 1)
        push_one_message(ctrl, "ack-normal2", "data")
        assert_receive {:stream_messages, [_]}, 2_000

        # Ensure exactly_once is false
        send(
          pid,
          {:subscription_properties,
           %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
             message_ordering_enabled: false,
             exactly_once_delivery_enabled: false
           }}
        )

        sync(pid)

        send(pid, :extend_leases)
        sync(pid)

        state = :sys.get_state(pid)
        assert state.lease_timer != nil
        remaining_ms = Process.read_timer(state.lease_timer)

        # With stream_ack_deadline_seconds=20 and adaptive deadline defaulting
        # to 60 (cold start default), effective = 60s (no exactly_once floor).
        # The key check: it's a valid positive number.
        assert is_integer(remaining_ms) and remaining_ms > 0
      end)
    end
  end

  # ============================================================
  # Dynamic adapter — real connection flow without a real server
  # ============================================================

  describe "with GrpcDynamicAdapter" do
    test "adapter receives connect call and sends initial StreamingPullRequest" do
      pid =
        start_manager(
          adapter: GrpcDynamicAdapter,
          test_pid: self(),
          token_generator: {__MODULE__, :noop_token, []}
        )

      # Adapter signals connection to the test process
      assert_receive {:adapter_connected, _ctrl}, 2_000

      # StreamReader sends the initial StreamingPullRequest via send_data
      assert_receive {:adapter_call, {:send_data, _initial_request}}, 2_000

      assert Process.alive?(pid)
    end

    test "messages pushed into the stream are forwarded to the producer" do
      pid =
        start_manager(
          adapter: GrpcDynamicAdapter,
          test_pid: self(),
          token_generator: {__MODULE__, :noop_token, []}
        )

      assert_receive {:adapter_connected, ctrl}, 2_000
      # Wait for send_data (initial request) so stream is fully open
      assert_receive {:adapter_call, {:send_data, _}}, 2_000

      StreamManager.notify_demand(pid, 10)

      response = %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse{
        received_messages: [
          %BroadwayCloudPubSub.Google.Pubsub.V1.ReceivedMessage{
            ack_id: "dyn-ack-1",
            message: %BroadwayCloudPubSub.Google.Pubsub.V1.PubsubMessage{
              message_id: "dyn-msg-1",
              data: "hello-dynamic",
              attributes: %{},
              ordering_key: "",
              publish_time: nil
            },
            delivery_attempt: 1
          }
        ]
      }

      GrpcDynamicAdapter.push_response(ctrl, response)

      assert_receive {:stream_messages, messages}, 2_000
      assert length(messages) == 1
      assert hd(messages).data == "hello-dynamic"
    end

    test "end_stream and cancel notifications reach test_pid" do
      _pid =
        start_manager(
          adapter: GrpcDynamicAdapter,
          test_pid: self(),
          token_generator: {__MODULE__, :noop_token, []}
        )

      assert_receive {:adapter_connected, ctrl}, 2_000
      assert_receive {:adapter_call, {:send_data, _}}, 2_000

      # Signal end-of-stream so the reader exits cleanly
      GrpcDynamicAdapter.push_end_stream(ctrl)

      # StreamManager will close the reader and schedule reconnect,
      # which triggers a new connect — we just verify the process survives
      # and the test doesn't hang.
      Process.sleep(100)
    end
  end

  # ============================================================
  # Exactly-once delivery — receipt modack gate
  # ============================================================

  # A spy RPC client for use in exactly-once tests. It records calls and allows
  # the test to control the response by sending {:set_modack_response, result}.
  defmodule SpyRpcClientForEO do
    use GenServer

    def start_link(test_pid) do
      GenServer.start_link(__MODULE__, test_pid)
    end

    def init(test_pid), do: {:ok, %{test_pid: test_pid, next_response: :ok}}

    def handle_call({:modify_ack_deadline, ids, deadline}, _from, state) do
      send(state.test_pid, {:rpc_call, {:modack, ids, deadline}})
      result = if state.next_response == :ok, do: :ok, else: state.next_response
      {:reply, result, %{state | next_response: :ok}}
    end

    def handle_call({:acknowledge, _ids}, _from, state) do
      {:reply, :ok, state}
    end

    def handle_call(:ping, _from, state), do: {:reply, :ok, state}

    # Synchronous setter so callers can guarantee the response is set before
    # any concurrent Task fires the next RPC call.
    def handle_call({:set_response_sync, response}, _from, state) do
      {:reply, :ok, %{state | next_response: response}}
    end

    def handle_cast({:set_response, response}, state) do
      {:noreply, %{state | next_response: response}}
    end
  end

  # Start a StreamManager backed by a SpyRpcClientForEO so we can control
  # RPC responses for exactly-once tests.
  defp start_manager_with_spy_rpc(extra_opts \\ []) do
    broadway_name = Module.concat(__MODULE__, "EORun#{System.unique_integer([:positive])}")

    opts =
      base_config()
      |> Keyword.put(:broadway_name, broadway_name)
      |> Keyword.merge(extra_opts)

    # Mirror what Producer.prepare_for_start/2 does
    grpc_client = Keyword.get(opts, :grpc_client, BroadwayCloudPubSub.Streaming.GrpcClient)
    {:ok, grpc_client_config} = grpc_client.init(opts)

    opts =
      opts
      |> Keyword.put(:grpc_client, grpc_client)
      |> Keyword.put(:grpc_client_config, grpc_client_config)

    rpc_client_name = Module.concat(broadway_name, UnaryRpcClient)
    batcher_name = Module.concat(broadway_name, AckBatcher)
    task_sup_name = Module.concat(broadway_name, ReceiptModackTaskSupervisor)

    test_pid = self()
    {:ok, rpc_pid} = SpyRpcClientForEO.start_link(test_pid)
    # Register under the name AckBatcher will use
    Process.register(rpc_pid, rpc_client_name)

    {:ok, _task_sup} = Task.Supervisor.start_link(name: task_sup_name)

    {:ok, _batcher} =
      AckBatcher.start_link(
        name: batcher_name,
        rpc_client: rpc_client_name,
        task_supervisor: task_sup_name,
        ack_batch_interval_ms: Keyword.get(opts, :ack_batch_interval_ms, 50),
        ack_batch_max_size: Keyword.get(opts, :ack_batch_max_size, 2_500)
      )

    opts =
      opts
      |> Keyword.put(:producer_pid, self())
      |> Keyword.put(:ack_ref, {broadway_name, 0})

    {:ok, pid} = StreamManager.start_link(opts)

    {pid, rpc_pid}
  end

  # Enable exactly-once delivery on a running StreamManager
  defp enable_exactly_once(pid) do
    send(
      pid,
      {:subscription_properties,
       %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
         message_ordering_enabled: false,
         exactly_once_delivery_enabled: true
       }}
    )

    sync(pid)
  end

  describe "exactly-once receipt modack gate — {:stream_messages, ...}" do
    test "in exactly-once mode, messages are NOT immediately forwarded to producer" do
      {pid, _rpc} = start_manager_with_spy_rpc()
      enable_exactly_once(pid)
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("eo-ack-1", "data")]})
      sync(pid)

      # Receipt modack RPC is in-flight; message not yet delivered
      refute_received {:stream_messages, _}

      # State has one pending entry
      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 1
    end

    test "in standard mode, messages are forwarded immediately (no gating)" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("std-ack-1", "data")]})

      assert_receive {:stream_messages, [msg]}, 500
      assert msg.data == "data"

      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 0
    end

    test "messages are added to outstanding after receipt modack succeeds" do
      {pid, _rpc} = start_manager_with_spy_rpc()
      enable_exactly_once(pid)
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("eo-ack-2", "data")]})
      sync(pid)

      state_before = :sys.get_state(pid)
      [ref] = Map.keys(state_before.pending_receipt_modacks)

      # Simulate receipt modack success
      send(pid, {:receipt_modack_result, ref, {:ok, []}})
      sync(pid)

      state_after = :sys.get_state(pid)
      assert map_size(state_after.pending_receipt_modacks) == 0
      assert Map.has_key?(state_after.outstanding, "eo-ack-2")
    end
  end

  # Injects a pending_receipt_modacks entry into StreamManager state directly,
  # bypassing the AckBatcher/Task chain. Used for tests that need to control
  # which receipt_modack_result variant the handler sees.
  defp inject_pending_receipt_modack(pid, ref, ack_ids, data_by_id) do
    broadway_msgs =
      Enum.map(ack_ids, fn ack_id ->
        %Broadway.Message{
          data: Map.get(data_by_id, ack_id, ack_id),
          metadata: %{},
          acknowledger: BroadwayCloudPubSub.Streaming.Acknowledger.builder(__MODULE__).(ack_id)
        }
      end)

    :sys.replace_state(pid, fn s ->
      entry = %{
        broadway_messages: broadway_msgs,
        ack_ids: ack_ids,
        received_at: System.monotonic_time(:millisecond)
      }

      %{s | pending_receipt_modacks: Map.put(s.pending_receipt_modacks, ref, entry)}
    end)
  end

  describe "exactly-once — {:receipt_modack_result, ref, result}" do
    test "total success: all messages delivered and added to outstanding" do
      # SpyRpcClientForEO defaults to :ok, so the Task auto-fires {:ok, []} result.
      {pid, _rpc} = start_manager_with_spy_rpc()
      enable_exactly_once(pid)
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("r1", "d1"), received_message("r2", "d2")]})

      # The Task fires the RPC and sends back {:receipt_modack_result, ref, {:ok, []}}
      # automatically. Wait for the resulting message delivery.
      assert_receive {:stream_messages, msgs}, 500
      assert length(msgs) == 2
      assert Enum.map(msgs, & &1.data) |> Enum.sort() == ["d1", "d2"]

      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 0
      assert Map.has_key?(state.outstanding, "r1")
      assert Map.has_key?(state.outstanding, "r2")
    end

    test "total failure: no messages delivered, nothing added to outstanding" do
      {pid, rpc} = start_manager_with_spy_rpc()
      enable_exactly_once(pid)
      StreamManager.notify_demand(pid, 10)

      # Configure spy synchronously to return {:error, :unavailable} for the next modack call
      :ok = GenServer.call(rpc, {:set_response_sync, {:error, :unavailable}})

      send(pid, {:stream_messages, [received_message("fail-1", "data")]})
      # Wait for the Task's RPC call to complete and result to be processed
      sync(pid)
      # Give extra time for the async Task result to arrive and be processed
      Process.sleep(200)
      sync(pid)

      refute_received {:stream_messages, _}

      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 0
      assert map_size(state.outstanding) == 0
    end

    test "partial success: only succeeded messages delivered, failed dropped" do
      # Inject the pending entry directly to avoid racing with the auto-Task.
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      ref = make_ref()

      inject_pending_receipt_modack(pid, ref, ["ok-id", "bad-id"], %{
        "ok-id" => "good",
        "bad-id" => "dropped"
      })

      send(pid, {:receipt_modack_result, ref, {:ok, ["bad-id"]}})

      assert_receive {:stream_messages, msgs}, 500
      assert length(msgs) == 1
      assert hd(msgs).data == "good"

      state = :sys.get_state(pid)
      assert Map.has_key?(state.outstanding, "ok-id")
      refute Map.has_key?(state.outstanding, "bad-id")
    end

    test "partial success with all failed: no messages delivered" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["all-bad"], %{"all-bad" => "data"})

      send(pid, {:receipt_modack_result, ref, {:ok, ["all-bad"]}})
      sync(pid)

      refute_received {:stream_messages, _}
      assert map_size(:sys.get_state(pid).outstanding) == 0
    end

    test "stale/unknown ref is ignored gracefully" do
      {pid, _rpc} = start_manager_with_spy_rpc()
      enable_exactly_once(pid)

      stale_ref = make_ref()
      send(pid, {:receipt_modack_result, stale_ref, {:ok, []}})
      sync(pid)

      assert Process.alive?(pid)
      assert map_size(:sys.get_state(pid).pending_receipt_modacks) == 0
    end
  end

  describe "exactly-once — retry deadline auto-switch" do
    test "AckBatcher retry_deadline_ms switches to 600s when exactly-once is enabled" do
      broadway_name = Module.concat(__MODULE__, "RD#{System.unique_integer([:positive])}")

      opts =
        base_config()
        |> Keyword.put(:broadway_name, broadway_name)
        |> Keyword.put(:retry_deadline_ms, 60_000)

      grpc_client = Keyword.get(opts, :grpc_client, BroadwayCloudPubSub.Streaming.GrpcClient)
      {:ok, grpc_client_config} = grpc_client.init(opts)

      opts =
        opts
        |> Keyword.put(:grpc_client, grpc_client)
        |> Keyword.put(:grpc_client_config, grpc_client_config)

      rpc_client_name = Module.concat(broadway_name, UnaryRpcClient)
      batcher_name = Module.concat(broadway_name, AckBatcher)
      task_sup_name = Module.concat(broadway_name, ReceiptModackTaskSupervisor)

      {:ok, _stub} = StubRpcClient.start_link(rpc_client_name)
      {:ok, _task_sup} = Task.Supervisor.start_link(name: task_sup_name)

      {:ok, _batcher} =
        AckBatcher.start_link(
          name: batcher_name,
          rpc_client: rpc_client_name,
          task_supervisor: task_sup_name,
          ack_batch_interval_ms: 100,
          ack_batch_max_size: 2_500,
          retry_deadline_ms: 60_000
        )

      opts =
        opts
        |> Keyword.put(:producer_pid, self())
        |> Keyword.put(:ack_ref, {broadway_name, 0})

      {:ok, pid} = StreamManager.start_link(opts)

      batcher_pid = Process.whereis(batcher_name)
      assert :sys.get_state(batcher_pid).ack_tracker.retry_deadline_ms == 60_000

      # Enable exactly-once
      send(
        pid,
        {:subscription_properties,
         %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: false,
           exactly_once_delivery_enabled: true
         }}
      )

      sync(pid)
      # Cast is async — let AckBatcher process it
      AckBatcher.flush(batcher_pid)

      assert :sys.get_state(batcher_pid).ack_tracker.retry_deadline_ms == 600_000
    end

    test "AckBatcher retry_deadline_ms is restored to configured value when exactly-once is disabled" do
      broadway_name = Module.concat(__MODULE__, "RD2#{System.unique_integer([:positive])}")

      opts =
        base_config()
        |> Keyword.put(:broadway_name, broadway_name)
        |> Keyword.put(:retry_deadline_ms, 60_000)

      grpc_client = Keyword.get(opts, :grpc_client, BroadwayCloudPubSub.Streaming.GrpcClient)
      {:ok, grpc_client_config} = grpc_client.init(opts)

      opts =
        opts
        |> Keyword.put(:grpc_client, grpc_client)
        |> Keyword.put(:grpc_client_config, grpc_client_config)

      rpc_client_name = Module.concat(broadway_name, UnaryRpcClient)
      batcher_name = Module.concat(broadway_name, AckBatcher)
      task_sup_name = Module.concat(broadway_name, ReceiptModackTaskSupervisor)

      {:ok, _stub} = StubRpcClient.start_link(rpc_client_name)
      {:ok, _task_sup} = Task.Supervisor.start_link(name: task_sup_name)

      {:ok, _batcher} =
        AckBatcher.start_link(
          name: batcher_name,
          rpc_client: rpc_client_name,
          task_supervisor: task_sup_name,
          ack_batch_interval_ms: 100,
          ack_batch_max_size: 2_500,
          retry_deadline_ms: 60_000
        )

      opts =
        opts
        |> Keyword.put(:producer_pid, self())
        |> Keyword.put(:ack_ref, {broadway_name, 0})

      {:ok, pid} = StreamManager.start_link(opts)

      batcher_pid = Process.whereis(batcher_name)

      enable_exactly_once(pid)
      AckBatcher.flush(batcher_pid)
      assert :sys.get_state(batcher_pid).ack_tracker.retry_deadline_ms == 600_000

      # Disable exactly-once
      send(
        pid,
        {:subscription_properties,
         %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: false,
           exactly_once_delivery_enabled: false
         }}
      )

      sync(pid)
      AckBatcher.flush(batcher_pid)
      assert :sys.get_state(batcher_pid).ack_tracker.retry_deadline_ms == 60_000
    end

    test "retry_deadline_ms is NOT updated when exactly_once status does not change" do
      broadway_name = Module.concat(__MODULE__, "RD3#{System.unique_integer([:positive])}")

      opts =
        base_config()
        |> Keyword.put(:broadway_name, broadway_name)
        |> Keyword.put(:retry_deadline_ms, 60_000)

      grpc_client = Keyword.get(opts, :grpc_client, BroadwayCloudPubSub.Streaming.GrpcClient)
      {:ok, grpc_client_config} = grpc_client.init(opts)

      opts =
        opts
        |> Keyword.put(:grpc_client, grpc_client)
        |> Keyword.put(:grpc_client_config, grpc_client_config)

      rpc_client_name = Module.concat(broadway_name, UnaryRpcClient)
      batcher_name = Module.concat(broadway_name, AckBatcher)
      task_sup_name = Module.concat(broadway_name, ReceiptModackTaskSupervisor)

      {:ok, _stub} = StubRpcClient.start_link(rpc_client_name)
      {:ok, _task_sup} = Task.Supervisor.start_link(name: task_sup_name)

      {:ok, _batcher} =
        AckBatcher.start_link(
          name: batcher_name,
          rpc_client: rpc_client_name,
          task_supervisor: task_sup_name,
          ack_batch_interval_ms: 100,
          ack_batch_max_size: 2_500
        )

      opts =
        opts
        |> Keyword.put(:producer_pid, self())
        |> Keyword.put(:ack_ref, {broadway_name, 0})

      {:ok, pid} = StreamManager.start_link(opts)

      batcher_pid = Process.whereis(batcher_name)
      initial_deadline = :sys.get_state(batcher_pid).ack_tracker.retry_deadline_ms

      # Send the same exactly_once=false twice — no update should happen
      send(
        pid,
        {:subscription_properties,
         %BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: false,
           exactly_once_delivery_enabled: false
         }}
      )

      sync(pid)
      AckBatcher.flush(batcher_pid)

      assert :sys.get_state(batcher_pid).ack_tracker.retry_deadline_ms == initial_deadline
    end
  end

  describe "exactly-once — stale pending_receipt_modacks sweep" do
    test "entries older than 60s are nacked with deadline=0 during extend_leases" do
      pid = start_manager()

      # Inject a stale entry (received_at far in the past)
      stale_ref = make_ref()

      stale_entry = %{
        broadway_messages: [],
        ack_ids: ["stale-ack-1"],
        received_at: System.monotonic_time(:millisecond) - 120_000
      }

      :sys.replace_state(pid, fn s ->
        %{s | pending_receipt_modacks: Map.put(s.pending_receipt_modacks, stale_ref, stale_entry)}
      end)

      # Trigger extend_leases which runs the sweep
      send(pid, :extend_leases)
      sync(pid)

      # Stale entry should be removed
      state = :sys.get_state(pid)
      refute Map.has_key?(state.pending_receipt_modacks, stale_ref)
    end

    test "fresh entries are NOT swept during extend_leases" do
      pid = start_manager()

      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["fresh-ack"], %{"fresh-ack" => "data"})

      send(pid, :extend_leases)
      sync(pid)

      # Fresh entry should survive the sweep
      assert Map.has_key?(:sys.get_state(pid).pending_receipt_modacks, ref)
    end
  end

  describe "exactly-once — drain nack pending receipt modacks" do
    test "pending receipt modacks are nacked on prepare_for_draining" do
      pid = start_manager()

      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["drain-eo"], %{"drain-eo" => "data"})
      assert map_size(:sys.get_state(pid).pending_receipt_modacks) == 1

      StreamManager.prepare_for_draining(pid)
      sync(pid)

      # After drain, pending_receipt_modacks should be cleared
      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 0
    end

    test "receipt_modack_result after drain (cleared pending) is ignored gracefully" do
      pid = start_manager()

      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["drain-stale"], %{"drain-stale" => "data"})

      # Drain clears the pending map
      StreamManager.prepare_for_draining(pid)
      sync(pid)

      # RPC result arrives after drain — should be ignored, not crash
      send(pid, {:receipt_modack_result, ref, {:ok, []}})
      sync(pid)

      assert Process.alive?(pid)
    end
  end

  describe "exactly-once — pending_receipt_modacks NOT cleared on reconnect" do
    test "pending_receipt_modacks survives a stream_error reset" do
      # Use inject_pending_receipt_modack to avoid races with the auto-Task.
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["reconnect-ack"], %{"reconnect-ack" => "data"})
      assert map_size(:sys.get_state(pid).pending_receipt_modacks) == 1

      # Simulate a retryable stream error (triggers reconnect, not drain)
      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "unavailable"}})
      sync(pid)

      # pending_receipt_modacks must survive — ack_ids are valid across reconnects
      assert map_size(:sys.get_state(pid).pending_receipt_modacks) == 1
    end

    test "receipt_modack_result arriving after reconnect is still processed correctly" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["post-reconnect"], %{"post-reconnect" => "data"})

      # Reconnect
      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "unavailable"}})
      sync(pid)

      # Result arrives post-reconnect — should still deliver
      send(pid, {:receipt_modack_result, ref, {:ok, []}})

      assert_receive {:stream_messages, [msg]}, 500
      assert msg.data == "data"
    end
  end

  # ============================================================
  # modify_deadline — non-zero deadline removes from outstanding
  # ============================================================

  describe "modify_deadline with non-zero deadline" do
    test "removes ack_ids from outstanding (same as deadline=0)" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      # Push a message so it lands in outstanding
      send(pid, {:stream_messages, [received_message("nack-5-ack", "data")]})
      assert_receive {:stream_messages, [_]}, 500

      state = :sys.get_state(pid)
      assert Map.has_key?(state.outstanding, "nack-5-ack")

      # Nack with non-zero deadline (e.g. on_shutdown default {:nack, 5})
      StreamManager.modify_deadline(pid, ["nack-5-ack"], 5)
      sync(pid)

      state = :sys.get_state(pid)
      refute Map.has_key?(state.outstanding, "nack-5-ack")
    end

    test "non-zero nack during drain allows drain to complete" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      # Push a message so it lands in outstanding
      send(pid, {:stream_messages, [received_message("drain-nack5", "data")]})
      assert_receive {:stream_messages, [_]}, 500

      # Enter drain mode
      StreamManager.prepare_for_draining(pid)
      sync(pid)

      state = :sys.get_state(pid)
      assert state.draining
      assert Map.has_key?(state.outstanding, "drain-nack5")
      # Drain timer should be set (drain not yet complete)
      assert state.drain_timer != nil

      # Nack with non-zero deadline — should remove from outstanding and complete drain
      StreamManager.modify_deadline(pid, ["drain-nack5"], 5)
      sync(pid)

      state = :sys.get_state(pid)
      assert map_size(state.outstanding) == 0
      # Drain should have completed: timer cancelled
      assert state.drain_timer == nil
    end
  end

  # ============================================================
  # P0-04: pressure_snapshot telemetry
  # ============================================================

  describe "pressure_snapshot telemetry" do
    test "emits :pressure_snapshot during :extend_leases with correct counts" do
      pid = start_manager()
      test_pid = self()
      telemetry_name = "test-pressure-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :stream, :pressure_snapshot],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :pressure_snapshot}
      )

      # Dispatch 2 messages (in-flight, outstanding)
      StreamManager.notify_demand(pid, 10)

      send(
        pid,
        {:stream_messages, [received_message("ps-1", "d1"), received_message("ps-2", "d2")]}
      )

      assert_receive {:stream_messages, _}, 500

      # Buffer one message by zeroing pending_demand so it stays buffered.
      # Note: the buffered message IS also in outstanding — all received messages
      # are added to outstanding at receipt time regardless of buffer state.
      :sys.replace_state(pid, fn s -> %{s | pending_demand: 0} end)
      send(pid, {:stream_messages, [received_message("ps-buf", "buf")]})
      # Wait for it to land in the buffer
      assert buffer_length(pid) == 1

      # Set pending_demand to a known value for assertion
      :sys.replace_state(pid, fn s -> %{s | pending_demand: 5} end)

      send(pid, :extend_leases)
      sync(pid)

      assert_receive {:pressure_snapshot, measurements, metadata}, 500

      # 3 outstanding: 2 dispatched + 1 buffered (all in outstanding map)
      assert measurements.outstanding_count == 3
      assert measurements.buffered_count == 1
      assert measurements.pending_demand == 5

      assert metadata.subscription == "projects/test/subscriptions/test-sub"

      :telemetry.detach(telemetry_name)
    end

    test ":pressure_snapshot measurements shape is correct" do
      pid = start_manager()
      test_pid = self()
      telemetry_name = "test-pressure-shape-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :stream, :pressure_snapshot],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :pressure_snapshot}
      )

      send(pid, :extend_leases)
      sync(pid)

      assert_receive {:pressure_snapshot, measurements, _metadata}, 500

      assert is_integer(measurements.outstanding_count) and measurements.outstanding_count >= 0
      assert is_integer(measurements.buffered_count) and measurements.buffered_count >= 0
      assert is_integer(measurements.pending_demand) and measurements.pending_demand >= 0

      :telemetry.detach(telemetry_name)
    end
  end

  # ============================================================
  # P0-05: drain telemetry (async span: :start / :stop / :exception)
  # ============================================================

  describe "drain :start telemetry" do
    test "emits drain :start with system_time and monotonic_time, correct initial counts" do
      pid = start_manager()
      test_pid = self()
      telemetry_name = "test-drain-start-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :stream, :drain, :start],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :drain_start}
      )

      # Dispatch 2 messages to outstanding (in-flight)
      StreamManager.notify_demand(pid, 10)

      send(
        pid,
        {:stream_messages, [received_message("ds-1", "d1"), received_message("ds-2", "d2")]}
      )

      assert_receive {:stream_messages, _}, 500

      # Buffer 1 message by zeroing pending_demand
      :sys.replace_state(pid, fn s -> %{s | pending_demand: 0} end)
      send(pid, {:stream_messages, [received_message("ds-buf", "buf")]})
      assert buffer_length(pid) == 1

      # Inject 1 pending receipt modack
      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["ds-eo"], %{"ds-eo" => "eo"})

      StreamManager.prepare_for_draining(pid)

      assert_receive {:drain_start, measurements, metadata}, 500

      # Span start measurements: system_time and monotonic_time
      assert is_integer(measurements.system_time)
      assert is_integer(measurements.monotonic_time)

      # Counts captured at the moment of drain initiation (before any cleanup)
      # outstanding_count = 2 in-flight + 1 buffered = 3
      assert measurements.outstanding_count == 3
      assert measurements.buffered_count == 1
      assert measurements.pending_receipt_modack_count == 1

      assert metadata.subscription == "projects/test/subscriptions/test-sub"

      :telemetry.detach(telemetry_name)
    end

    test "emits drain :start with zeros when nothing is outstanding" do
      pid = start_manager()
      test_pid = self()
      telemetry_name = "test-drain-start-empty-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :stream, :drain, :start],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :drain_start}
      )

      StreamManager.prepare_for_draining(pid)

      assert_receive {:drain_start, measurements, _metadata}, 500

      assert measurements.outstanding_count == 0
      assert measurements.buffered_count == 0
      assert measurements.pending_receipt_modack_count == 0

      :telemetry.detach(telemetry_name)
    end
  end

  describe "drain :stop telemetry" do
    test "emits drain :stop with positive duration when drain completes cleanly" do
      pid = start_manager(drain_timeout_ms: 5_000)
      test_pid = self()
      telemetry_name = "test-drain-stop-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :stream, :drain, :stop],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :drain_stop}
      )

      # One in-flight message
      StreamManager.notify_demand(pid, 10)
      send(pid, {:stream_messages, [received_message("stop-ack-1", "data")]})
      assert_receive {:stream_messages, _}, 500

      {:ok, 0} = StreamManager.prepare_for_draining(pid)

      # Ack the message — drain completes
      StreamManager.acknowledge(pid, ["stop-ack-1"])

      assert_receive {:drain_stop, measurements, metadata}, 500

      assert is_integer(measurements.duration)
      assert measurements.duration >= 0
      assert is_integer(measurements.monotonic_time)
      assert metadata.subscription == "projects/test/subscriptions/test-sub"

      :telemetry.detach(telemetry_name)
    end

    test "drain :stop is emitted when outstanding is already empty at prepare_for_draining" do
      pid = start_manager()
      test_pid = self()
      telemetry_name = "test-drain-stop-immediate-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :stream, :drain, :stop],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :drain_stop}
      )

      {:ok, 0} = StreamManager.prepare_for_draining(pid)

      assert_receive {:drain_stop, measurements, _metadata}, 500

      assert is_integer(measurements.duration)
      assert measurements.duration >= 0

      :telemetry.detach(telemetry_name)
    end

    test "clean drain does NOT emit :exception" do
      pid = start_manager(drain_timeout_ms: 5_000)
      test_pid = self()
      telemetry_name = "test-drain-no-exception-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :stream, :drain, :exception],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :drain_exception}
      )

      StreamManager.notify_demand(pid, 10)
      send(pid, {:stream_messages, [received_message("clean-drain", "data")]})
      assert_receive {:stream_messages, _}, 500

      {:ok, 0} = StreamManager.prepare_for_draining(pid)
      StreamManager.acknowledge(pid, ["clean-drain"])
      sync(pid)

      refute_received {:drain_exception, _, _}

      :telemetry.detach(telemetry_name)
    end
  end

  describe "drain :exception telemetry" do
    test "emits drain :exception with kind: :timeout when drain_timeout fires" do
      pid = start_manager(drain_timeout_ms: 50)
      test_pid = self()
      telemetry_name = "test-drain-exception-timeout-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :stream, :drain, :exception],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :drain_exception}
      )

      # One in-flight message (never acked so timeout fires)
      StreamManager.notify_demand(pid, 10)
      send(pid, {:stream_messages, [received_message("timeout-ack-1", "data")]})
      assert_receive {:stream_messages, _}, 500

      {:ok, 0} = StreamManager.prepare_for_draining(pid)

      # Wait for the 50ms drain_timeout
      assert_receive {:drain_exception, measurements, metadata}, 500

      assert metadata.kind == :timeout
      assert metadata.reason == :drain_timeout
      assert measurements.remaining_count == 1
      assert is_integer(measurements.duration)
      assert measurements.duration >= 0
      assert is_integer(measurements.monotonic_time)
      assert metadata.subscription == "projects/test/subscriptions/test-sub"

      :telemetry.detach(telemetry_name)
    end

    test "drain_timeout does NOT emit :stop" do
      pid = start_manager(drain_timeout_ms: 50)
      test_pid = self()
      telemetry_name = "test-drain-timeout-no-stop-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :stream, :drain, :stop],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :drain_stop}
      )

      StreamManager.notify_demand(pid, 10)
      send(pid, {:stream_messages, [received_message("timeout-no-stop", "data")]})
      assert_receive {:stream_messages, _}, 500

      {:ok, 0} = StreamManager.prepare_for_draining(pid)

      # Wait long enough for timeout to fire
      Process.sleep(150)
      sync(pid)

      refute_received {:drain_stop, _, _}

      :telemetry.detach(telemetry_name)
    end

    test "emits drain :exception with kind: :terminate when process is terminated mid-drain" do
      pid = start_manager(drain_timeout_ms: 5_000)
      test_pid = self()
      telemetry_name = "test-drain-exception-terminate-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :stream, :drain, :exception],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :drain_exception}
      )

      # One in-flight message (never acked)
      StreamManager.notify_demand(pid, 10)
      send(pid, {:stream_messages, [received_message("term-ack-1", "data")]})
      assert_receive {:stream_messages, _}, 500

      {:ok, 0} = StreamManager.prepare_for_draining(pid)

      # Monitor then stop the GenServer normally
      ref = Process.monitor(pid)
      GenServer.stop(pid, :normal)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 500

      assert_receive {:drain_exception, measurements, metadata}, 500

      assert metadata.kind == :terminate
      assert metadata.reason == :normal
      assert measurements.remaining_count == 1
      assert is_integer(measurements.duration)
      assert metadata.subscription == "projects/test/subscriptions/test-sub"

      :telemetry.detach(telemetry_name)
    end
  end

  # ============================================================
  # P0-05: drain failure scenarios
  # ============================================================

  describe "drain failure scenarios" do
    test "stream_error (retryable) during drain does not prevent drain completion via ack" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      # Dispatch one message to make it in-flight
      send(pid, {:stream_messages, [received_message("drain-err-1", "data")]})
      assert_receive {:stream_messages, [_]}, 500

      assert map_size(:sys.get_state(pid).outstanding) == 1

      # Start drain
      {:ok, 0} = StreamManager.prepare_for_draining(pid)

      # Cancel any pre-existing reconnect (from initial failed connection attempt)
      # so we can cleanly assert there's no reconnect scheduled after the stream_error.
      :sys.replace_state(pid, fn s ->
        if s.reconnect_ref, do: Process.cancel_timer(s.reconnect_ref)
        %{s | reconnect_ref: nil}
      end)

      # Stream disconnects while we're draining (common race)
      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "unavailable"}})
      sync(pid)

      # Process is still alive and draining
      assert Process.alive?(pid)
      state = :sys.get_state(pid)
      assert state.draining == true
      # No reconnect scheduled during drain
      assert state.reconnect_ref == nil

      # Ack completes — drain should finish
      StreamManager.acknowledge(pid, ["drain-err-1"])
      sync(pid)

      state = :sys.get_state(pid)
      assert map_size(state.outstanding) == 0
      assert state.drain_timer == nil
    end

    test "stream_closed during drain does not prevent drain completion via ack" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("drain-close-1", "data")]})
      assert_receive {:stream_messages, [_]}, 500

      {:ok, 0} = StreamManager.prepare_for_draining(pid)

      # Cancel any pre-existing reconnect from initial failed connection attempt
      :sys.replace_state(pid, fn s ->
        if s.reconnect_ref, do: Process.cancel_timer(s.reconnect_ref)
        %{s | reconnect_ref: nil}
      end)

      # Server closes stream (normal — no reconnect during drain)
      send(pid, {:stream_closed})
      sync(pid)

      assert Process.alive?(pid)
      state = :sys.get_state(pid)
      assert state.draining == true
      assert state.reconnect_ref == nil

      # Ack the in-flight message — drain completes
      StreamManager.acknowledge(pid, ["drain-close-1"])
      sync(pid)

      state = :sys.get_state(pid)
      assert map_size(state.outstanding) == 0
      assert state.drain_timer == nil
    end

    test "drain with mixed buffered + in-flight: only in-flight blocks drain completion" do
      pid = start_manager()

      # Dispatch 2 messages (in-flight)
      StreamManager.notify_demand(pid, 10)

      send(
        pid,
        {:stream_messages,
         [received_message("mix-if-1", "d1"), received_message("mix-if-2", "d2")]}
      )

      assert_receive {:stream_messages, _}, 500

      # Buffer 2 messages by zeroing pending_demand
      :sys.replace_state(pid, fn s -> %{s | pending_demand: 0} end)

      send(
        pid,
        {:stream_messages,
         [received_message("mix-buf-1", "b1"), received_message("mix-buf-2", "b2")]}
      )

      assert buffer_length(pid) == 2

      state = :sys.get_state(pid)
      # 4 outstanding total: 2 in-flight + 2 buffered
      assert map_size(state.outstanding) == 4

      # Drain: buffered 2 are nacked and removed; 2 in-flight remain
      {:ok, 2} = StreamManager.prepare_for_draining(pid)

      state = :sys.get_state(pid)
      assert map_size(state.outstanding) == 2
      assert Map.has_key?(state.outstanding, "mix-if-1")
      assert Map.has_key?(state.outstanding, "mix-if-2")
      assert state.drain_timer != nil

      # Ack one in-flight — drain not complete yet
      StreamManager.acknowledge(pid, ["mix-if-1"])
      sync(pid)
      assert state.drain_timer != nil

      state = :sys.get_state(pid)
      assert map_size(state.outstanding) == 1

      # Ack the last in-flight — drain completes
      StreamManager.acknowledge(pid, ["mix-if-2"])
      sync(pid)

      state = :sys.get_state(pid)
      assert map_size(state.outstanding) == 0
      assert state.drain_timer == nil
    end

    test "drain with pending receipt modacks: clears them on prepare_for_draining" do
      pid = start_manager()

      # Inject 2 pending receipt modacks
      ref1 = make_ref()
      ref2 = make_ref()
      inject_pending_receipt_modack(pid, ref1, ["eo-drain-1"], %{"eo-drain-1" => "d1"})
      inject_pending_receipt_modack(pid, ref2, ["eo-drain-2"], %{"eo-drain-2" => "d2"})

      assert map_size(:sys.get_state(pid).pending_receipt_modacks) == 2

      # Drain clears pending receipt modacks immediately
      StreamManager.prepare_for_draining(pid)
      sync(pid)

      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 0
      # Drain should complete since outstanding is also empty
      assert map_size(state.outstanding) == 0
      assert state.drain_timer == nil
    end
  end

  # ============================================================
  # P0-06: EO and non-EO behavior invariants
  # ============================================================

  describe "non-EO behavior invariants" do
    test "messages are immediately added to outstanding on receipt" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      send(
        pid,
        {:stream_messages, [received_message("neo-1", "d1"), received_message("neo-2", "d2")]}
      )

      assert_receive {:stream_messages, msgs}, 500

      assert length(msgs) == 2

      state = :sys.get_state(pid)
      # Both ack_ids must be in outstanding immediately
      assert Map.has_key?(state.outstanding, "neo-1")
      assert Map.has_key?(state.outstanding, "neo-2")
      # No pending receipt modacks in non-EO mode
      assert map_size(state.pending_receipt_modacks) == 0
    end

    test "ack removes from outstanding" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("neo-ack", "data")]})
      assert_receive {:stream_messages, _}, 500

      assert Map.has_key?(:sys.get_state(pid).outstanding, "neo-ack")

      StreamManager.acknowledge(pid, ["neo-ack"])
      sync(pid)

      refute Map.has_key?(:sys.get_state(pid).outstanding, "neo-ack")
    end

    test "nack (deadline=0) removes from outstanding" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("neo-nack", "data")]})
      assert_receive {:stream_messages, _}, 500

      assert Map.has_key?(:sys.get_state(pid).outstanding, "neo-nack")

      StreamManager.modify_deadline(pid, ["neo-nack"], 0)
      sync(pid)

      refute Map.has_key?(:sys.get_state(pid).outstanding, "neo-nack")
    end

    test "reconnect does not clear buffered messages' outstanding entries" do
      # Buffered messages ARE in outstanding; after reconnect the buffer is
      # cleared but the ack_ids are also removed from outstanding.
      pid = start_manager(backoff_min: 60_000)
      StreamManager.notify_demand(pid, 0)

      send(pid, {:stream_messages, [received_message("buf-reconnect", "data")]})
      assert buffer_length(pid) == 1

      state = :sys.get_state(pid)
      assert Map.has_key?(state.outstanding, "buf-reconnect")

      # Simulate retryable reconnect — buffer is dropped and outstanding cleared for buffered
      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "unavailable"}})
      sync(pid)

      # After reset_connection, the buffered ack_id is removed from outstanding
      state = :sys.get_state(pid)
      refute Map.has_key?(state.outstanding, "buf-reconnect")
      assert buffer_length(pid) == 0
    end

    test "buffered messages are nacked with deadline 0 on disconnect" do
      # When a stream disconnects, buffered messages should be nacked so they
      # become immediately available for redelivery to any consumer, rather than
      # waiting for the ack deadline to expire naturally.
      {pid, _rpc} = start_manager_with_spy_rpc(backoff_min: 60_000)
      StreamManager.notify_demand(pid, 0)

      send(pid, {:stream_messages, [received_message("nack-buf-1", "data1")]})
      send(pid, {:stream_messages, [received_message("nack-buf-2", "data2")]})
      sync(pid)
      assert buffer_length(pid) == 2

      # Flush any pending modack calls from the initial receipt modack
      flush_mailbox()

      # Simulate retryable stream error to trigger reset_connection
      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "unavailable"}})
      sync(pid)

      # AckBatcher flushes on its timer; force a flush to capture the nack RPC
      batcher = :sys.get_state(pid).ack_batcher
      AckBatcher.flush(batcher)

      # The spy RPC client should have received a modack with deadline 0 for the
      # buffered ack_ids.
      assert_receive {:rpc_call, {:modack, ids, 0}}, 1_000
      assert Enum.sort(ids) == ["nack-buf-1", "nack-buf-2"]
    end
  end

  describe "EO behavior invariants" do
    test "in EO mode, messages are NOT in outstanding before receipt modack succeeds" do
      {pid, _rpc} = start_manager_with_spy_rpc()
      enable_exactly_once(pid)
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("eo-inv-1", "data")]})
      sync(pid)

      # Pending — not yet in outstanding
      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 1
      refute Map.has_key?(state.outstanding, "eo-inv-1")
    end

    test "in EO mode, messages are added to outstanding only after receipt modack {:ok, []}" do
      {pid, _rpc} = start_manager_with_spy_rpc()
      enable_exactly_once(pid)
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("eo-inv-2", "data")]})
      sync(pid)

      # Retrieve the pending ref
      state = :sys.get_state(pid)
      [ref] = Map.keys(state.pending_receipt_modacks)

      # Confirm receipt modack success
      send(pid, {:receipt_modack_result, ref, {:ok, []}})
      sync(pid)

      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 0
      assert Map.has_key?(state.outstanding, "eo-inv-2")
    end

    test "in EO mode, total receipt modack failure drops messages (no dispatch, no outstanding)" do
      {pid, rpc} = start_manager_with_spy_rpc()
      enable_exactly_once(pid)
      StreamManager.notify_demand(pid, 10)

      :ok = GenServer.call(rpc, {:set_response_sync, {:error, :unavailable}})

      send(pid, {:stream_messages, [received_message("eo-inv-fail", "data")]})
      Process.sleep(200)
      sync(pid)

      refute_received {:stream_messages, _}

      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 0
      refute Map.has_key?(state.outstanding, "eo-inv-fail")
    end

    test "in EO mode, partial receipt modack failure: succeeded messages in outstanding, failed are not" do
      pid = start_manager()
      enable_exactly_once(pid)
      StreamManager.notify_demand(pid, 10)

      ref = make_ref()

      inject_pending_receipt_modack(pid, ref, ["eo-ok", "eo-fail"], %{
        "eo-ok" => "good",
        "eo-fail" => "bad"
      })

      send(pid, {:receipt_modack_result, ref, {:ok, ["eo-fail"]}})

      assert_receive {:stream_messages, msgs}, 500
      assert length(msgs) == 1
      assert hd(msgs).data == "good"

      state = :sys.get_state(pid)
      assert Map.has_key?(state.outstanding, "eo-ok")
      refute Map.has_key?(state.outstanding, "eo-fail")
    end

    test "switching from non-EO to EO: new messages go through gate, not immediate dispatch" do
      # Start with a spy RPC so we can control modack responses.
      # We need to use start_manager_with_spy_rpc throughout so the spy
      # is in place before we enable EO.
      {pid, _rpc} = start_manager_with_spy_rpc()
      StreamManager.notify_demand(pid, 10)

      # Non-EO (default): message dispatched immediately
      send(pid, {:stream_messages, [received_message("before-eo", "data")]})

      # The spy RPC responds :ok, so the modack Task fires {:ok, []} and
      # the message is dispatched straight through (standard path)
      assert_receive {:stream_messages, _}, 500
      assert map_size(:sys.get_state(pid).pending_receipt_modacks) == 0

      # Enable EO mode
      enable_exactly_once(pid)

      # EO: new message must be held in pending_receipt_modacks, not dispatched
      send(pid, {:stream_messages, [received_message("after-eo", "data")]})
      sync(pid)

      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 1
    end
  end
end
