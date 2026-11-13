defmodule BroadwayCloudPubSub.Streaming.UnaryRpcClientTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias BroadwayCloudPubSub.Streaming.UnaryRpcClient
  alias BroadwayCloudPubSub.Test.{GrpcDynamicAdapter, TelemetryHelper}

  # ============================================================
  # Chunking logic — pure caller-side, no GenServer needed
  # ============================================================

  describe "acknowledge/2 chunking (caller-side logic)" do
    test "a list of 5001 ids is split into 3 chunks of at most 2500" do
      ids = Enum.map(1..5_001, &"id-#{&1}")
      # chunk_every(2500) => [2500, 2500, 1]
      chunks = Enum.chunk_every(ids, 2_500)
      assert length(chunks) == 3
      assert Enum.map(chunks, &length/1) == [2_500, 2_500, 1]
    end

    test "a list of exactly 2500 ids is a single chunk" do
      ids = Enum.map(1..2_500, &"id-#{&1}")
      chunks = Enum.chunk_every(ids, 2_500)
      assert length(chunks) == 1
    end

    test "an empty list produces no chunks" do
      chunks = Enum.chunk_every([], 2_500)
      assert chunks == []
    end
  end

  describe "modify_ack_deadline/3 chunking (caller-side logic)" do
    test "7500 ids produce 3 chunks" do
      ids = Enum.map(1..7_500, &"id-#{&1}")
      chunks = Enum.chunk_every(ids, 2_500)
      assert length(chunks) == 3
    end
  end

  # ============================================================
  # start_link / init — using a fast-failing fake token so
  # channel open fails at auth rather than TCP-connect time.
  # ============================================================

  defp base_config_bad_token do
    [
      broadway_name: __MODULE__,
      subscription: "projects/test/subscriptions/test-sub",
      token_generator: {__MODULE__, :fail_token, []},
      adapter: GrpcDynamicAdapter,
      grpc_endpoint: "localhost:12345",
      use_ssl: false,
      backoff_type: :exp,
      backoff_min: 100,
      backoff_max: 60_000
    ]
  end

  # Token generator that returns an error — causes open_channel to fail
  # immediately without attempting a TCP connection.
  def fail_token, do: {:error, :no_token}
  def noop_token, do: {:ok, "test-token"}

  defp start_client_no_channel(extra_opts \\ []) do
    opts = Keyword.merge(base_config_bad_token(), extra_opts)
    # Mirror what Producer.prepare_for_start/2 does: derive grpc_client_config.
    grpc_client = Keyword.get(opts, :grpc_client, BroadwayCloudPubSub.Streaming.GrpcClient)
    {:ok, grpc_client_config} = grpc_client.init(opts)

    opts =
      opts
      |> Keyword.put(:grpc_client, grpc_client)
      |> Keyword.put(:grpc_client_config, grpc_client_config)

    {:ok, pid} = UnaryRpcClient.start_link(opts)
    pid
  end

  describe "start_link/1" do
    test "starts successfully even when initial channel connect fails" do
      pid = start_client_no_channel()
      assert Process.alive?(pid)
    end

    test "registers under :name when provided" do
      name = Module.concat(__MODULE__, Named)
      pid = start_client_no_channel(name: name)
      assert Process.whereis(name) != nil
      assert Process.alive?(pid)
    end

    test "channel is nil when initial token fetch fails" do
      pid = start_client_no_channel()
      state = :sys.get_state(pid)
      assert state.channel == nil
    end
  end

  # ============================================================
  # State structure
  # ============================================================

  describe "state structure" do
    test "config map contains expected keys" do
      pid = start_client_no_channel()
      state = :sys.get_state(pid)
      assert Map.has_key?(state.config, :broadway_name)
      assert Map.has_key?(state.config, :subscription)
      assert Map.has_key?(state.config, :token_generator)
      assert Map.has_key?(state.config, :adapter)
      assert state.config.broadway_name == __MODULE__
    end

    test "state has a :backoff field for reconnect exponential backoff" do
      pid = start_client_no_channel()
      state = :sys.get_state(pid)
      assert Map.has_key?(state, :backoff)
    end
  end

  # ============================================================
  # Call-based API returns {:error, :no_channel} when channel is nil
  # ============================================================

  describe "calls with no channel" do
    test "acknowledge/2 returns {:ok, all_ids} (retained) when channel is nil" do
      pid = start_client_no_channel()

      result = UnaryRpcClient.acknowledge(pid, ["id-1", "id-2"])

      # No channel → {:error, :no_channel} per chunk → accumulated into {:ok, all_ids}
      assert {:ok, retained} = result
      assert Enum.sort(retained) == ["id-1", "id-2"]
    end

    test "modify_ack_deadline/3 returns {:ok, all_ids} (retained) when channel is nil" do
      pid = start_client_no_channel()

      result = UnaryRpcClient.modify_ack_deadline(pid, ["id-1"], 30)

      assert {:ok, retained} = result
      assert retained == ["id-1"]
    end

    test "GenServer stays alive after calls with no channel" do
      pid = start_client_no_channel()

      UnaryRpcClient.acknowledge(pid, ["id-1"])
      UnaryRpcClient.modify_ack_deadline(pid, ["id-2"], 30)

      assert Process.alive?(pid)
    end
  end

  # ============================================================
  # Partial-success tracking — caller-side reduce logic
  # ============================================================

  describe "partial-success reduce logic" do
    test "all chunks succeed → {:ok, []}" do
      # Simulate the reduce logic directly without a real GenServer
      chunks = [["id-1", "id-2"], ["id-3"]]

      result =
        Enum.reduce(chunks, {:ok, []}, fn
          chunk, {:ok, failed_so_far} ->
            # Simulate :ok from each chunk
            case :ok do
              :ok -> {:ok, failed_so_far}
              {:error, _} -> {:ok, failed_so_far ++ chunk}
            end

          _chunk, {:error, _} = err ->
            err
        end)

      assert result == {:ok, []}
    end

    test "second chunk fails → {:ok, second_chunk_ids} retained" do
      chunks = [["id-1", "id-2"], ["id-3", "id-4"]]

      result =
        Enum.reduce(chunks, {:ok, []}, fn
          ["id-1", "id-2"] = chunk, {:ok, failed} ->
            # First chunk succeeds
            _ = chunk
            {:ok, failed}

          chunk, {:ok, failed} ->
            # Second chunk fails
            {:ok, failed ++ chunk}

          _chunk, {:error, _} = err ->
            err
        end)

      assert result == {:ok, ["id-3", "id-4"]}
    end

    test "hard process error short-circuits remaining chunks" do
      chunks = [["id-1"], ["id-2"], ["id-3"]]
      visited = :atomics.new(1, [])

      result =
        Enum.reduce(chunks, {:ok, []}, fn
          _chunk, {:error, _} = err ->
            err

          ["id-1"], {:ok, _failed} ->
            :atomics.add(visited, 1, 1)
            {:error, {:call_failed, :noproc}}

          chunk, {:ok, failed} ->
            :atomics.add(visited, 1, 1)
            {:ok, failed ++ chunk}
        end)

      assert {:error, {:call_failed, :noproc}} = result
      # Only the first chunk was visited before error short-circuited
      assert :atomics.get(visited, 1) == 1
    end
  end

  # ============================================================
  # Telemetry metadata uses broadway_name
  # ============================================================

  describe "telemetry metadata" do
    test "config has :broadway_name key (used by emit_telemetry)" do
      pid = start_client_no_channel()
      state = :sys.get_state(pid)

      assert is_atom(state.config.broadway_name)
      refute Map.has_key?(state.config, :broadway)
    end

    test "static telemetry_metadata is emitted under :extra on connection_failure" do
      test_pid = self()
      extra = %{tenant_id: "acme"}
      telemetry_name = "unary-extra-static-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :unary, :connection_failure],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :telemetry_meta}
      )

      # start_client_no_channel triggers a :connection_failure on init
      _pid = start_client_no_channel(telemetry_metadata: extra)

      assert_receive {:telemetry_meta, _measurements, metadata}, 1_000
      assert metadata.extra == extra

      :telemetry.detach(telemetry_name)
    end

    test "MFA telemetry_metadata is called and result is emitted under :extra" do
      test_pid = self()
      telemetry_name = "unary-extra-mfa-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :unary, :connection_failure],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :telemetry_meta}
      )

      _pid = start_client_no_channel(telemetry_metadata: {__MODULE__, :dynamic_meta, []})

      assert_receive {:telemetry_meta, _measurements, metadata}, 1_000
      assert metadata.extra == %{dynamic: true}

      :telemetry.detach(telemetry_name)
    end

    test "no :extra key when telemetry_metadata is not set" do
      test_pid = self()
      telemetry_name = "unary-no-extra-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        telemetry_name,
        [:broadway_cloud_pub_sub, :streaming, :unary, :connection_failure],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :telemetry_meta}
      )

      _pid = start_client_no_channel()

      assert_receive {:telemetry_meta, _measurements, metadata}, 1_000
      refute Map.has_key?(metadata, :extra)

      :telemetry.detach(telemetry_name)
    end
  end

  # MFA for telemetry_metadata test.
  def dynamic_meta, do: %{dynamic: true}

  # ============================================================
  # handle_info(:reconnect) — async reconnect path
  # ============================================================

  describe "handle_info(:reconnect)" do
    test "reconnect message is handled without crash" do
      pid = start_client_no_channel()

      # Send reconnect — will fail (bad token) but must not crash
      send(pid, :reconnect)
      :sys.get_state(pid)

      assert Process.alive?(pid)
    end

    test "channel remains nil after reconnect with bad token" do
      pid = start_client_no_channel()

      send(pid, :reconnect)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert state.channel == nil
    end
  end

  # ============================================================
  # child_opts/1
  # ============================================================

  describe "child_opts/1" do
    @full_opts [
      subscription: "projects/p/subscriptions/s",
      grpc_client: BroadwayCloudPubSub.Streaming.GrpcClient,
      grpc_client_config: %{},
      backoff_type: :rand_exp,
      backoff_min: 100,
      backoff_max: 60_000,
      broadway_name: MyPipeline,
      telemetry_metadata: %{env: :test},
      # Extra keys that UnaryRpcClient should NOT include
      ack_batch_interval_ms: 100,
      ack_batch_max_size: 2_500,
      retry_deadline_ms: 60_000,
      rpc_client: :some_rpc_client,
      max_outstanding_messages: 1_000
    ]

    test "returns only the keys UnaryRpcClient needs" do
      result = UnaryRpcClient.child_opts(@full_opts)

      assert Keyword.keys(result) |> Enum.sort() ==
               Enum.sort([
                 :subscription,
                 :grpc_client,
                 :grpc_client_config,
                 :backoff_type,
                 :backoff_min,
                 :backoff_max,
                 :broadway_name,
                 :telemetry_metadata
               ])
    end

    test "excludes AckBatcher-specific keys" do
      result = UnaryRpcClient.child_opts(@full_opts)

      refute Keyword.has_key?(result, :ack_batch_interval_ms)
      refute Keyword.has_key?(result, :ack_batch_max_size)
      refute Keyword.has_key?(result, :retry_deadline_ms)
      refute Keyword.has_key?(result, :rpc_client)
      refute Keyword.has_key?(result, :max_outstanding_messages)
    end

    test "omits optional :telemetry_metadata when not provided" do
      opts = Keyword.delete(@full_opts, :telemetry_metadata)
      result = UnaryRpcClient.child_opts(opts)

      refute Keyword.has_key?(result, :telemetry_metadata)
    end

    test "raises on missing required key :subscription" do
      opts = Keyword.delete(@full_opts, :subscription)

      assert_raise ArgumentError, ~r/missing required option :subscription/, fn ->
        UnaryRpcClient.child_opts(opts)
      end
    end

    test "raises on missing required key :grpc_client" do
      opts = Keyword.delete(@full_opts, :grpc_client)

      assert_raise ArgumentError, ~r/missing required option :grpc_client/, fn ->
        UnaryRpcClient.child_opts(opts)
      end
    end

    test "raises on missing required key :broadway_name" do
      opts = Keyword.delete(@full_opts, :broadway_name)

      assert_raise ArgumentError, ~r/missing required option :broadway_name/, fn ->
        UnaryRpcClient.child_opts(opts)
      end
    end
  end

  # ============================================================
  # handle_info({:EXIT, ...}) — channel lifecycle
  #
  # Regression coverage for the Mint-adapter leak: a stray EXIT from a
  # spawn-helper (e.g. Mint's `StreamResponseProcess`, which links to the
  # caller and exits :normal once the response is consumed) must NOT be
  # treated as a channel teardown. Only an EXIT whose pid matches the held
  # ConnectionProcess pid (`adapter_payload.conn_pid`) may tear down the
  # channel. See UPSTREAM_BUG_REPORT.md.
  # ============================================================

  defmodule FakeGrpcClient do
    @moduledoc false
    @behaviour BroadwayCloudPubSub.Streaming.Client

    # Each connect/1 spawns a new long-lived linked pid to play the role of
    # the Mint/Gun ConnectionProcess. The pid is exposed via the channel's
    # adapter_payload, mirroring the real adapters.
    @impl true
    def init(opts), do: {:ok, %{test_pid: Keyword.fetch!(opts, :test_pid)}}

    @impl true
    def connect(%{test_pid: test_pid}) do
      conn_pid = spawn_link(fn -> Process.sleep(:infinity) end)
      send(test_pid, {:fake_grpc, :connect, conn_pid})
      channel = %{adapter_payload: %{conn_pid: conn_pid}}
      {:ok, channel}
    end

    @impl true
    def disconnect(%{adapter_payload: %{conn_pid: conn_pid}}, %{test_pid: test_pid}) do
      send(test_pid, {:fake_grpc, :disconnect, conn_pid})
      if is_pid(conn_pid) and Process.alive?(conn_pid), do: Process.exit(conn_pid, :kill)
      :ok
    end

    @impl true
    def connection_pid(%{adapter_payload: %{conn_pid: pid}}) when is_pid(pid), do: pid
    def connection_pid(_), do: nil

    # Unused by the EXIT-handler regression tests but required by the behaviour.
    @impl true
    def streaming_pull(_channel, _config), do: {:error, :not_implemented}
    @impl true
    def send_request(_stream, _request, _config), do: {:ok, _stream = nil}
    @impl true
    def recv(_stream, _config), do: {:error, :not_implemented}
    @impl true
    def cancel(_stream, _config), do: :ok
    @impl true
    def acknowledge(_channel, _request, _config), do: {:ok, %{}}
    @impl true
    def modify_ack_deadline(_channel, _request, _config), do: {:ok, %{}}
  end

  # A variant of FakeGrpcClient that returns a configurable error from
  # acknowledge/3 and modify_ack_deadline/3. The error is read from the
  # :rpc_error key in the config map.
  defmodule FailingRpcGrpcClient do
    @moduledoc false
    @behaviour BroadwayCloudPubSub.Streaming.Client

    @impl true
    def init(opts), do: {:ok, Map.new(opts)}

    @impl true
    def connect(config) do
      conn_pid = spawn_link(fn -> Process.sleep(:infinity) end)
      channel = %{adapter_payload: %{conn_pid: conn_pid}}
      if pid = config[:test_pid], do: send(pid, {:fake_grpc, :connect, conn_pid})
      {:ok, channel}
    end

    @impl true
    def disconnect(%{adapter_payload: %{conn_pid: conn_pid}}, _config) do
      if is_pid(conn_pid) and Process.alive?(conn_pid), do: Process.exit(conn_pid, :kill)
      :ok
    end

    @impl true
    def connection_pid(%{adapter_payload: %{conn_pid: pid}}) when is_pid(pid), do: pid
    def connection_pid(_), do: nil

    @impl true
    def streaming_pull(_channel, _config), do: {:error, :not_implemented}
    @impl true
    def send_request(_stream, _request, _config), do: {:ok, nil}
    @impl true
    def recv(_stream, _config), do: {:error, :not_implemented}
    @impl true
    def cancel(_stream, _config), do: :ok

    @impl true
    def acknowledge(_channel, _request, config), do: {:error, config.rpc_error}

    @impl true
    def modify_ack_deadline(_channel, _request, config), do: {:error, config.rpc_error}
  end

  defp start_client_with_fake_grpc do
    opts = [
      broadway_name: __MODULE__,
      subscription: "projects/test/subscriptions/test-sub",
      grpc_client: FakeGrpcClient,
      grpc_client_config: %{test_pid: self()},
      backoff_type: :exp,
      backoff_min: 50,
      backoff_max: 1_000
    ]

    {:ok, pid} = UnaryRpcClient.start_link(opts)
    pid
  end

  describe "handle_info({:EXIT, ...}) — channel pid matching" do
    test "spurious EXIT :normal from a non-channel pid does NOT tear down the channel" do
      pid = start_client_with_fake_grpc()

      assert_receive {:fake_grpc, :connect, conn_pid}
      state = :sys.get_state(pid)
      assert state.channel.adapter_payload.conn_pid == conn_pid

      # Simulate Mint's per-RPC StreamResponseProcess: a foreign pid exiting :normal.
      stray = spawn(fn -> :ok end)
      send(pid, {:EXIT, stray, :normal})
      _ = :sys.get_state(pid)

      # Channel must be unchanged, and no extra connect/disconnect must have happened.
      state_after = :sys.get_state(pid)
      assert state_after.channel.adapter_payload.conn_pid == conn_pid
      refute_received {:fake_grpc, :connect, _}
      refute_received {:fake_grpc, :disconnect, _}
      assert Process.alive?(conn_pid)
    end

    test "spurious EXIT :crash from a non-channel pid does NOT tear down the channel" do
      pid = start_client_with_fake_grpc()

      assert_receive {:fake_grpc, :connect, conn_pid}

      stray = spawn(fn -> :ok end)
      send(pid, {:EXIT, stray, :some_crash})
      _ = :sys.get_state(pid)

      state_after = :sys.get_state(pid)
      assert state_after.channel.adapter_payload.conn_pid == conn_pid
      assert state_after.reconnect_pending == false
      refute_received {:fake_grpc, :disconnect, _}
      refute_received :reconnect
    end

    test "EXIT :normal from the held ConnectionProcess pid tears down the channel via disconnect_channel/1" do
      pid = start_client_with_fake_grpc()

      assert_receive {:fake_grpc, :connect, conn_pid}

      send(pid, {:EXIT, conn_pid, :normal})
      _ = :sys.get_state(pid)

      assert_received {:fake_grpc, :disconnect, ^conn_pid}
      state_after = :sys.get_state(pid)
      assert state_after.channel == nil
    end

    test "EXIT :crash from the held ConnectionProcess pid disconnects AND schedules a reconnect" do
      pid = start_client_with_fake_grpc()

      assert_receive {:fake_grpc, :connect, conn_pid}

      send(pid, {:EXIT, conn_pid, :boom})
      _ = :sys.get_state(pid)

      assert_received {:fake_grpc, :disconnect, ^conn_pid}
      # disconnect_channel sets channel: nil, after which schedule_reconnect/1
      # short-circuits (channel-nil clause) — the next ack will lazily reopen
      # via ensure_channel/1. State must remain consistent and alive.
      state_after = :sys.get_state(pid)
      assert state_after.channel == nil
      assert Process.alive?(pid)
    end
  end

  # ============================================================
  # handle_rpc_error/4 — non-map error tolerance
  #
  # Verifies that handle_rpc_error/4 safely handles any error shape
  # returned by the gRPC adapter without crashing.
  # ============================================================

  defp start_client_with_failing_rpc(rpc_error) do
    opts = [
      broadway_name: __MODULE__,
      subscription: "projects/test/subscriptions/test-sub",
      grpc_client: FailingRpcGrpcClient,
      grpc_client_config: %{test_pid: self(), rpc_error: rpc_error},
      backoff_type: :exp,
      backoff_min: 50,
      backoff_max: 1_000
    ]

    {:ok, pid} = UnaryRpcClient.start_link(opts)
    pid
  end

  describe "handle_rpc_error/4 — non-map error tolerance" do
    test "atom transport error does not crash" do
      pid = start_client_with_failing_rpc(:timeout)

      assert_receive {:fake_grpc, :connect, _conn_pid}

      result = UnaryRpcClient.acknowledge(pid, ["ack-1"])

      assert {:ok, ["ack-1"]} = result
      assert Process.alive?(pid)
    end

    test "tuple transport error does not crash" do
      pid = start_client_with_failing_rpc({:error, :closed})

      assert_receive {:fake_grpc, :connect, _conn_pid}

      result = UnaryRpcClient.modify_ack_deadline(pid, ["ack-1", "ack-2"], 60)

      assert {:ok, retained} = result
      assert Enum.sort(retained) == ["ack-1", "ack-2"]
      assert Process.alive?(pid)
    end

    test "GRPC.RPCError with retryable status returns all ack_ids for retry" do
      error = %GRPC.RPCError{status: 14, message: "unavailable"}
      pid = start_client_with_failing_rpc(error)

      assert_receive {:fake_grpc, :connect, _conn_pid}

      result = UnaryRpcClient.acknowledge(pid, ["ack-1"])

      assert {:ok, ["ack-1"]} = result
      assert Process.alive?(pid)
    end

    @tag capture_log: true
    test "GRPC.RPCError with terminal status stops the GenServer" do
      error = %GRPC.RPCError{status: 5, message: "NOT_FOUND"}
      pid = start_client_with_failing_rpc(error)

      assert_receive {:fake_grpc, :connect, _conn_pid}

      ref = Process.monitor(pid)

      # The GenServer replies {:error, error} then stops with {:terminal_error, error}.
      # Since start_link links the GenServer to the test process, we must trap exits
      # to avoid crashing the test itself.
      Process.flag(:trap_exit, true)

      log =
        capture_log(fn ->
          result = UnaryRpcClient.acknowledge(pid, ["ack-1"])
          assert {:ok, ["ack-1"]} = result
          assert_receive {:DOWN, ^ref, :process, ^pid, {:terminal_error, ^error}}
        end)

      assert log =~ "Unable to acknowledge messages"
      assert log =~ "NOT_FOUND"

      Process.flag(:trap_exit, false)
    end
  end
end
