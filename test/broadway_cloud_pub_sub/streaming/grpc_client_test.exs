defmodule BroadwayCloudPubSub.Streaming.GrpcClientTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Streaming.GrpcClient
  alias BroadwayCloudPubSub.Test.TelemetryHelper
  alias BroadwayCloudPubSub.Google.Pubsub.V1.{AcknowledgeRequest, ModifyAckDeadlineRequest}

  describe "adapter_opts/2" do
    test "gives Mint its native keepalive options" do
      assert GrpcClient.adapter_opts(GRPC.Client.Adapters.Mint, 30_000) ==
               [keepalive: 30_000, keepalive_tolerance: 3]
    end

    test "Mint options pass the adapter's own validation" do
      opts = GrpcClient.adapter_opts(GRPC.Client.Adapters.Mint, 30_000)
      assert GRPC.Client.Adapters.Mint.validate_opts(opts) == :ok
    end

    test "gives Gun and custom adapters http2_opts" do
      expected = [http2_opts: %{keepalive: 15_000, settings_timeout: :infinity}]

      assert GrpcClient.adapter_opts(GRPC.Client.Adapters.Gun, 15_000) == expected
      assert GrpcClient.adapter_opts(BroadwayCloudPubSub.Test.GrpcTestAdapter, 15_000) == expected
    end
  end

  # Answers every RPC with the configured result instead of sending it.
  defmodule CannedInterceptor do
    @behaviour GRPC.Client.Interceptor

    @impl true
    def init(result), do: result

    @impl true
    def call(_stream, _request, _next, result), do: result
  end

  @subscription "projects/p/subscriptions/s"

  # Other async tests run the real GrpcClient too, so each test uses its own
  # broadway name and only matches events carrying it.
  setup context do
    name = context.test

    for rpc <- [:ack, :modack], kind <- [:stop, :exception] do
      handler_id = {__MODULE__, name, rpc, kind}

      :telemetry.attach(
        handler_id,
        [:broadway_cloud_pub_sub, :streaming, :grpc_client, rpc, kind],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: self(), msg: {rpc, kind}}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)
    end

    %{config: %{broadway_name: name, subscription: @subscription}, name: name}
  end

  defp channel(result) do
    %GRPC.Channel{codec: GRPC.Codec.Proto, interceptors: [{CannedInterceptor, result}]}
  end

  defp ack(result, config) do
    GrpcClient.acknowledge(channel(result), %AcknowledgeRequest{ack_ids: ["a", "b"]}, config)
  end

  defp modack(result, config) do
    request = %ModifyAckDeadlineRequest{ack_ids: ["a"], ack_deadline_seconds: 10}
    GrpcClient.modify_ack_deadline(channel(result), request, config)
  end

  describe "acknowledge/3" do
    test "reports result: :ok in the :stop metadata when the RPC succeeds", %{
      config: config,
      name: name
    } do
      assert {:ok, _} = ack({:ok, %Google.Protobuf.Empty{}}, config)

      assert_receive {{:ack, :stop}, _measurements, %{name: ^name} = metadata}
      assert %{result: :ok, subscription: @subscription, count: 2} = metadata
      refute Map.has_key?(metadata, :error)
    end

    test "reports result: :error and the error in the :stop metadata when the RPC fails", %{
      config: config,
      name: name
    } do
      error = GRPC.RPCError.exception(GRPC.Status.unavailable(), "down")

      assert {:error, ^error} = ack({:error, error}, config)

      assert_receive {{:ack, :stop}, _measurements, %{name: ^name} = metadata}
      assert %{result: :error, error: ^error, count: 2} = metadata
      refute_received {{:ack, :exception}, _, %{name: ^name}}
    end
  end

  describe "modify_ack_deadline/3" do
    test "reports result: :ok in the :stop metadata when the RPC succeeds", %{
      config: config,
      name: name
    } do
      assert {:ok, _} = modack({:ok, %Google.Protobuf.Empty{}}, config)

      assert_receive {{:modack, :stop}, _measurements, %{name: ^name} = metadata}
      assert %{result: :ok, subscription: @subscription, count: 1} = metadata
      refute Map.has_key?(metadata, :error)
    end

    test "reports result: :error and the error in the :stop metadata when the RPC fails", %{
      config: config,
      name: name
    } do
      error = GRPC.RPCError.exception(GRPC.Status.permission_denied(), "denied")

      assert {:error, ^error} = modack({:error, error}, config)

      assert_receive {{:modack, :stop}, _measurements, %{name: ^name} = metadata}
      assert %{result: :error, error: ^error, count: 1} = metadata
      refute_received {{:modack, :exception}, _, %{name: ^name}}
    end
  end
end
