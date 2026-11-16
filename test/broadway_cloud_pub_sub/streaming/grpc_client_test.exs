defmodule BroadwayCloudPubSub.Streaming.GrpcClientTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Streaming.GrpcClient

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
end
