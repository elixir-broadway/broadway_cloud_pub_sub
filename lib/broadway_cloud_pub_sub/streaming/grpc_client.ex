defmodule BroadwayCloudPubSub.Streaming.GrpcClient do
  @moduledoc """
  The default gRPC client for `BroadwayCloudPubSub.Producer`.

  Implements `BroadwayCloudPubSub.Streaming.Client` using the `grpc` library
  with the `Google.Pubsub.V1.Subscriber.Stub` generated stub.

  This module handles:
  - Token fetching and channel connection (with TLS and adapter configuration)
  - Bidirectional `StreamingPull` stream management
  - Unary `Acknowledge` and `ModifyAckDeadline` RPCs with telemetry spans

  ## Telemetry

  This module emits the following telemetry events:

  * `[:broadway_cloud_pub_sub, :streaming, :grpc_client, :ack, :start | :stop | :exception]` —
    emitted as a span when sending an `Acknowledge` unary RPC.

    Measurements: as described in `:telemetry.span/3`.
    Metadata: `%{name: broadway_name, subscription: subscription, count: ack_count}`

  * `[:broadway_cloud_pub_sub, :streaming, :grpc_client, :modack, :start | :stop | :exception]` —
    emitted as a span when sending a `ModifyAckDeadline` unary RPC.

    Measurements: as described in `:telemetry.span/3`.
    Metadata: `%{name: broadway_name, subscription: subscription, count: ack_count}`

  Custom `BroadwayCloudPubSub.Streaming.Client` implementations that wish to emit
  the same events should use the same event name prefix and metadata shape.
  """

  @behaviour BroadwayCloudPubSub.Streaming.Client

  alias BroadwayCloudPubSub.Streaming.Telemetry
  alias Google.Pubsub.V1.Subscriber.Stub
  alias Google.Pubsub.V1.{AcknowledgeRequest, ModifyAckDeadlineRequest}

  # Default RPC timeout for unary calls.
  @unary_rpc_timeout_ms 30_000

  @impl BroadwayCloudPubSub.Streaming.Client
  def init(opts) do
    {:ok, Map.new(opts)}
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def connect(config) do
    with {:ok, token} <- fetch_token(config) do
      open_channel(config, token)
    end
  rescue
    e -> {:error, {:connect_failed, Exception.message(e)}}
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def disconnect(channel, _config) do
    GRPC.Stub.disconnect(channel)
    :ok
  catch
    _, _ -> :ok
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  # Both the Mint and Gun adapters in the `grpc` library expose the connection-
  # owning process under `channel.adapter_payload.conn_pid`. Returns nil for any
  # other shape (e.g. a closed channel whose adapter_payload has been cleared)
  # so callers can safely pattern-match without raising.
  def connection_pid(%GRPC.Channel{adapter_payload: %{conn_pid: pid}}) when is_pid(pid) do
    pid
  end

  def connection_pid(_channel), do: nil

  @impl BroadwayCloudPubSub.Streaming.Client
  def streaming_pull(channel, _config) do
    Stub.streaming_pull(channel, [])
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def send_request(stream, request, _config) do
    case GRPC.Stub.send_request(stream, request) do
      %GRPC.Client.Stream{} = updated_stream -> {:ok, updated_stream}
      {:error, reason} -> {:error, reason}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def recv(stream, _config) do
    GRPC.Stub.recv(stream, timeout: :infinity)
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def cancel(stream, _config) do
    GRPC.Stub.cancel(stream)
    :ok
  catch
    _, _ -> :ok
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def acknowledge(channel, %AcknowledgeRequest{ack_ids: ack_ids} = request, config) do
    count = length(ack_ids)
    metadata = %{name: config.broadway_name, subscription: config.subscription, count: count}

    Telemetry.span(
      :grpc_client,
      :ack,
      metadata,
      fn ->
        result = Stub.acknowledge(channel, request, timeout: @unary_rpc_timeout_ms)

        {result, %{}}
      end,
      Map.get(config, :telemetry_metadata)
    )
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def modify_ack_deadline(
        channel,
        %ModifyAckDeadlineRequest{ack_ids: ack_ids} = request,
        config
      ) do
    count = length(ack_ids)
    metadata = %{name: config.broadway_name, subscription: config.subscription, count: count}

    Telemetry.span(
      :grpc_client,
      :modack,
      metadata,
      fn ->
        result = Stub.modify_ack_deadline(channel, request, timeout: @unary_rpc_timeout_ms)

        {result, %{}}
      end,
      Map.get(config, :telemetry_metadata)
    )
  end

  defp fetch_token(%{token_generator: {mod, fun, args}}) do
    apply(mod, fun, args)
  end

  defp open_channel(
         %{grpc_endpoint: endpoint, use_ssl: use_ssl, adapter: adapter} = config,
         token
       ) do
    keepalive_interval_ms = Map.get(config, :keepalive_interval_ms, 30_000)
    interceptors = Map.get(config, :interceptors, [])

    adapter_opts = [http2_opts: %{keepalive: keepalive_interval_ms, settings_timeout: :infinity}]

    adapter_opts =
      case Map.get(config, :test_pid) do
        nil -> adapter_opts
        pid -> Keyword.put(adapter_opts, :test_pid, pid)
      end

    base_opts = [
      adapter: adapter,
      headers: [{"authorization", "Bearer #{token}"}],
      adapter_opts: adapter_opts,
      interceptors: interceptors
    ]

    opts =
      if use_ssl do
        cred = GRPC.Credential.new(ssl: [cacerts: :public_key.cacerts_get()])
        Keyword.put(base_opts, :cred, cred)
      else
        base_opts
      end

    case GRPC.Stub.connect(endpoint, opts) do
      {:ok, channel} -> {:ok, channel}
      {:error, reason} -> {:error, {:connect_failed, reason}}
    end
  end
end
