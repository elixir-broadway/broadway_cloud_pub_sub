defmodule BroadwayCloudPubSub.Test.GrpcTestAdapter do
  @moduledoc """
  A static (no-op) GRPC adapter for unit tests.

  - `connect/2` populates `adapter_payload` with `conn_pid: self()` so that
    StreamManager's `{:stream_opened, ...}` handler can read a live pid.
  - `receive_data/2` returns `{:ok, []}` — an empty enumerable — which causes
    StreamReader to immediately send `{:stream_closed}` and exit. StreamManager
    will schedule a reconnect, which is acceptable for tests that inject state
    directly via `:sys.replace_state`.
  - All other callbacks are no-ops that keep the stream struct intact.

  Use `BroadwayCloudPubSub.Test.GrpcDynamicAdapter` when you need to push
  responses into an open stream from the test process.
  """

  @behaviour GRPC.Client.Adapter

  @impl GRPC.Client.Adapter
  def connect(%GRPC.Channel{} = channel, _opts) do
    {:ok, %{channel | adapter_payload: %{conn_pid: self()}}}
  end

  @impl GRPC.Client.Adapter
  def disconnect(%GRPC.Channel{} = channel) do
    {:ok, %{channel | adapter_payload: %{conn_pid: nil}}}
  end

  @impl GRPC.Client.Adapter
  def send_headers(stream, _opts) do
    GRPC.Client.Stream.put_payload(stream, :stream_ref, make_ref())
  end

  @impl GRPC.Client.Adapter
  def send_request(stream, _message, _opts) do
    GRPC.Client.Stream.put_payload(stream, :stream_ref, make_ref())
  end

  @impl GRPC.Client.Adapter
  def send_data(stream, _message, _opts), do: stream

  @impl GRPC.Client.Adapter
  def end_stream(stream), do: stream

  @impl GRPC.Client.Adapter
  def cancel(_stream), do: :ok

  # Returns an empty enumerable — StreamReader sends {:stream_closed} immediately.
  @impl GRPC.Client.Adapter
  def receive_data(_stream, _opts), do: {:ok, []}
end
