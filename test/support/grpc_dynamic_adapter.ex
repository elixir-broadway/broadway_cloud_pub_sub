defmodule BroadwayCloudPubSub.Test.GrpcDynamicAdapter do
  @moduledoc """
  A controllable GRPC adapter for unit tests.

  Unlike `GrpcTestAdapter`, this adapter keeps the stream open and allows the
  test process to push responses, errors, or an end-of-stream signal into the
  live stream.

  ## Usage

      # Start a StreamManager with this adapter and a test_pid.
      pid = start_manager(adapter: GrpcDynamicAdapter, test_pid: self())

      # Wait for the adapter to signal that it connected.
      assert_receive {:adapter_connected, ctrl}

      # Push a response into the stream.
      GrpcDynamicAdapter.push_response(ctrl, %StreamingPullResponse{...})

      # End the stream.
      GrpcDynamicAdapter.push_end_stream(ctrl)

  ## Notifications sent to `test_pid`

    * `{:adapter_connected, ctrl}` — adapter connected, `ctrl` is the Controller pid
    * `{:adapter_call, :connect}` — `connect/2` was called
    * `{:adapter_call, :disconnect}` — `disconnect/1` was called
    * `{:adapter_call, :send_headers}` — `send_headers/2` was called
    * `{:adapter_call, :send_request, message}` — `send_request/3` was called
    * `{:adapter_call, :send_data, message}` — `send_data/3` was called
    * `{:adapter_call, :end_stream}` — `end_stream/1` was called
    * `{:adapter_call, :cancel}` — `cancel/1` was called
  """

  @behaviour GRPC.Client.Adapter

  # --- Controller GenServer ---

  defmodule Controller do
    @moduledoc false
    use GenServer

    def start_link do
      GenServer.start_link(__MODULE__, :ok)
    end

    def stop(ctrl), do: GenServer.stop(ctrl)

    @doc "Push a decoded response into the stream."
    def push_response(ctrl, response) do
      GenServer.cast(ctrl, {:push, {:ok, response}})
    end

    @doc "Push an error into the stream."
    def push_error(ctrl, error) do
      GenServer.cast(ctrl, {:push, {:error, error}})
    end

    @doc "Signal end-of-stream — the receive_data enumerable will halt."
    def push_end_stream(ctrl) do
      GenServer.cast(ctrl, {:push, :end_stream})
    end

    @doc "Block until one item is available in the queue (called from StreamReader process)."
    def pop(ctrl, timeout) do
      GenServer.call(ctrl, :pop, timeout)
    end

    # --- GenServer callbacks ---

    def init(:ok) do
      {:ok, %{queue: :queue.new(), waiting: nil}}
    end

    def handle_cast({:push, item}, %{waiting: nil} = state) do
      {:noreply, %{state | queue: :queue.in(item, state.queue)}}
    end

    def handle_cast({:push, item}, %{waiting: from} = state) do
      GenServer.reply(from, item)
      {:noreply, %{state | waiting: nil}}
    end

    def handle_call(:pop, from, state) do
      case :queue.out(state.queue) do
        {{:value, item}, rest} ->
          {:reply, item, %{state | queue: rest}}

        {:empty, _} ->
          {:noreply, %{state | waiting: from}}
      end
    end
  end

  # --- Adapter callbacks ---

  @impl GRPC.Client.Adapter
  def connect(%GRPC.Channel{} = channel, opts) do
    test_pid = Keyword.get(opts, :test_pid)
    {:ok, ctrl} = Controller.start_link()

    if test_pid do
      send(test_pid, {:adapter_connected, ctrl})
      send(test_pid, {:adapter_call, :connect})
    end

    payload = %{conn_pid: self(), ctrl: ctrl, test_pid: test_pid}
    {:ok, %{channel | adapter_payload: payload}}
  end

  @impl GRPC.Client.Adapter
  def disconnect(%GRPC.Channel{} = channel) do
    notify(channel, :disconnect)

    case channel.adapter_payload do
      %{ctrl: ctrl} when is_pid(ctrl) ->
        if Process.alive?(ctrl), do: Controller.stop(ctrl)

      _ ->
        :ok
    end

    {:ok, %{channel | adapter_payload: %{conn_pid: nil}}}
  end

  @impl GRPC.Client.Adapter
  def send_headers(stream, _opts) do
    notify_stream(stream, :send_headers)
    GRPC.Client.Stream.put_payload(stream, :stream_ref, make_ref())
  end

  @impl GRPC.Client.Adapter
  def send_request(stream, message, _opts) do
    notify_stream(stream, {:send_request, message})
    GRPC.Client.Stream.put_payload(stream, :stream_ref, make_ref())
  end

  @impl GRPC.Client.Adapter
  def send_data(stream, message, _opts) do
    notify_stream(stream, {:send_data, message})
    stream
  end

  @impl GRPC.Client.Adapter
  def end_stream(stream) do
    notify_stream(stream, :end_stream)
    stream
  end

  @impl GRPC.Client.Adapter
  def cancel(stream) do
    notify_stream(stream, :cancel)
    :ok
  end

  # Returns a lazy Stream that blocks on Controller.pop/2 until items are pushed.
  @impl GRPC.Client.Adapter
  def receive_data(stream, _opts) do
    ctrl = stream.channel.adapter_payload.ctrl

    lazy =
      Stream.resource(
        fn -> ctrl end,
        fn ctrl ->
          try do
            case Controller.pop(ctrl, 5_000) do
              :end_stream -> {:halt, ctrl}
              {:ok, response} -> {[{:ok, response}], ctrl}
              {:error, err} -> {[{:error, err}], ctrl}
            end
          rescue
            _ -> {:halt, ctrl}
          catch
            :exit, _ -> {:halt, ctrl}
          end
        end,
        fn _ctrl -> :ok end
      )

    {:ok, lazy}
  end

  # --- Public API (delegates to Controller) ---

  defdelegate push_response(ctrl, response), to: Controller
  defdelegate push_error(ctrl, error), to: Controller
  defdelegate push_end_stream(ctrl), to: Controller

  # --- Private helpers ---

  defp notify(channel, event) do
    case channel.adapter_payload do
      %{test_pid: pid} when is_pid(pid) ->
        send(pid, {:adapter_call, event})

      _ ->
        :ok
    end
  end

  defp notify_stream(stream, event) do
    case stream.channel.adapter_payload do
      %{test_pid: pid} when is_pid(pid) ->
        send(pid, {:adapter_call, event})

      _ ->
        :ok
    end
  end
end
