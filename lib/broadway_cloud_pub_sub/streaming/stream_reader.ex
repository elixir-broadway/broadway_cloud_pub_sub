defmodule BroadwayCloudPubSub.Streaming.StreamReader do
  @moduledoc false

  # A short-lived process that owns the gRPC bidirectional streaming connection
  # and forwards decoded messages to the StreamManager.
  #
  # ## Why a separate process?
  #
  # The `grpc_client` library's `GRPC.Stub.recv/2` returns a blocking `Enumerable`.
  # A GenServer cannot block on enumeration (it would stop processing casts,
  # calls, and timers). By spawning a dedicated reader process, the GenServer
  # remains fully responsive while streaming runs concurrently.
  #
  # ## Message protocol with StreamManager
  #
  # After the stream is opened, this process sends the grpc_stream back:
  #
  #   `{:stream_opened, pid, grpc_stream}`
  #
  # Then it forwards received messages and lifecycle events:
  #
  #   `{:stream_messages, [ReceivedMessage.t()]}` — one or more decoded messages
  #   `{:stream_error, error}`                    — stream-level gRPC error
  #   `{:stream_closed}`                          — server closed stream normally
  #
  # On exit (normal or crash), the StreamManager detects it via the linked
  # process `{:EXIT, pid, reason}` signal (StreamManager traps exits).
  #
  # ## Sending on the stream (acks, deadline modifications)
  #
  # After receiving `{:stream_opened, _pid, grpc_stream}`, the StreamManager
  # calls `grpc_client.send_request(grpc_stream, request, client_config)` directly
  # from the GenServer process. Both the Gun and Mint adapters implement this as a
  # fire-and-forget cast, safe to call from any process concurrently with the
  # reader process enumerating the receive stream.

  alias BroadwayCloudPubSub.Google.Pubsub.V1.{StreamingPullRequest, StreamingPullResponse}

  @doc """
  Spawns a linked reader process. The reader opens the gRPC stream and sends
  the stream struct back to `manager` via `{:stream_opened, self(), grpc_stream}`
  before beginning enumeration.

  Returns the reader pid.
  """
  @spec start_link(pid(), channel :: term(), map()) :: {:ok, pid()}
  def start_link(manager, channel, config) do
    Task.start_link(fn -> run(manager, channel, config) end)
  end

  # --- Private ---

  defp run(manager, channel, config) do
    grpc_client = config.grpc_client
    grpc_client_config = config.grpc_client_config

    grpc_stream = open_stream(channel, config)

    # Notify the manager that the stream is open. The manager needs the
    # grpc_stream struct to call send_request for acks and deadline
    # modifications on the bidirectional stream.
    send(manager, {:stream_opened, self(), grpc_stream})

    case grpc_client.recv(grpc_stream, grpc_client_config) do
      {:ok, enum} -> enumerate(enum, manager)
      {:error, error} -> send(manager, {:stream_error, error})
    end
  end

  defp open_stream(channel, config) do
    grpc_client = config.grpc_client
    grpc_client_config = config.grpc_client_config
    client_id = Map.fetch!(config, :client_id)

    initial_request = %StreamingPullRequest{
      subscription: config.subscription,
      stream_ack_deadline_seconds: config.stream_ack_deadline_seconds,
      max_outstanding_messages: config.max_outstanding_messages,
      max_outstanding_bytes: config.max_outstanding_bytes,
      client_id: client_id
    }

    stream = grpc_client.streaming_pull(channel, grpc_client_config)
    # Intentional match to crash if the stream fails to open
    {:ok, stream} = grpc_client.send_request(stream, initial_request, grpc_client_config)
    stream
  end

  defp enumerate(enum, manager) do
    enum
    |> Stream.each(fn
      {:ok, %StreamingPullResponse{received_messages: msgs, subscription_properties: props}} ->
        # Forward subscription_properties whenever the server sends them.
        # The server may send this on any response (including heartbeats) to
        # signal that the subscription's ordering or exactly-once settings have
        # changed. StreamManager stores the latest value in state.
        if props != nil do
          send(manager, {:subscription_properties, props})
        end

        if msgs != [] do
          send(manager, {:stream_messages, msgs})
        end

      {:error, error} ->
        send(manager, {:stream_error, error})
    end)
    |> Stream.run()

    # Stream exhausted normally — notify manager before exiting.
    # Sending {:stream_closed} lets StreamManager distinguish normal closes
    # from crashes before the {:EXIT, reader_pid, :normal} signal arrives.
    send(manager, {:stream_closed})
  end
end
