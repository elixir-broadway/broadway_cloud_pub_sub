defmodule BroadwayCloudPubSub.Streaming.Client do
  @moduledoc """
  Behaviour for gRPC interactions used by the streaming Pub/Sub producer.

  The default implementation is `BroadwayCloudPubSub.Streaming.GrpcClient`.
  Provide a custom module via the `:grpc_client` option on
  `BroadwayCloudPubSub.Producer` for testing or alternate transports.

  ## Implementing a custom client

  A custom client module must implement all callbacks in this behaviour. The `init/1`
  callback receives the full producer options and returns an opaque `config` term that
  is stored in state and forwarded as the last argument to every subsequent call.

  Example:

      defmodule MyApp.FakeGrpcClient do
        @behaviour BroadwayCloudPubSub.Streaming.Client

        @impl true
        def init(opts), do: {:ok, Map.new(opts)}

        @impl true
        def connect(config), do: {:ok, :fake_channel}

        # ... implement remaining callbacks
      end

  Then configure the producer:

      {BroadwayCloudPubSub.Producer,
       grpc_client: MyApp.FakeGrpcClient,
       subscription: "projects/my-project/subscriptions/my-sub",
       ...}
  """

  @typedoc "An opaque term returned by `init/1` and passed to all subsequent calls."
  @type config :: term()

  @typedoc "An opaque gRPC channel returned by `connect/1`."
  @type channel :: term()

  @typedoc "An opaque gRPC bidirectional stream returned by `streaming_pull/2`."
  @type stream :: term()

  # --- Lifecycle ---

  @doc """
  Invoked once during producer startup to normalize options into a `config` term.

  The `config` term is stored in state and forwarded as the last argument to all
  subsequent callbacks, analogous to how `c:BroadwayCloudPubSub.Pull.Client.init/1`
  works for the pull producer.
  """
  @callback init(opts :: keyword()) :: {:ok, config()} | {:error, term()}

  @doc """
  Opens a gRPC channel to the Pub/Sub service.

  Called by `StreamManager` and `UnaryRpcClient` before each (re)connection.
  Should handle token fetching, TLS setup, and adapter selection internally.
  Returns `{:ok, channel}` on success or `{:error, reason}` on failure.
  """
  @callback connect(config()) :: {:ok, channel()} | {:error, reason :: term()}

  @doc """
  Closes a gRPC channel.

  Called during reconnect, shutdown, and error recovery. Implementations should
  handle the case where the channel is already closed or the connection process
  is dead.
  """
  @callback disconnect(channel(), config()) :: :ok

  @doc """
  Returns the pid that owns the underlying transport connection for `channel`,
  or `nil` if the channel does not have one (e.g. an already-closed channel,
  or a custom client whose transport is not pid-backed).

  Implementations should return `nil` rather than raise on unknown channel
  shapes; callers treat `nil` as “no match possible” and ignore the EXIT.
  """
  @callback connection_pid(channel()) :: pid() | nil

  # --- Streaming RPCs ---

  @doc """
  Opens a bidirectional `StreamingPull` gRPC stream on the given channel.

  Returns the stream struct. Called by `StreamReader` after a successful `connect/1`.
  """
  @callback streaming_pull(channel(), config()) :: stream()

  @doc """
  Sends a request on an open bidirectional gRPC stream.

  Used to send the initial `StreamingPullRequest` and subsequent keep-alive or
  deadline-extension requests on the stream. Returns `{:ok, stream}` (the stream
  struct may be updated by the underlying library after a send) or `{:error, reason}`.
  """
  @callback send_request(stream(), request :: term(), config()) ::
              {:ok, stream()} | {:error, term()}

  @doc """
  Begins enumerating responses from an open bidirectional gRPC stream.

  Returns `{:ok, enumerable}` where `enumerable` yields `{:ok, response}` or
  `{:error, error}` terms as the server sends data. The enumeration blocks until
  the stream closes. Any timeout or blocking behaviour is an internal implementation
  detail of the client.
  """
  @callback recv(stream(), config()) :: {:ok, Enumerable.t()} | {:error, term()}

  @doc """
  Cancels an open gRPC stream.

  Called during graceful shutdown or error recovery to stop receiving new messages.
  Implementations should handle the case where the stream is already closed.
  """
  @callback cancel(stream(), config()) :: :ok

  # --- Unary RPCs ---

  @doc """
  Sends an `Acknowledge` unary RPC to the Pub/Sub service.

  `request` is a `BroadwayCloudPubSub.Google.Pubsub.V1.AcknowledgeRequest` struct. Returns `{:ok, response}`
  on success or `{:error, reason}` on failure. Implementations may emit telemetry spans.
  """
  @callback acknowledge(channel(), request :: term(), config()) ::
              {:ok, term()} | {:error, term()}

  @doc """
  Sends a `ModifyAckDeadline` unary RPC to the Pub/Sub service.

  `request` is a `BroadwayCloudPubSub.Google.Pubsub.V1.ModifyAckDeadlineRequest` struct. Returns
  `{:ok, response}` on success or `{:error, reason}` on failure. Implementations
  may emit telemetry spans.
  """
  @callback modify_ack_deadline(channel(), request :: term(), config()) ::
              {:ok, term()} | {:error, term()}
end
