defmodule BroadwayCloudPubSub.Streaming.Options do
  @moduledoc false

  @default_grpc_endpoint "pubsub.googleapis.com:443"
  @default_max_outstanding_messages 1_000
  @default_max_outstanding_bytes 100 * 1024 * 1024
  @default_stream_ack_deadline_seconds 60
  # 60 minutes, matches the official client library default.
  @default_max_extension_ms 60 * 60 * 1_000
  # gax defaults: https://github.com/googleapis/gax-go/blob/main/v2/call_option.go
  @default_backoff_min 100
  @default_backoff_max 60_000
  @default_keepalive_interval_ms 30_000
  @default_ack_batch_interval_ms 100
  @default_ack_batch_max_size 2_500

  definition = [
    # Handled by Broadway.
    broadway: [type: :any, doc: false],
    broadway_name: [type: :atom, doc: false],

    # -- Connection --
    subscription: [
      type: {:custom, __MODULE__, :type_non_empty_string, [[{:name, :subscription}]]},
      required: true,
      doc: """
      The name of the subscription, including the project.
      For example, if your project is `"my-project"` and your
      subscription is `"my-subscription"`, the full name is
      `"projects/my-project/subscriptions/my-subscription"`.
      """
    ],
    goth: [
      type: :atom,
      doc: """
      The `Goth` module to use for authentication. Note that this option only
      applies to the default token generator.
      """
    ],
    token_generator: [
      type: :mfa,
      doc: """
      An MFArgs tuple that will be called before each gRPC connection to fetch
      an authentication token. Should return `{:ok, String.t()} | {:error, any()}`.
      By default this will invoke `Goth.fetch/1` with the `:goth` option.
      """
    ],

    # -- Flow control --
    max_outstanding_messages: [
      type: :pos_integer,
      default: @default_max_outstanding_messages,
      doc: """
      The maximum number of outstanding messages (delivered but not yet
      acknowledged) that the server will push. Acts as the primary flow
      control mechanism. Analogous to AMQP `prefetch_count`.
      """
    ],
    max_outstanding_bytes: [
      type: :pos_integer,
      default: @default_max_outstanding_bytes,
      doc: """
      The maximum total size in bytes of outstanding messages. The server
      will not push more messages if the total byte size of outstanding
      messages exceeds this limit.
      """
    ],

    # -- Acknowledgement --
    on_success: [
      type: {:custom, __MODULE__, :type_ack_option, [[{:name, :on_success}]]},
      default: :ack,
      doc: """
      Configures the acknowledgement behaviour for successfully processed
      messages.
      """
    ],
    on_failure: [
      type: {:custom, __MODULE__, :type_ack_option, [[{:name, :on_failure}]]},
      default: {:nack, 0},
      doc: """
      Configures the acknowledgement behaviour for failed messages. The
      default makes failed messages immediately available for redelivery.
      """
    ],
    on_shutdown: [
      type: {:custom, __MODULE__, :type_shutdown_option, [[{:name, :on_shutdown}]]},
      default: {:nack, 5},
      doc: """
      Configures what happens to messages received but not yet processed
      when the producer is shut down.

          - `{:nack, seconds}` - Sends a `modifyAckDeadline` request with the
          given `seconds` for all outstanding messages, making them available
          for redelivery after that delay.
          - `:nack` - Equivalent to `{:nack, 0}`. Immediately makes unprocessed
          messages available for redelivery.
          - `:noop` - Does nothing. Messages become available after their ack
          deadline expires naturally.
      """
    ],

    # -- Lease management --
    stream_ack_deadline_seconds: [
      type:
        {:custom, __MODULE__, :type_integer_in_range,
         [[{:name, :stream_ack_deadline_seconds}, {:min, 10}, {:max, 600}]]},
      default: @default_stream_ack_deadline_seconds,
      doc: """
      The number of seconds the server will wait before re-delivering an
      unacknowledged message. Must be between 10 and 600.
      The producer will extend leases automatically before this deadline.
      """
    ],
    max_extension_ms: [
      type: :pos_integer,
      default: @default_max_extension_ms,
      doc: """
      The maximum total time in milliseconds that a message's ack deadline will
      be extended from the moment of initial receipt. After this duration, the
      message is dropped from lease management and the server will redeliver it.
      This prevents a stuck consumer from holding messages indefinitely.
      """
    ],

    # -- Message ordering --
    enable_message_ordering: [
      type: :boolean,
      default: false,
      doc: """
      When `true`, messages with the same `ordering_key` are routed to the
      same Broadway processor and processed sequentially. This guarantees
      in-order delivery for ordered subscriptions.

          Ordering is enforced via Broadway's built-in `:partition_by` option,
          which assigns messages with the same `orderingKey` metadata to the
          same processor partition. The subscription itself must also have
          message ordering enabled in Google Cloud Pub/Sub.

          When `false` (default), messages are distributed across processors
          without regard to ordering key, matching the unordered behaviour of a
          standard Pub/Sub subscription.

          Note: the server will also report whether the subscription has ordering
          enabled in each `StreamingPullResponse.subscription_properties`. This
          client-side option controls whether to enforce it in the Broadway
          processing topology.
      """
    ],
    client_id: [
      type: :string,
      doc: """
      An identifier shared across all streaming connections opened by this
      pipeline. When a stream disconnects and reconnects with the same
      `client_id`, the server transfers any guarantees (e.g. ordered delivery
      assignment) from the old stream to the new one. If not provided, a
      random ID is generated once at pipeline startup. This is sufficient for
      most use cases: the ID is stable across gRPC reconnections within the
      same process lifetime, which is all the server requires. You only need
      to set this explicitly if you want a human-readable value for debugging.
      """
    ],

    # -- Ack batching --
    ack_batch_interval_ms: [
      type:
        {:custom, __MODULE__, :type_integer_in_range,
         [[{:name, :ack_batch_interval_ms}, {:min, 10}, {:max, 5_000}]]},
      default: @default_ack_batch_interval_ms,
      doc: """
      Interval in milliseconds at which batched ack and modifyAckDeadline
      requests are flushed to the Pub/Sub service via unary RPCs.
      Lower values reduce end-to-end ack latency; higher values improve
      batching efficiency.
      """
    ],
    ack_batch_max_size: [
      type:
        {:custom, __MODULE__, :type_integer_in_range,
         [[{:name, :ack_batch_max_size}, {:min, 1}, {:max, 10_000}]]},
      default: @default_ack_batch_max_size,
      doc: """
      Maximum number of ack_ids to accumulate before triggering an
      immediate flush, regardless of the timer. Each unary RPC carries
      at most 2,500 ack_ids (the Google API limit), so values above 2,500
      result in multiple RPCs per flush.
      """
    ],
    retry_deadline_ms: [
      type: :pos_integer,
      default: 60_000,
      doc: """
      Maximum total time in milliseconds to keep retrying a failed acknowledge or
      modifyAckDeadline request before giving up and dropping the ack_ids.

          The default of 60,000ms (60 seconds) applies to standard delivery subscriptions.
          When exactly-once delivery is detected from subscription properties, the library
          automatically switches to 600,000ms (600 seconds) for exactly-once acks. The
          configured value is restored if exactly-once delivery is later disabled on the
          subscription.
      """
    ],

    # -- Reconnection --
    backoff_type: [
      type: {:in, [:rand_exp, :exp, :rand, :stop]},
      default: :rand_exp,
      doc: """
      The backoff strategy used when reconnecting after a stream failure.

          - `:rand_exp` - Randomized exponential backoff. Adds jitter to
          prevent thundering herd after mass disconnects.
          - `:exp` - Pure exponential backoff.
          - `:rand` - Random value between `backoff_min` and `backoff_max`.
          - `:stop` - Do not reconnect. The producer will crash after one
          failure.
      """
    ],
    backoff_min: [
      type: :pos_integer,
      default: @default_backoff_min,
      doc: "Minimum reconnection backoff in milliseconds."
    ],
    backoff_max: [
      type: :pos_integer,
      default: @default_backoff_max,
      doc: "Maximum reconnection backoff in milliseconds."
    ],

    # -- Shutdown --
    drain_timeout_ms: [
      type: :pos_integer,
      default: 30_000,
      doc: """
      Maximum time in milliseconds to wait for in-flight messages to be
      processed and acknowledged during graceful shutdown. After this timeout,
      any remaining outstanding messages are nacked (per the `on_shutdown`
      setting) and the connection is force-closed.

          This drain phase waits for all outstanding messages to be acked before
          calling `CloseSend` on the stream.
      """
    ],

    # -- Transport --
    adapter: [
      type: {:custom, __MODULE__, :type_adapter, [[{:name, :adapter}]]},
      default: :gun,
      doc: """
      The gRPC HTTP/2 adapter to use for the streaming connection.

          - `:gun` - Uses the Gun HTTP/2 client. Well-tested and the
          traditional adapter for the Elixir gRPC library.
          - `:mint` - Uses the Mint HTTP/2 client. May be preferable where
          Gun is not available.
          - Any module implementing the `GRPC.Client.Adapter` behaviour.

          Both built-in adapters are provided by the `grpc` dependency. The
          adapter choice does not affect the public API or message semantics.
      """
    ],
    grpc_endpoint: [
      type: {:custom, __MODULE__, :type_non_empty_string, [[{:name, :grpc_endpoint}]]},
      default: @default_grpc_endpoint,
      doc: """
      The gRPC endpoint for the Cloud Pub/Sub service. Useful for testing
      with the Pub/Sub emulator (e.g., `"localhost:8085"`).
      """
    ],
    use_ssl: [
      type: :boolean,
      default: true,
      doc: """
      Whether to use TLS when connecting to the gRPC endpoint. Set to `false`
      when connecting to the Pub/Sub emulator, which does not use TLS.
      """
    ],
    keepalive_interval_ms: [
      type: :pos_integer,
      default: @default_keepalive_interval_ms,
      doc: """
      Interval in milliseconds at which HTTP/2 PING frames are sent on the gRPC
      connection to keep it alive. This prevents Google Cloud's load balancer
      from closing idle connections (which it does after roughly 20 seconds by
      default). Only applies to the `:gun` adapter.
      """
    ],
    interceptors: [
      type: {:custom, __MODULE__, :type_interceptors, [[]]},
      default: [],
      doc: """
      A list of client-side gRPC interceptors attached to every channel opened
      by the producer (both the StreamingPull channel and the unary ack/modack channel).

          Each entry is either a bare module (e.g. `MyInterceptor`, which calls
          `MyInterceptor.init([])`) or a `{module, opts}` tuple (e.g.
          `{MyInterceptor, level: :debug}`, which calls
          `MyInterceptor.init(level: :debug)`).

          Modules must implement the `GRPC.Client.Interceptor` behaviour (`init/1` and `call/4`).

          ## Example

              interceptors: [GRPC.Client.Interceptors.Logger]
              interceptors: [{GRPC.Client.Interceptors.Logger, level: :warning}]
      """
    ],

    # -- Advanced --
    grpc_client: [
      type: {:custom, __MODULE__, :type_grpc_client, [[]]},
      default: BroadwayCloudPubSub.Streaming.GrpcClient,
      doc: """
      The module implementing the `BroadwayCloudPubSub.Streaming.Client` behaviour.
      The built-in `BroadwayCloudPubSub.Streaming.GrpcClient` uses the `grpc`
      library to communicate with Google Cloud Pub/Sub.

          Accepts either a bare module or a `{module, opts}` tuple. When a tuple is
          given, `opts` are merged into the producer options and passed to
          `c:BroadwayCloudPubSub.Streaming.Client.init/1`:

              grpc_client: {MyGrpcClient, channel_opts: [transport_opts: []]}

          Swap this for testing or custom gRPC transports.
      """
    ],
    telemetry_metadata: [
      type: {:custom, __MODULE__, :type_telemetry_metadata, [[]]},
      doc: """
      Extra data to attach to every telemetry event emitted by the streaming
      producer. The value is included in the event metadata under the `:extra`
      key.

          Accepts either a static term (e.g. a map or keyword list), which is
          stored once and included verbatim in every event, or an
          `{module, function, args}` tuple that is called on every event emission
          and whose return value is used as the `:extra` value (useful for
          attaching dynamic data such as node names or runtime counters).

          When not set, no `:extra` key is added to event metadata.
      """
    ],

    # Testing options
    test_pid: [type: :pid, doc: false]
  ]

  @definition NimbleOptions.new!(definition)

  def definition do
    @definition
  end

  @acknowledger_definition definition
                           |> Keyword.take([:on_failure, :on_success])
                           |> NimbleOptions.new!()

  def acknowledger_definition do
    @acknowledger_definition
  end

  @doc """
  Builds an MFArgs tuple for a token generator using Goth.
  """
  defdelegate make_token_generator(opts), to: BroadwayCloudPubSub.Pull.Options

  # --- Custom type validators ---

  def type_non_empty_string("", [{:name, name}]) do
    {:error, "expected :#{name} to be a non-empty string, got: \"\""}
  end

  def type_non_empty_string(value, _) when is_binary(value) do
    {:ok, value}
  end

  def type_non_empty_string(value, [{:name, name}]) do
    {:error, "expected :#{name} to be a non-empty string, got: #{inspect(value)}"}
  end

  def type_integer_in_range(value, [{:name, _name}, {:min, min}, {:max, max}])
      when is_integer(value) and value >= min and value <= max do
    {:ok, value}
  end

  def type_integer_in_range(value, [{:name, name}, {:min, min}, {:max, max}]) do
    {:error,
     "expected :#{name} to be an integer between #{min} and #{max}, got: #{inspect(value)}"}
  end

  def type_ack_option(:ack, _), do: {:ok, :ack}
  def type_ack_option(:noop, _), do: {:ok, :noop}
  def type_ack_option(:nack, _), do: {:ok, {:nack, 0}}

  def type_ack_option({:nack, value}, _)
      when is_integer(value) and value >= 0 and value <= 600 do
    {:ok, {:nack, value}}
  end

  def type_ack_option(value, [{:name, name}]) do
    {:error,
     "expected :#{name} to be one of :ack, :noop, :nack, or {:nack, integer} where " <>
       "integer is between 0 and 600, got: #{inspect(value)}"}
  end

  def type_shutdown_option(:nack, _), do: {:ok, {:nack, 0}}
  def type_shutdown_option(:noop, _), do: {:ok, :noop}

  def type_shutdown_option({:nack, value}, _)
      when is_integer(value) and value >= 0 and value <= 600 do
    {:ok, {:nack, value}}
  end

  def type_shutdown_option(value, [{:name, name}]) do
    {:error,
     "expected :#{name} to be :nack, :noop, or {:nack, integer} where " <>
       "integer is between 0 and 600, got: #{inspect(value)}"}
  end

  def type_grpc_client(mod, _opts) when is_atom(mod) and not is_nil(mod), do: {:ok, mod}

  def type_grpc_client({mod, inner_opts}, _opts)
      when is_atom(mod) and not is_nil(mod) and is_list(inner_opts),
      do: {:ok, {mod, inner_opts}}

  def type_grpc_client(value, _opts) do
    {:error,
     "expected :grpc_client to be a module or {module, opts} tuple, got: #{inspect(value)}"}
  end

  def type_adapter(:gun, _), do: {:ok, GRPC.Client.Adapters.Gun}
  def type_adapter(:mint, _), do: {:ok, GRPC.Client.Adapters.Mint}

  def type_adapter(mod, [{:name, name}]) when is_atom(mod) do
    case Code.ensure_loaded(mod) do
      {:module, ^mod} ->
        if function_exported?(mod, :connect, 2) do
          {:ok, mod}
        else
          {:error,
           "expected :#{name} to be a module implementing GRPC.Client.Adapter, " <>
             "but #{inspect(mod)} does not export connect/2"}
        end

      {:error, _} ->
        {:error,
         "expected :#{name} to be :gun, :mint, or a module implementing GRPC.Client.Adapter, " <>
           "but #{inspect(mod)} is not a loaded module"}
    end
  end

  def type_adapter(value, [{:name, name}]) do
    {:error,
     "expected :#{name} to be :gun, :mint, or a module implementing GRPC.Client.Adapter, " <>
       "got: #{inspect(value)}"}
  end

  def type_interceptors(list, _opts) when is_list(list) do
    case Enum.find(Enum.map(list, &validate_interceptor/1), &match?({:error, _}, &1)) do
      nil -> {:ok, list}
      {:error, _} = err -> err
    end
  end

  def type_interceptors(value, _opts) do
    {:error, "expected :interceptors to be a list, got: #{inspect(value)}"}
  end

  defp validate_interceptor({mod, _opts}) when is_atom(mod), do: validate_interceptor_module(mod)
  defp validate_interceptor(mod) when is_atom(mod), do: validate_interceptor_module(mod)

  defp validate_interceptor(value) do
    {:error,
     "expected each interceptor to be a module or {module, opts} tuple, got: #{inspect(value)}"}
  end

  defp validate_interceptor_module(mod) do
    case Code.ensure_loaded(mod) do
      {:module, ^mod} ->
        if function_exported?(mod, :init, 1) and function_exported?(mod, :call, 4) do
          {:ok, mod}
        else
          {:error,
           "expected interceptor #{inspect(mod)} to implement GRPC.Client.Interceptor " <>
             "(init/1 and call/4), but one or both callbacks are missing"}
        end

      {:error, _} ->
        {:error,
         "expected interceptor to be a loaded module, but #{inspect(mod)} could not be loaded"}
    end
  end

  @doc false
  def type_telemetry_metadata({m, f, a}, _opts) when is_atom(m) and is_atom(f) and is_list(a) do
    case Code.ensure_loaded(m) do
      {:module, ^m} ->
        if function_exported?(m, f, length(a)) do
          {:ok, {m, f, a}}
        else
          {:error,
           "expected :telemetry_metadata MFA to be an exported function, " <>
             "but #{inspect(m)}.#{f}/#{length(a)} is not exported"}
        end

      {:error, _} ->
        {:error,
         "expected :telemetry_metadata MFA to reference a loaded module, " <>
           "but #{inspect(m)} could not be loaded"}
    end
  end

  def type_telemetry_metadata(term, _opts), do: {:ok, term}

  @doc """
  Validates and selects child options from a full config keyword list.

  Takes the given `all_keys`, validates that all `required_keys` are present,
  and returns only the selected keys. Used by `AckBatcher.child_opts/1` and
  `UnaryRpcClient.child_opts/1` to extract their required options from the
  shared pipeline config.
  """
  @spec validate_child_opts(keyword(), [atom()], [atom()]) :: keyword()
  def validate_child_opts(opts, all_keys, required_keys) do
    picked = Keyword.take(opts, all_keys)

    Enum.each(required_keys, fn key ->
      unless Keyword.has_key?(picked, key) do
        raise ArgumentError,
              "missing required option #{inspect(key)} for child opts"
      end
    end)

    picked
  end
end
