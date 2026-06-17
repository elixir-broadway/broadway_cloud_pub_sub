defmodule BroadwayCloudPubSub.Options do
  @moduledoc false

  @default_base_url "https://pubsub.googleapis.com"

  @default_max_number_of_messages 10

  @default_receive_interval 5_000

  @default_receive_timeout :infinity

  definition = [
    # Handled by Broadway.
    broadway: [type: :any, doc: false],
    client: [
      type: {:or, [:atom, :mod_arg]},
      default: BroadwayCloudPubSub.PullClient,
      doc: """
      A module that implements the BroadwayCloudPubSub.Client behaviour.
      This module is responsible for fetching and acknowledging the messages.
      Pay attention that all options passed to the producer will be forwarded
      to the client. It's up to the client to normalize the options it needs.

      The BroadwayCloudPubSub.PullClient is the default client and will
      automatically retry the following errors [408, 500, 502, 503, 504, 522,
      524] up to 10 times with a 500ms pause between retries. This can be
      configured by passing the module with options to the client:

        {BroadwayCloudPubSub.PullClient,
          retry_codes: [502, 503],
          retry_delay_ms: 300,
          max_retries: 5}

      These options will be merged with the options to the producer and passed
      to the client init/1 function.
      """
    ],
    subscription: [
      type: {:custom, __MODULE__, :type_non_empty_string, [[{:name, :subscription}]]},
      required: true,
      doc: """
      The name of the subscription, including the project.
      For example, if you project is named `"my-project"` and your
      subscription is named `"my-subscription"`, then your subscription
      name is `"projects/my-project/subscriptions/my-subscription"`.
      """
    ],
    max_number_of_messages: [
      doc: "The maximum number of messages to be fetched per request.",
      type: :pos_integer,
      default: @default_max_number_of_messages
    ],
    on_failure: [
      type:
        {:custom, __MODULE__, :type_atom_action_or_nack_with_bounded_integer,
         [[{:name, :on_failure}, {:min, 0}, {:max, 600}]]},
      doc: """
      Configures the acking behaviour for failed messages.
      See the "Acknowledgements" section below for all the possible values.
      This option can also be changed for each message through
      `Broadway.Message.configure_ack/2`.
      """,
      default: :noop
    ],
    on_success: [
      type:
        {:custom, __MODULE__, :type_atom_action_or_nack_with_bounded_integer,
         [[{:name, :on_success}, {:min, 0}, {:max, 600}]]},
      doc: """
      Configures the acking behaviour for successful messages.
      See the "Acknowledgements" section below for all the possible values.
      This option can also be changed for each message through
      `Broadway.Message.configure_ack/2`.
      """,
      default: :ack
    ],
    on_draining: [
      type:
        {:custom, __MODULE__, :type_atom_action_or_nack_with_bounded_integer,
         [[{:name, :on_success}, {:min, 0}, {:max, 600}]]},
      doc: """
      Configures the acking behaviour for the non consumed messages when draining.
      See the "Acknowledgements" section below for all the possible values.
      """,
      default: {:nack, 0}
    ],
    receive_interval: [
      type: :integer,
      default: @default_receive_interval,
      doc: """
      The duration (in milliseconds) for which the producer waits
      before making a request for more messages.
      """
    ],
    receive_timeout: [
      type:
        {:custom, __MODULE__, :type_positive_integer_or_infinity, [[{:name, :receive_timeout}]]},
      default: @default_receive_timeout,
      doc: """
      The maximum time (in milliseconds) to wait for a response
      before the pull client returns an error.
      """
    ],
    goth: [
      type: :atom,
      doc: """
      The `Goth` module to use for authentication. Note that this option only applies to the
      default token generator.
      """
    ],
    token_generator: [
      type: :mfa,
      doc: """
      An MFArgs tuple that will be called before each request
      to fetch an authentication token. It should return `{:ok, String.t()} | {:error, any()}`.
      By default this will invoke `Goth.fetch/1` with the `:goth` option.
      See the "Custom token generator" section below for more information.
      """
    ],
    base_url: [
      type: {:custom, __MODULE__, :type_non_empty_string, [[{:name, :base_url}]]},
      default: @default_base_url,
      doc: """
      The base URL for the Cloud Pub/Sub service.
      This option is mostly useful for testing via the Pub/Sub emulator.
      """
    ],
    finch: [
      type: :atom,
      default: nil,
      doc: """
      The name of the `Finch` pool. If no name is provided, then a default
      pool will be started by the pipeline's supervisor.
      """
    ],
    return_immediately: [
      doc: false,
      deprecated: "Google Pub/Sub will remove this option in the future.",
      type: :boolean
    ],
    # Testing Options
    test_pid: [
      type: :pid,
      doc: false
    ],
    message_server: [
      type: :pid,
      doc: false
    ],
    prepare_to_connect_ref: [
      type: :any,
      doc: false
    ]
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
  Builds an MFArgs tuple for a token generator.
  """
  def make_token_generator(opts) do
    goth = Keyword.get(opts, :goth)

    unless goth do
      require Logger

      Logger.error("""
      the :goth option is required for the default authentication token generator

      If you are upgrading from an earlier version of Goth, refer to the
      upgrade guide for more information:

      https://hexdocs.pm/goth/upgrade_guide.html
      """)
    end

    ensure_goth_loaded()
    {__MODULE__, :generate_goth_token, [goth]}
  end

  defp ensure_goth_loaded() do
    unless Code.ensure_loaded?(Goth) do
      require Logger

      Logger.error("""
      the default authentication token generator uses the Goth library but it's not available

      Add goth to your dependencies:

          defp deps do
            {:goth, "~> 1.3"}
          end

      Or provide your own token generator:

          Broadway.start_link(
            producers: [
              default: [
                module: {BroadwayCloudPubSub.Producer,
                  token_generator: {MyGenerator, :generate, ["foo"]}
                }
              ]
            ]
          )
      """)
    end
  end

  def type_atom_action_or_nack_with_bounded_integer(:ack, [{:name, _}, {:min, _}, {:max, _}]) do
    {:ok, :ack}
  end

  def type_atom_action_or_nack_with_bounded_integer(:noop, [{:name, _}, {:min, _}, {:max, _}]) do
    {:ok, :noop}
  end

  def type_atom_action_or_nack_with_bounded_integer(:nack, [{:name, _}, {:min, _}, {:max, _}]) do
    {:ok, {:nack, 0}}
  end

  def type_atom_action_or_nack_with_bounded_integer(
        {:nack, value},
        [{:name, _}, {:min, min}, {:max, max}]
      )
      when is_integer(value) and value >= min and value <= max do
    {:ok, {:nack, value}}
  end

  def type_atom_action_or_nack_with_bounded_integer(value, [
        {:name, name},
        {:min, min},
        {:max, max}
      ]) do
    {:error,
     "expected :#{name} to be one of :ack, :noop, :nack, or {:nack, integer} where " <>
       "integer is between #{min} and #{max}, got: #{inspect(value)}"}
  end

  def type_non_empty_string("", [{:name, name}]) do
    {:error, "expected :#{name} to be a non-empty string, got: \"\""}
  end

  def type_non_empty_string(value, _)
      when not is_nil(value) and is_binary(value) do
    {:ok, value}
  end

  def type_non_empty_string(value, [{:name, name}]) do
    {:error, "expected :#{name} to be a non-empty string, got: #{inspect(value)}"}
  end

  def type_non_empty_string_or_tagged_tuple(value, [{:name, name}])
      when not (is_binary(value) or is_tuple(value)) or (value == "" or value == {}) do
    {:error, "expected :#{name} to be a non-empty string or tuple, got: #{inspect(value)}"}
  end

  def type_non_empty_string_or_tagged_tuple(value, _) do
    {:ok, value}
  end

  def type_positive_integer_or_infinity(value, _) when is_integer(value) and value > 0 do
    {:ok, value}
  end

  def type_positive_integer_or_infinity(:infinity, _) do
    {:ok, :infinity}
  end

  def type_positive_integer_or_infinity(value, [{:name, name}]) do
    {:error, "expected :#{name} to be a positive integer or :infinity, got: #{inspect(value)}"}
  end

  @doc false
  def generate_goth_token(goth_name) do
    with {:ok, %{token: token}} <- Goth.fetch(goth_name) do
      {:ok, token}
    end
  end
end
