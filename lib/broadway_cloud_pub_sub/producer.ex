defmodule BroadwayCloudPubSub.Producer do
  @moduledoc """
  A GenStage producer that continuously receives messages from a Google Cloud Pub/Sub
  topic and acknowledges them after being successfully processed.

  By default this producer uses `BroadwayCloudPubSub.PullClient` to talk to Cloud
  Pub/Sub, but you can provide your client by implementing the `BroadwayCloudPubSub.Client`
  behaviour.

  For a quick getting started on using Broadway with Cloud Pub/Sub, please see
  the [Google Cloud Pub/Sub Guide](https://hexdocs.pm/broadway/google-cloud-pubsub.html).

  ## Options

  Aside from `:receive_interval` and `:client` which are generic and apply to all
  producers (regardless of the client implementation), all other options are specific to
  `BroadwayCloudPubSub.PullClient`, which is the default client.

  #{NimbleOptions.docs(BroadwayCloudPubSub.Options.definition())}

  ### Custom token generator

  A custom token generator can be given as a MFArgs tuple.

  For example, define a `MyApp.fetch_token/0` function:

      defmodule MyApp do

        @doc "Fetches a Google auth token"
        def fetch_token do
          with {:ok, token} <- Goth.fetch(MyApp.Goth)
            {:ok, token.token}
          end
        end
      end

  and configure the producer to use it:

      token_generator: {MyApp, :fetch_token, []}

  ## Acknowledgements

  You can use the `:on_success` and `:on_failure` options to control how
  messages are acknowledged with the Pub/Sub system.

  By default successful messages are acknowledged and failed messages are ignored.
  You can set `:on_success` and `:on_failure` when starting this producer,
  or change them for each message through `Broadway.Message.configure_ack/2`.

  The following values are supported by both `:on_success` and `:on_failure`:

  * `:ack` - Acknowledge the message. Pub/Sub can remove the message from
     the subscription.

  * `:noop` - Do nothing. No requests will be made to Pub/Sub, and the
     message will be rescheduled according to the subscription-level
     `ackDeadlineSeconds`.

  * `:nack` - Make a request to Pub/Sub to set `ackDeadlineSeconds` to `0`,
     which may cause the message to be immediately redelivered to another
     connected consumer. Note that this does not modify the subscription-level
     `ackDeadlineSeconds` used for subsequent messages.

  * `{:nack, integer}` - Modifies the `ackDeadlineSeconds` for a particular
     message. Note that this does not modify the subscription-level
     `ackDeadlineSeconds` used for subsequent messages.

  ### Batching

  Even if you are not interested in working with Broadway batches via the
  `handle_batch/3` callback, we recommend all Broadway pipelines with Pub/Sub
  producers to define a default batcher with `batch_size` set to 10, so
  messages can be acknowledged in batches, which improves the performance
  and reduces the cost of integrating with Google Cloud Pub/Sub. In addition,
  you should ensure that `batch_timeout` is set to a value less than
  the acknowledgement deadline on the subscription. Otherwise you could
  potentially have messages that remain in the subscription and are never
  acknowledged successfully.

  ### Example

      Broadway.start_link(MyBroadway,
        name: MyBroadway,
        producer: [
          module: {BroadwayCloudPubSub.Producer,
            goth: MyApp.Goth,
            subscription: "projects/my-project/subscriptions/my_subscription"
          }
        ],
        processors: [
          default: []
        ],
        batchers: [
          default: [
            batch_size: 10,
            batch_timeout: 2_000
          ]
        ]
      )

  The above configuration will set up a producer that continuously receives
  messages from `"projects/my-project/subscriptions/my_subscription"` and sends
  them downstream.

  ## Telemetry

  This producer emits a few [Telemetry](https://github.com/beam-telemetry/telemetry)
  events which are listed below.

    * `[:broadway_cloud_pub_sub, :pull_client, :receive_messages, :start | :stop | :exception]` spans -
      these events are emitted in "span style" when executing pull requests to GCP PubSub.
      See `:telemetry.span/3`.

      All these events have the measurements described in `:telemetry.span/3`. The events
      contain the following metadata:

      * `:max_messages` - the number of messages requested after applying the `max_messages`
      config option to the existing demand
      * `:demand` - the total demand accumulated into the producer
      * `:name` - the name of the Broadway topology

    * `[:broadway_cloud_pub_sub, :pull_client, :ack, :start | :stop | :exception]` span - these events
      are emitted in "span style" when acking messages on GCP PubSub. See `:telemetry.span/3`.

      All these events have the measurements described in `:telemetry.span/3`. The events
      contain the following metadata:

      * `:name` - the name of the Broadway topology
  """

  use GenStage
  alias Broadway.Producer
  alias BroadwayCloudPubSub.{Acknowledger, Options}

  @behaviour Producer

  @impl GenStage
  def init(opts) do
    receive_interval = opts[:receive_interval]
    client = opts[:client]
    on_draining = opts[:on_draining]

    {:ok, config} = client.init(opts)
    ack_ref = opts[:broadway][:name]

    {:producer,
     %{
       demand: 0,
       draining: false,
       on_draining: on_draining,
       receive_timer: nil,
       receive_interval: receive_interval,
       client: {client, config},
       ack_ref: ack_ref,
       worker_task: nil
     }}
  end

  @impl Producer
  def prepare_for_start(_module, broadway_opts) do
    {producer_module, client_opts} = broadway_opts[:producer][:module]

    opts = NimbleOptions.validate!(client_opts, Options.definition())

    ack_ref = broadway_opts[:name]

    {client, opts} =
      case opts[:client] do
        {client, client_opts} -> {client, Keyword.merge(opts, client_opts)}
        client -> {client, opts}
      end

    opts = Keyword.put(opts, :client, client)

    opts =
      Keyword.put_new_lazy(opts, :token_generator, fn ->
        Options.make_token_generator(opts)
      end)

    {specs, opts} = prepare_to_connect(broadway_opts[:name], client, opts)

    ack_opts = Keyword.put(opts, :topology_name, broadway_opts[:name])

    :persistent_term.put(ack_ref, Map.new(ack_opts))

    broadway_opts_with_defaults =
      put_in(broadway_opts, [:producer, :module], {producer_module, opts})

    {specs, broadway_opts_with_defaults}
  end

  defp prepare_to_connect(module, client, producer_opts) do
    if Code.ensure_loaded?(client) and function_exported?(client, :prepare_to_connect, 2) do
      client.prepare_to_connect(module, producer_opts)
    else
      {[], producer_opts}
    end
  end

  @impl true
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    handle_receive_messages(%{state | demand: demand + incoming_demand})
  end

  @impl true
  def handle_info(:receive_messages, state) do
    handle_receive_messages(%{state | receive_timer: nil})
  end

  def handle_info(
        {ref, messages},
        %{draining: true, on_draining: on_draining, worker_task: %{ref: ref}} = state
      ) do
    # Drain messages, not acknowledging them and setting the ack to draining_deadline
    messages
    |> Enum.map(&Broadway.Message.configure_ack(&1, on_success: on_draining))
    |> Broadway.Message.ack_immediately()

    {:noreply, [], %{state | worker_task: nil}}
  end

  def handle_info({ref, messages}, %{demand: demand, worker_task: %{ref: ref}} = state) do
    new_demand = demand - length(messages)

    receive_timer =
      case {messages, new_demand} do
        {[], _} ->
          schedule_receive_messages(state.receive_interval)

        {_, 0} ->
          nil

        _ ->
          schedule_receive_messages(0)
      end

    {:noreply, messages,
     %{state | demand: new_demand, receive_timer: receive_timer, worker_task: nil}}
  end

  @impl true
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  @impl Producer
  def prepare_for_draining(state) do
    {:noreply, [], %{state | draining: true}}
  end

  defp handle_receive_messages(%{draining: true} = state) do
    {:noreply, [], state}
  end

  defp handle_receive_messages(%{receive_timer: nil, demand: demand, worker_task: nil} = state)
       when demand > 0 do
    task = receive_messages_from_pubsub(state, demand)

    {:noreply, [], %{state | worker_task: task}}
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  defp receive_messages_from_pubsub(state, total_demand) do
    %{client: {client, opts}, ack_ref: ack_ref} = state

    Task.async(fn ->
      client.receive_messages(total_demand, Acknowledger.builder(ack_ref), opts)
    end)
  end

  defp schedule_receive_messages(interval) do
    Process.send_after(self(), :receive_messages, interval)
  end
end
