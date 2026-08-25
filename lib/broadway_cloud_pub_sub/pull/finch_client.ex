defmodule BroadwayCloudPubSub.Pull.FinchClient do
  @moduledoc """
  The default Pub/Sub pull client, built on `Finch`.

  Implements the `BroadwayCloudPubSub.Pull.Client` behaviour and handles
  [pull](https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/pull)
  requests to the Cloud Pub/Sub REST API.
  """
  alias Broadway.Message
  alias BroadwayCloudPubSub.Pull.MessageBuilder
  alias Finch.Response

  require Logger

  @behaviour BroadwayCloudPubSub.Pull.Client

  @default_retry_codes [408, 500, 502, 503, 504, 522, 524]
  @default_retry_delay_ms 500
  @default_max_retries 10

  @impl BroadwayCloudPubSub.Pull.Client
  def prepare_to_connect(name, producer_opts) do
    case Keyword.fetch(producer_opts, :finch) do
      {:ok, nil} ->
        prepare_finch(name, producer_opts)

      {:ok, _} ->
        {[], producer_opts}

      :error ->
        prepare_finch(name, producer_opts)
    end
  end

  defp prepare_finch(name, producer_opts) do
    finch = Module.concat(name, __MODULE__)

    specs = [
      {Finch, name: finch}
    ]

    producer_opts = Keyword.put(producer_opts, :finch, finch)

    {specs, producer_opts}
  end

  @impl BroadwayCloudPubSub.Pull.Client
  def init(opts) do
    {:ok, Map.new(opts)}
  end

  @impl BroadwayCloudPubSub.Pull.Client
  def receive_messages(demand, ack_builder, config) do
    max_messages = min(demand, config.max_number_of_messages)

    :telemetry.span(
      [:broadway_cloud_pub_sub, :pull_client, :receive_messages],
      %{
        max_messages: max_messages,
        demand: demand,
        name: config.broadway[:name]
      },
      fn ->
        result =
          config
          |> execute(:pull, %{"maxMessages" => max_messages})
          |> handle_response(:receive_messages)
          |> wrap_received_messages(ack_builder)

        {result, %{name: config.broadway[:name], max_messages: max_messages, demand: demand}}
      end
    )
  end

  @impl BroadwayCloudPubSub.Pull.Client
  def put_deadline(ack_ids, ack_deadline_seconds, config) do
    payload = %{
      "ackIds" => ack_ids,
      "ackDeadlineSeconds" => ack_deadline_seconds
    }

    config
    |> execute(:modack, payload)
    |> handle_response(:put_deadline)
  end

  @impl BroadwayCloudPubSub.Pull.Client
  def acknowledge(ack_ids, config) do
    :telemetry.span(
      [:broadway_cloud_pub_sub, :pull_client, :ack],
      %{name: config.topology_name},
      fn ->
        result =
          config
          |> execute(:acknowledge, %{"ackIds" => ack_ids})
          |> handle_response(:acknowledge)

        {result, %{name: config.topology_name}}
      end
    )
  end

  defp handle_response({:ok, response}, :receive_messages) do
    case response do
      %{"receivedMessages" => received_messages} -> received_messages
      _ -> []
    end
  end

  defp handle_response({:ok, _}, _) do
    :ok
  end

  defp handle_response({:error, reason}, :receive_messages) do
    Logger.error("Unable to fetch events from Cloud Pub/Sub - reason: #{reason}")
    []
  end

  defp handle_response({:error, reason}, :acknowledge) do
    Logger.error("Unable to acknowledge messages with Cloud Pub/Sub - reason: #{reason}")
    :ok
  end

  defp handle_response({:error, reason}, :put_deadline) do
    Logger.error("Unable to put new ack deadline with Cloud Pub/Sub - reason: #{reason}")
    :ok
  end

  defp wrap_received_messages(pub_sub_messages, ack_builder) do
    Enum.map(pub_sub_messages, fn pub_sub_msg ->
      pub_sub_msg_to_broadway_msg(pub_sub_msg, ack_builder)
    end)
  end

  defp pub_sub_msg_to_broadway_msg(pub_sub_msg, ack_builder) do
    %{"ackId" => ack_id, "message" => message} = pub_sub_msg

    # 2022-09-21 (MC) The docs falsely claim the following:
    # "If a DeadLetterPolicy is not set on the subscription, this will be 0."
    # In reality, if DeadLetterPolicy is not set, neither is the deliveryAttempt field.
    # https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/pull#receivedmessage
    delivery_attempt = Map.get(pub_sub_msg, "deliveryAttempt")

    {data, metadata} =
      message
      |> decode_message()
      |> Map.pop("data")

    metadata =
      MessageBuilder.build_metadata(%{
        message_id: metadata["messageId"],
        ordering_key: metadata["orderingKey"],
        publish_time: parse_datetime(metadata["publishTime"]),
        delivery_attempt: delivery_attempt,
        attributes: metadata["attributes"]
      })

    %Message{
      data: data,
      metadata: metadata,
      acknowledger: ack_builder.(ack_id)
    }
  end

  defp parse_datetime(nil), do: nil

  defp parse_datetime(str) when is_binary(str) do
    case DateTime.from_iso8601(str) do
      {:ok, dt, _} ->
        dt

      err ->
        raise """
        invalid datetime string: #{inspect(err)}
        """
    end
  end

  defp decode_message(%{"data" => encoded_data} = message) when is_binary(encoded_data) do
    %{message | "data" => Base.decode64!(encoded_data)}
  end

  defp decode_message(%{"data" => nil} = message), do: message
  defp decode_message(%{} = message) when not is_map_key(message, "data"), do: message

  defp headers(config) do
    token = get_token(config)
    [{"authorization", "Bearer #{token}"}, {"content-type", "application/json"}]
  end

  @mod_ack_action "modifyAckDeadline"
  defp url(config, :modack), do: url(config, @mod_ack_action)

  defp url(config, action) do
    sub = URI.encode(config.subscription)
    path = "/v1/" <> sub <> ":" <> to_string(action)
    config.base_url <> path
  end

  defp execute(config, action, payload) do
    url = url(config, action)
    body = Jason.encode!(payload)
    headers = headers(config)
    execute(url, body, headers, config, action, payload, max_retries(config))
  end

  defp execute(url, body, headers, config, action, payload, retries_left) do
    case finch_request(config.finch, url, body, headers, config.receive_timeout) do
      {:ok, %Response{status: 200, body: body}} ->
        {:ok, Jason.decode!(body)}

      {:ok, %Response{} = resp} ->
        maybe_retry(resp, url, body, headers, config, action, payload, retries_left)

      {:error, err} ->
        {:error, format_error(url, err)}
    end
  end

  defp maybe_retry(resp, url, body, headers, config, action, payload, retries_left) do
    if should_retry(resp, config, retries_left) do
      config |> retry_delay() |> Process.sleep()
      execute(url, body, headers, config, action, payload, retries_left - 1)
    else
      {:error, format_error(url, resp)}
    end
  end

  defp should_retry(%Response{status: status}, config, retries_left) do
    retries_left > 0 and status in retry_codes(config)
  end

  defp max_retries(config) do
    config[:max_retries] || @default_max_retries
  end

  defp retry_codes(config) do
    config[:retry_codes] || @default_retry_codes
  end

  defp retry_delay(config) do
    config[:retry_delay_ms] || @default_retry_delay_ms
  end

  defp finch_request(finch, url, body, headers, timeout) do
    :post
    |> Finch.build(url, headers, body)
    |> Finch.request(finch, receive_timeout: timeout)
  end

  defp get_token(%{token_generator: {m, f, a}}) do
    {:ok, token} = apply(m, f, a)
    token
  end

  defp format_error(url, %Response{status: status, body: body}) do
    """
    \nRequest to #{inspect(url)} failed with status #{inspect(status)}, got:
    #{inspect(body)}
    """
  end

  defp format_error(url, err) do
    inspect(%{url: url, error: err})
  end
end
