defmodule BroadwayCloudPubSub.Pull.Acknowledger do
  @moduledoc false
  alias Broadway.Acknowledger
  alias BroadwayCloudPubSub.Pull.{Client, Options}

  @behaviour Acknowledger

  @typedoc """
  Acknowledgement data for a `Broadway.Message`.
  """
  @type ack_data :: %{
          :ack_id => Client.ack_id(),
          optional(:on_failure) => ack_option,
          optional(:on_success) => ack_option
        }

  @typedoc """
  An acknowledgement action.
  """
  @type ack_option :: :ack | :noop | :nack | {:nack, Client.ack_deadline()}

  @type ack_ref :: reference

  # The maximum number of ackIds to be sent in acknowledge/modifyAckDeadline
  # requests. There is an API limit of 524288 bytes (512KiB) per acknowledge/modifyAckDeadline
  # request. ackIds have a maximum size of 184 bytes, so 524288/184 ~= 2849.
  # Accounting for some overhead, a maximum of 2500 ackIds per request should be safe.
  # See https://github.com/googleapis/nodejs-pubsub/pull/65/files#diff-3d29c4447546c72118ed5d5cbf38ab8bR34-R42
  @max_ack_ids_per_request 2_500

  @doc """
  Returns an `acknowledger` to be put on a `Broadway.Message`.
  """
  @spec builder(ack_ref) :: (Client.ack_id() -> {__MODULE__, ack_ref, ack_data})
  def builder(ack_ref) do
    &{__MODULE__, ack_ref, %{ack_id: &1}}
  end

  @impl Acknowledger
  def ack(ack_ref, successful, failed) do
    config = :persistent_term.get(ack_ref)

    success_actions = group_actions_ack_ids(successful, :on_success, config)
    failure_actions = group_actions_ack_ids(failed, :on_failure, config)

    success_actions
    |> Map.merge(failure_actions, fn _, a, b -> a ++ b end)
    |> ack_messages(config)

    :ok
  end

  @impl Acknowledger
  def configure(_ack_ref, ack_data, options) do
    opts = NimbleOptions.validate!(options, Options.acknowledger_definition())
    ack_data = Map.merge(ack_data, Map.new(opts))

    {:ok, ack_data}
  end

  defp group_actions_ack_ids(messages, key, config) do
    Enum.group_by(messages, &group_acknowledger(&1, key, config), &extract_ack_id/1)
  end

  defp group_acknowledger(%{acknowledger: {_, _, ack_data}}, key, config) do
    Map.get_lazy(ack_data, key, fn -> config_action(key, config) end)
  end

  defp config_action(:on_success, %{on_success: action}), do: action
  defp config_action(:on_failure, %{on_failure: action}), do: action

  defp extract_ack_id(message) do
    {_, _, %{ack_id: ack_id}} = message.acknowledger
    ack_id
  end

  defp ack_messages(actions_and_ids, config) do
    Enum.each(actions_and_ids, fn {action, ack_ids} ->
      ack_ids
      |> Enum.chunk_every(@max_ack_ids_per_request)
      |> Enum.each(&apply_ack_func(action, &1, config))
    end)
  end

  defp apply_ack_func(:noop, _ack_ids, _config), do: :ok

  defp apply_ack_func(:ack, ack_ids, config) do
    %{client: client} = config
    client.acknowledge(ack_ids, config)
  end

  defp apply_ack_func(:nack, ack_ids, config) do
    apply_ack_func({:nack, 0}, ack_ids, config)
  end

  defp apply_ack_func({:nack, deadline}, ack_ids, config) do
    %{client: client} = config
    client.put_deadline(ack_ids, deadline, config)
  end
end
