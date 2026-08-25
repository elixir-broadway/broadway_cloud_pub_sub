defmodule BroadwayCloudPubSub.Pull.MessageBuilder do
  @moduledoc false

  # Shared message-building logic used by both the pull client and the
  # streaming client to ensure consistent Broadway.Message metadata structure.

  alias Broadway.Message

  @doc """
  Builds a `Broadway.Message` metadata map from a normalized fields map.

  Both the pull client (REST/JSON) and the streaming client (gRPC/protobuf)
  normalize their transport-specific message representation into the same
  intermediate shape before calling this function, guaranteeing that all
  producers emit identical metadata keys.

  ## Input fields

  The `fields` map must have the following atom keys:

    * `:message_id` — the Pub/Sub message ID
    * `:ordering_key` — the ordering key (may be `nil` or `""`)
    * `:publish_time` — a `%DateTime{}` or `nil`
    * `:delivery_attempt` — a positive integer or `nil`
    * `:attributes` — a `%{String.t() => String.t()}` map or `nil`

  ## Output

  Returns a metadata map with the following camelCase atom keys, which is
  the established API convention for this library:

      %{
        messageId: message_id,
        orderingKey: ordering_key,
        publishTime: publish_time,
        deliveryAttempt: delivery_attempt,
        attributes: attributes
      }
  """
  @spec build_metadata(%{
          message_id: term(),
          ordering_key: term(),
          publish_time: DateTime.t() | nil,
          delivery_attempt: non_neg_integer() | nil,
          attributes: map() | nil
        }) :: map()
  def build_metadata(%{
        message_id: message_id,
        ordering_key: ordering_key,
        publish_time: publish_time,
        delivery_attempt: delivery_attempt,
        attributes: attributes
      }) do
    %{
      messageId: message_id,
      orderingKey: ordering_key,
      publishTime: publish_time,
      deliveryAttempt: delivery_attempt,
      attributes: attributes
    }
  end

  @doc """
  Builds a `Broadway.Message` from `data`, a metadata fields map, and an
  `acknowledger` tuple.

  The `fields` map is passed to `build_metadata/1` — see that function for
  the expected keys.
  """
  @spec build_message(term(), map(), Broadway.Message.acknowledger()) :: Message.t()
  def build_message(data, fields, acknowledger) do
    %Message{
      data: data,
      metadata: build_metadata(fields),
      acknowledger: acknowledger
    }
  end
end
