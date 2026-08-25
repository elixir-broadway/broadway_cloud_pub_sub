defmodule BroadwayCloudPubSub.Google.Pubsub.V1.PubsubMessage.AttributesEntry do
  use Protobuf,
    full_name: "google.pubsub.v1.PubsubMessage.AttributesEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule BroadwayCloudPubSub.Google.Pubsub.V1.PubsubMessage do
  @moduledoc """
  A message that is published by publishers and consumed by subscribers. The
  message must contain either a non-empty data field or at least one attribute.
  Note that client libraries represent this object differently
  depending on the language. See the corresponding [client library
  documentation](https://cloud.google.com/pubsub/docs/reference/libraries) for
  more information. See [quotas and limits]
  (https://cloud.google.com/pubsub/quotas) for more information about message
  limits.
  """

  use Protobuf,
    full_name: "google.pubsub.v1.PubsubMessage",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:data, 1, type: :bytes, deprecated: false)

  field(:attributes, 2,
    repeated: true,
    type: BroadwayCloudPubSub.Google.Pubsub.V1.PubsubMessage.AttributesEntry,
    map: true,
    deprecated: false
  )

  field(:message_id, 3, type: :string, json_name: "messageId")
  field(:publish_time, 4, type: Google.Protobuf.Timestamp, json_name: "publishTime")
  field(:ordering_key, 5, type: :string, json_name: "orderingKey", deprecated: false)
end

defmodule BroadwayCloudPubSub.Google.Pubsub.V1.ReceivedMessage do
  @moduledoc """
  A message and its corresponding acknowledgment ID.
  """

  use Protobuf,
    full_name: "google.pubsub.v1.ReceivedMessage",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:ack_id, 1, type: :string, json_name: "ackId", deprecated: false)
  field(:message, 2, type: BroadwayCloudPubSub.Google.Pubsub.V1.PubsubMessage, deprecated: false)
  field(:delivery_attempt, 3, type: :int32, json_name: "deliveryAttempt", deprecated: false)
end

defmodule BroadwayCloudPubSub.Google.Pubsub.V1.ModifyAckDeadlineRequest do
  @moduledoc """
  Request for the ModifyAckDeadline method.
  """

  use Protobuf,
    full_name: "google.pubsub.v1.ModifyAckDeadlineRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:subscription, 1, type: :string, deprecated: false)
  field(:ack_ids, 4, repeated: true, type: :string, json_name: "ackIds", deprecated: false)

  field(:ack_deadline_seconds, 3,
    type: :int32,
    json_name: "ackDeadlineSeconds",
    deprecated: false
  )
end

defmodule BroadwayCloudPubSub.Google.Pubsub.V1.AcknowledgeRequest do
  @moduledoc """
  Request for the Acknowledge method.
  """

  use Protobuf,
    full_name: "google.pubsub.v1.AcknowledgeRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:subscription, 1, type: :string, deprecated: false)
  field(:ack_ids, 2, repeated: true, type: :string, json_name: "ackIds", deprecated: false)
end

defmodule BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullRequest do
  @moduledoc """
  Request for the `StreamingPull` streaming RPC method. This request is used to
  establish the initial stream as well as to stream acknowledgments and ack
  deadline modifications from the client to the server.
  """

  use Protobuf,
    full_name: "google.pubsub.v1.StreamingPullRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:subscription, 1, type: :string, deprecated: false)
  field(:ack_ids, 2, repeated: true, type: :string, json_name: "ackIds", deprecated: false)

  field(:modify_deadline_seconds, 3,
    repeated: true,
    type: :int32,
    json_name: "modifyDeadlineSeconds",
    deprecated: false
  )

  field(:modify_deadline_ack_ids, 4,
    repeated: true,
    type: :string,
    json_name: "modifyDeadlineAckIds",
    deprecated: false
  )

  field(:stream_ack_deadline_seconds, 5,
    type: :int32,
    json_name: "streamAckDeadlineSeconds",
    deprecated: false
  )

  field(:client_id, 6, type: :string, json_name: "clientId", deprecated: false)

  field(:max_outstanding_messages, 7,
    type: :int64,
    json_name: "maxOutstandingMessages",
    deprecated: false
  )

  field(:max_outstanding_bytes, 8,
    type: :int64,
    json_name: "maxOutstandingBytes",
    deprecated: false
  )

  field(:protocol_version, 10, type: :int64, json_name: "protocolVersion", deprecated: false)
end

defmodule BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.AcknowledgeConfirmation do
  @moduledoc """
  Acknowledgment IDs sent in one or more previous requests to acknowledge a
  previously received message.
  """

  use Protobuf,
    full_name: "google.pubsub.v1.StreamingPullResponse.AcknowledgeConfirmation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:ack_ids, 1, repeated: true, type: :string, json_name: "ackIds", deprecated: false)

  field(:invalid_ack_ids, 2,
    repeated: true,
    type: :string,
    json_name: "invalidAckIds",
    deprecated: false
  )

  field(:unordered_ack_ids, 3,
    repeated: true,
    type: :string,
    json_name: "unorderedAckIds",
    deprecated: false
  )

  field(:temporary_failed_ack_ids, 4,
    repeated: true,
    type: :string,
    json_name: "temporaryFailedAckIds",
    deprecated: false
  )
end

defmodule BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.ModifyAckDeadlineConfirmation do
  @moduledoc """
  Acknowledgment IDs sent in one or more previous requests to modify the
  deadline for a specific message.
  """

  use Protobuf,
    full_name: "google.pubsub.v1.StreamingPullResponse.ModifyAckDeadlineConfirmation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:ack_ids, 1, repeated: true, type: :string, json_name: "ackIds", deprecated: false)

  field(:invalid_ack_ids, 2,
    repeated: true,
    type: :string,
    json_name: "invalidAckIds",
    deprecated: false
  )

  field(:temporary_failed_ack_ids, 3,
    repeated: true,
    type: :string,
    json_name: "temporaryFailedAckIds",
    deprecated: false
  )
end

defmodule BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties do
  @moduledoc """
  Subscription properties sent as part of the response.
  """

  use Protobuf,
    full_name: "google.pubsub.v1.StreamingPullResponse.SubscriptionProperties",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:exactly_once_delivery_enabled, 1,
    type: :bool,
    json_name: "exactlyOnceDeliveryEnabled",
    deprecated: false
  )

  field(:message_ordering_enabled, 2,
    type: :bool,
    json_name: "messageOrderingEnabled",
    deprecated: false
  )
end

defmodule BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse do
  @moduledoc """
  Response for the `StreamingPull` method. This response is used to stream
  messages from the server to the client.
  """

  use Protobuf,
    full_name: "google.pubsub.v1.StreamingPullResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:received_messages, 1,
    repeated: true,
    type: BroadwayCloudPubSub.Google.Pubsub.V1.ReceivedMessage,
    json_name: "receivedMessages",
    deprecated: false
  )

  field(:acknowledge_confirmation, 5,
    type: BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.AcknowledgeConfirmation,
    json_name: "acknowledgeConfirmation",
    deprecated: false
  )

  field(:modify_ack_deadline_confirmation, 3,
    type:
      BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.ModifyAckDeadlineConfirmation,
    json_name: "modifyAckDeadlineConfirmation",
    deprecated: false
  )

  field(:subscription_properties, 4,
    type: BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties,
    json_name: "subscriptionProperties",
    deprecated: false
  )
end

defmodule BroadwayCloudPubSub.Google.Pubsub.V1.Subscriber.Service do
  @moduledoc """
  The service that an application uses to manipulate subscriptions and to
  consume messages from a subscription via the `Pull` method or by
  establishing a bi-directional stream using the `StreamingPull` method.
  """

  use GRPC.Service, name: "google.pubsub.v1.Subscriber", protoc_gen_elixir_version: "0.16.0"

  rpc(
    :ModifyAckDeadline,
    BroadwayCloudPubSub.Google.Pubsub.V1.ModifyAckDeadlineRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :Acknowledge,
    BroadwayCloudPubSub.Google.Pubsub.V1.AcknowledgeRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :StreamingPull,
    stream(BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullRequest),
    stream(BroadwayCloudPubSub.Google.Pubsub.V1.StreamingPullResponse)
  )
end

defmodule BroadwayCloudPubSub.Google.Pubsub.V1.Subscriber.Stub do
  use GRPC.Stub, service: BroadwayCloudPubSub.Google.Pubsub.V1.Subscriber.Service
end
