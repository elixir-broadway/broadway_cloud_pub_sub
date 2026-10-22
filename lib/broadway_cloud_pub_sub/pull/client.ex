defmodule BroadwayCloudPubSub.Pull.Client do
  @moduledoc """
  A generic behaviour to implement Pub/Sub Clients for `BroadwayCloudPubSub.Pull.Producer`.

  This module defines callbacks to normalize options and receive messages
  from a Cloud Pub/Sub topic. Modules that implement this behaviour should be passed
  as the `:client` option from `BroadwayCloudPubSub.Pull.Producer`.
  """

  alias Broadway.Message

  @typedoc """
  A list of `Broadway.Message` structs.
  """
  @type messages :: [Message.t()]

  @typedoc """
  The amount of time (in seconds) before Pub/Sub should reschedule a message.
  """
  @type ack_deadline :: 0..600

  @typedoc """
  The `ackId` returned by Pub/Sub to be used when acknowledging a message.
  """
  @type ack_id :: String.t()

  @typedoc """
  A list of `ackId` strings.
  """
  @type ack_ids :: list(ack_id)

  @doc """
  Invoked once by BroadwayCloudPubSub.Producer during `Broadway.start_link/2`.

  The goal of this task is to manipulate the producer options,
  if necessary at all, and introduce any new child specs that will be
  started in Broadway's supervision tree.
  """
  @callback prepare_to_connect(name :: atom, producer_opts :: keyword) ::
              {[:supervisor.child_spec() | {module, any} | module], producer_opts :: keyword}

  @callback init(opts :: any) :: {:ok, normalized_opts :: any} | {:error, message :: binary}

  @doc """
  Dispatches a [`pull`](https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/pull) request.
  """
  @callback receive_messages(demand :: pos_integer, ack_builder :: (ack_id -> term), opts :: any) ::
              messages

  @doc """
  Dispatches a [`modifyAckDeadline`](https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/modifyAckDeadline) request.
  """
  @callback put_deadline(ack_ids, ack_deadline, opts :: any) :: any

  @doc """
  Dispatches an [`acknowledge`](https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/acknowledge) request.
  """
  @callback acknowledge(ack_ids, opts :: any) :: any

  @optional_callbacks acknowledge: 2, prepare_to_connect: 2, put_deadline: 3
end
