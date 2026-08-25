defmodule BroadwayCloudPubSub.Streaming.MessageDispatch do
  @moduledoc false

  # Pure-function module for message buffer management, demand-based flushing,
  # Broadway message construction, and outstanding ack_id tracking.
  #
  # All functions accept explicit inputs and return plain data — no state structs,
  # no side effects (no send/2, no telemetry). StreamManager handles all side
  # effects based on the returned results.

  alias BroadwayCloudPubSub.Pull.MessageBuilder
  alias BroadwayCloudPubSub.Streaming.{Acknowledger, AckTimeDistribution}

  # --- Buffer and demand ---

  @typedoc """
  Result of flushing demand from the message buffer.

  * `to_send` — messages to send to the producer (in order).
  * `remaining_buffer` — the buffer after dequeuing.
  * `remaining_demand` — unfulfilled demand after flushing.
  """
  @type flush_result :: %{
          to_send: [Broadway.Message.t()],
          remaining_buffer: :queue.queue(),
          remaining_demand: non_neg_integer()
        }

  @doc """
  Dequeues up to `pending_demand` messages from the buffer.

  Returns a `flush_result` map. The caller is responsible for sending
  `to_send` to the producer and updating state with the remaining
  buffer and demand.
  """
  @spec flush_demand(:queue.queue(), non_neg_integer()) :: flush_result()
  def flush_demand(buffer, 0) do
    %{to_send: [], remaining_buffer: buffer, remaining_demand: 0}
  end

  def flush_demand(buffer, pending_demand) when pending_demand > 0 do
    if :queue.is_empty(buffer) do
      %{to_send: [], remaining_buffer: buffer, remaining_demand: pending_demand}
    else
      {remaining, demand_left, batch_reversed} =
        flush_demand_loop(buffer, pending_demand, [])

      %{
        to_send: Enum.reverse(batch_reversed),
        remaining_buffer: remaining,
        remaining_demand: demand_left
      }
    end
  end

  defp flush_demand_loop(queue, 0, acc), do: {queue, 0, acc}

  defp flush_demand_loop(queue, n, acc) do
    case :queue.out(queue) do
      {{:value, msg}, rest} -> flush_demand_loop(rest, n - 1, [msg | acc])
      {:empty, _} -> {queue, n, acc}
    end
  end

  # --- Outstanding tracking ---

  @doc """
  Adds ack_ids to the outstanding map with their received_at and max_expiry.
  """
  @spec add_to_outstanding(
          outstanding :: map(),
          ack_ids :: [String.t()],
          received_at :: integer(),
          max_extension_ms :: pos_integer()
        ) :: map()
  def add_to_outstanding(outstanding, ack_ids, received_at, max_extension_ms) do
    Enum.reduce(ack_ids, outstanding, fn ack_id, acc ->
      Map.put(acc, ack_id, %{received_at: received_at, max_expiry: received_at + max_extension_ms})
    end)
  end

  @doc """
  Records processing times for ack_ids in the adaptive p99 distribution, then
  removes them from outstanding.

  Returns `{updated_outstanding, updated_ack_time_dist}`.
  """
  @spec record_and_remove(
          outstanding :: map(),
          ack_time_dist :: AckTimeDistribution.t(),
          ack_ids :: [String.t()],
          now_ms :: integer()
        ) :: {map(), AckTimeDistribution.t()}
  def record_and_remove(outstanding, ack_time_dist, ack_ids, now_ms) do
    updated_dist =
      Enum.reduce(ack_ids, ack_time_dist, fn ack_id, dist ->
        case Map.get(outstanding, ack_id) do
          %{received_at: received_at} ->
            duration_s = max(1, div(now_ms - received_at, 1_000))
            AckTimeDistribution.record(dist, duration_s)

          nil ->
            dist
        end
      end)

    updated_outstanding = Enum.reduce(ack_ids, outstanding, &Map.delete(&2, &1))
    {updated_outstanding, updated_dist}
  end

  @doc """
  Extracts ack_ids from buffered Broadway messages.
  """
  @spec extract_buffered_ack_ids(:queue.queue()) :: [String.t()]
  def extract_buffered_ack_ids(message_buffer) do
    message_buffer
    |> :queue.to_list()
    |> Enum.map(&Acknowledger.ack_id_from/1)
  end

  @doc """
  Splits broadway_messages into {succeeded_msgs, succeeded_ids} by removing
  messages whose ack_id is in failed_ids.
  """
  @spec partition_succeeded([Broadway.Message.t()], [String.t()], [String.t()]) ::
          {[Broadway.Message.t()], [String.t()]}
  def partition_succeeded(broadway_messages, all_ack_ids, failed_ids) do
    failed_set = MapSet.new(failed_ids)

    {ok_msgs_reversed, ok_ids_reversed} =
      Enum.zip(broadway_messages, all_ack_ids)
      |> Enum.reduce({[], []}, fn {msg, id}, {msgs_acc, ids_acc} ->
        if MapSet.member?(failed_set, id) do
          {msgs_acc, ids_acc}
        else
          {[msg | msgs_acc], [id | ids_acc]}
        end
      end)

    {Enum.reverse(ok_msgs_reversed), Enum.reverse(ok_ids_reversed)}
  end

  # --- Message construction ---

  @doc """
  Builds a `Broadway.Message` from a decoded Pub/Sub ReceivedMessage and
  an ack_ref.
  """
  @spec build_broadway_message(map(), term()) :: Broadway.Message.t()
  def build_broadway_message(
        %{ack_id: ack_id, message: pubsub_msg, delivery_attempt: delivery_attempt},
        ack_ref
      ) do
    acknowledger = BroadwayCloudPubSub.Streaming.Acknowledger.builder(ack_ref).(ack_id)

    %Broadway.Message{
      data: pubsub_msg.data,
      metadata: build_metadata(pubsub_msg, delivery_attempt),
      acknowledger: acknowledger
    }
  end

  defp build_metadata(msg, delivery_attempt) do
    MessageBuilder.build_metadata(%{
      message_id: msg.message_id,
      ordering_key: msg.ordering_key,
      publish_time: to_datetime(msg.publish_time),
      delivery_attempt: delivery_attempt,
      attributes: Map.new(msg.attributes || [])
    })
  end

  defp to_datetime(nil), do: nil

  defp to_datetime(%{seconds: seconds, nanos: nanos}) do
    DateTime.from_unix!(seconds * 1_000_000_000 + nanos, :nanosecond)
  rescue
    _ -> nil
  end
end
