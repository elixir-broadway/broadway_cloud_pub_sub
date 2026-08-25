defmodule BroadwayCloudPubSub.Streaming.MessageDispatchTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Streaming.{AckTimeDistribution, MessageDispatch}

  # ============================================================
  # flush_demand/2
  # ============================================================

  describe "flush_demand/2" do
    test "empty buffer, demand > 0 — returns empty to_send, demand unchanged" do
      buffer = :queue.new()
      result = MessageDispatch.flush_demand(buffer, 5)

      assert result.to_send == []
      assert :queue.is_empty(result.remaining_buffer)
      assert result.remaining_demand == 5
    end

    test "buffer with 3 messages, demand 2 — dequeues 2, leaves 1" do
      msgs = for i <- 1..3, do: make_broadway_message("ack-#{i}", "data-#{i}")
      buffer = Enum.reduce(msgs, :queue.new(), &:queue.in(&1, &2))

      result = MessageDispatch.flush_demand(buffer, 2)

      assert length(result.to_send) == 2
      assert :queue.len(result.remaining_buffer) == 1
      assert result.remaining_demand == 0

      # Verify order preservation
      [first, second] = result.to_send
      assert first.data == "data-1"
      assert second.data == "data-2"
    end

    test "buffer with 2 messages, demand 5 — dequeues 2, remaining demand 3" do
      msgs = for i <- 1..2, do: make_broadway_message("ack-#{i}", "data-#{i}")
      buffer = Enum.reduce(msgs, :queue.new(), &:queue.in(&1, &2))

      result = MessageDispatch.flush_demand(buffer, 5)

      assert length(result.to_send) == 2
      assert :queue.is_empty(result.remaining_buffer)
      assert result.remaining_demand == 3
    end

    test "demand 0 — returns empty to_send, buffer unchanged" do
      msgs = [make_broadway_message("ack-1", "data")]
      buffer = Enum.reduce(msgs, :queue.new(), &:queue.in(&1, &2))

      result = MessageDispatch.flush_demand(buffer, 0)

      assert result.to_send == []
      assert :queue.len(result.remaining_buffer) == 1
      assert result.remaining_demand == 0
    end

    test "both empty — returns empty" do
      result = MessageDispatch.flush_demand(:queue.new(), 0)

      assert result.to_send == []
      assert :queue.is_empty(result.remaining_buffer)
      assert result.remaining_demand == 0
    end
  end

  # ============================================================
  # record_and_remove/4
  # ============================================================

  describe "record_and_remove/4" do
    test "records processing times and removes ids from outstanding" do
      dist = AckTimeDistribution.new(60)
      now = 10_000

      outstanding = %{
        "id-1" => %{received_at: 5_000, max_expiry: 1_000_000},
        "id-2" => %{received_at: 3_000, max_expiry: 1_000_000},
        "id-3" => %{received_at: 1_000, max_expiry: 1_000_000}
      }

      {updated_outstanding, updated_dist} =
        MessageDispatch.record_and_remove(outstanding, dist, ["id-1", "id-2"], now)

      # id-1 and id-2 removed; id-3 remains
      assert map_size(updated_outstanding) == 1
      assert Map.has_key?(updated_outstanding, "id-3")

      # Distribution should have 2 samples recorded
      assert AckTimeDistribution.sample_count(updated_dist) == 2
    end

    test "ids not in outstanding are skipped — no dist update" do
      dist = AckTimeDistribution.new(60)
      now = 10_000

      outstanding = %{
        "id-1" => %{received_at: 5_000, max_expiry: 1_000_000}
      }

      {updated_outstanding, updated_dist} =
        MessageDispatch.record_and_remove(outstanding, dist, ["id-1", "nonexistent"], now)

      assert map_size(updated_outstanding) == 0
      # Only 1 sample — "nonexistent" was skipped
      assert AckTimeDistribution.sample_count(updated_dist) == 1
    end

    test "duration calculation: max(1, (now - received_at) / 1000) seconds" do
      dist = AckTimeDistribution.new(60)

      # received_at=0, now=500 → 500ms → 0s → clamped to 1s
      outstanding = %{"id-1" => %{received_at: 0, max_expiry: 1_000_000}}

      {_outstanding, updated_dist} =
        MessageDispatch.record_and_remove(outstanding, dist, ["id-1"], 500)

      # The record should be at duration 1 (clamped), which gets clamped to 10 by ATD
      assert AckTimeDistribution.sample_count(updated_dist) == 1
    end

    test "empty ack_ids — no-op" do
      dist = AckTimeDistribution.new(60)
      outstanding = %{"id-1" => %{received_at: 0, max_expiry: 1_000_000}}

      {updated_outstanding, updated_dist} =
        MessageDispatch.record_and_remove(outstanding, dist, [], 10_000)

      assert updated_outstanding == outstanding
      assert AckTimeDistribution.sample_count(updated_dist) == 0
    end
  end

  # ============================================================
  # add_to_outstanding/4
  # ============================================================

  describe "add_to_outstanding/4" do
    test "adds entries with received_at and computed max_expiry" do
      outstanding = MessageDispatch.add_to_outstanding(%{}, ["id-1", "id-2"], 1_000, 60_000)

      assert outstanding["id-1"] == %{received_at: 1_000, max_expiry: 61_000}
      assert outstanding["id-2"] == %{received_at: 1_000, max_expiry: 61_000}
    end

    test "overwrites existing key (Map.put semantics)" do
      outstanding = %{"id-1" => %{received_at: 500, max_expiry: 60_500}}

      updated = MessageDispatch.add_to_outstanding(outstanding, ["id-1"], 1_000, 60_000)

      assert updated["id-1"] == %{received_at: 1_000, max_expiry: 61_000}
    end

    test "preserves existing entries not in ack_ids" do
      outstanding = %{"existing" => %{received_at: 0, max_expiry: 100}}

      updated = MessageDispatch.add_to_outstanding(outstanding, ["new"], 1_000, 60_000)

      assert Map.has_key?(updated, "existing")
      assert Map.has_key?(updated, "new")
    end

    test "empty ack_ids — returns original map" do
      outstanding = %{"id-1" => %{received_at: 0, max_expiry: 100}}

      updated = MessageDispatch.add_to_outstanding(outstanding, [], 1_000, 60_000)

      assert updated == outstanding
    end
  end

  # ============================================================
  # extract_buffered_ack_ids/1
  # ============================================================

  describe "extract_buffered_ack_ids/1" do
    test "extracts ack_ids from Broadway messages in queue" do
      msgs = for i <- 1..3, do: make_broadway_message("ack-#{i}", "data-#{i}")
      buffer = Enum.reduce(msgs, :queue.new(), &:queue.in(&1, &2))

      ids = MessageDispatch.extract_buffered_ack_ids(buffer)

      assert ids == ["ack-1", "ack-2", "ack-3"]
    end

    test "empty queue — empty list" do
      ids = MessageDispatch.extract_buffered_ack_ids(:queue.new())

      assert ids == []
    end
  end

  # ============================================================
  # partition_succeeded/3
  # ============================================================

  describe "partition_succeeded/3" do
    test "filters out failed ids; preserves order" do
      msgs = for i <- 1..4, do: make_broadway_message("ack-#{i}", "data-#{i}")
      all_ids = ["ack-1", "ack-2", "ack-3", "ack-4"]
      failed_ids = ["ack-2", "ack-4"]

      {ok_msgs, ok_ids} = MessageDispatch.partition_succeeded(msgs, all_ids, failed_ids)

      assert ok_ids == ["ack-1", "ack-3"]
      assert Enum.map(ok_msgs, & &1.data) == ["data-1", "data-3"]
    end

    test "all failed — empty result" do
      msgs = [make_broadway_message("ack-1", "data")]
      {ok_msgs, ok_ids} = MessageDispatch.partition_succeeded(msgs, ["ack-1"], ["ack-1"])

      assert ok_msgs == []
      assert ok_ids == []
    end

    test "none failed — all returned" do
      msgs = [make_broadway_message("ack-1", "data")]
      {ok_msgs, ok_ids} = MessageDispatch.partition_succeeded(msgs, ["ack-1"], [])

      assert length(ok_msgs) == 1
      assert ok_ids == ["ack-1"]
    end
  end

  # ============================================================
  # build_broadway_message/2
  # ============================================================

  describe "build_broadway_message/2" do
    test "constructs correct Broadway.Message with acknowledger tuple" do
      received_msg = %{
        ack_id: "test-ack-id",
        message: %BroadwayCloudPubSub.Google.Pubsub.V1.PubsubMessage{
          message_id: "msg-123",
          data: "hello",
          attributes: %{"key" => "value"},
          ordering_key: "order-1",
          publish_time: %Google.Protobuf.Timestamp{seconds: 1_700_000_000, nanos: 500_000_000}
        },
        delivery_attempt: 2
      }

      ack_ref = {:my_pipeline, 0}
      msg = MessageDispatch.build_broadway_message(received_msg, ack_ref)

      assert %Broadway.Message{} = msg
      assert msg.data == "hello"
      assert msg.metadata.messageId == "msg-123"
      assert msg.metadata.orderingKey == "order-1"
      assert msg.metadata.deliveryAttempt == 2
      assert msg.metadata.attributes == %{"key" => "value"}

      {mod, ref, data} = msg.acknowledger
      assert mod == BroadwayCloudPubSub.Streaming.Acknowledger
      assert ref == ack_ref
      assert data.ack_id == "test-ack-id"
    end

    test "handles nil publish_time" do
      received_msg = %{
        ack_id: "ack-1",
        message: %BroadwayCloudPubSub.Google.Pubsub.V1.PubsubMessage{
          message_id: "msg-1",
          data: "test",
          attributes: %{},
          ordering_key: "",
          publish_time: nil
        },
        delivery_attempt: 1
      }

      msg = MessageDispatch.build_broadway_message(received_msg, {:test, 0})

      assert msg.metadata.publishTime == nil
    end

    test "maps empty attributes correctly" do
      received_msg = %{
        ack_id: "ack-1",
        message: %BroadwayCloudPubSub.Google.Pubsub.V1.PubsubMessage{
          message_id: "msg-1",
          data: "test",
          attributes: [],
          ordering_key: "",
          publish_time: nil
        },
        delivery_attempt: 1
      }

      msg = MessageDispatch.build_broadway_message(received_msg, {:test, 0})

      assert msg.metadata.attributes == %{}
    end
  end

  # ============================================================
  # Helpers
  # ============================================================

  # Build a minimal Broadway.Message for testing buffer operations.
  defp make_broadway_message(ack_id, data) do
    %Broadway.Message{
      data: data,
      metadata: %{},
      acknowledger: {BroadwayCloudPubSub.Streaming.Acknowledger, {:test, 0}, %{ack_id: ack_id}}
    }
  end
end
