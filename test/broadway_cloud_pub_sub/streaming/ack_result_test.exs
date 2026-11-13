defmodule BroadwayCloudPubSub.Streaming.AckResultTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Streaming.AckResult

  # ============================================================
  # Constructors
  # ============================================================

  describe "success/1" do
    test "returns an AckResult with status :success and nil error" do
      result = AckResult.success("ack-123")
      assert %AckResult{ack_id: "ack-123", status: :success, error: nil} = result
    end
  end

  describe "failure/3" do
    test "returns an AckResult with the given status and error" do
      result = AckResult.failure("ack-456", :invalid_ack_id, "expired")
      assert %AckResult{ack_id: "ack-456", status: :invalid_ack_id, error: "expired"} = result
    end

    test "supports all documented status atoms" do
      for status <- [:permission_denied, :failed_precondition, :invalid_ack_id, :other] do
        result = AckResult.failure("ack-x", status, nil)
        assert result.status == status
      end
    end
  end

  # ============================================================
  # transient_reason?/1
  # ============================================================

  describe "transient_reason?/1" do
    test "returns true for TRANSIENT_ prefix" do
      assert AckResult.transient_reason?("TRANSIENT_FAILURE_INVALID_ACK_ID") == true
    end

    test "returns true for any TRANSIENT_ variant" do
      assert AckResult.transient_reason?("TRANSIENT_SOMETHING_ELSE") == true
    end

    test "returns false for PERMANENT_ prefix" do
      refute AckResult.transient_reason?("PERMANENT_FAILURE_INVALID_ACK_ID")
    end

    test "returns false for empty string" do
      refute AckResult.transient_reason?("")
    end

    test "returns false for unrecognised reason" do
      refute AckResult.transient_reason?("UNKNOWN_REASON")
    end
  end

  # ============================================================
  # parse_error_details/1
  # ============================================================

  describe "parse_error_details/1 — nil / empty" do
    test "returns empty map for nil" do
      assert AckResult.parse_error_details(nil) == %{}
    end

    test "returns empty map for empty list" do
      assert AckResult.parse_error_details([]) == %{}
    end
  end

  describe "parse_error_details/1 — unrecognised Any type_url" do
    test "ignores Any entries with a non-ErrorInfo type_url" do
      other_any = %Google.Protobuf.Any{
        type_url: "type.googleapis.com/google.rpc.Status",
        value: <<>>
      }

      assert AckResult.parse_error_details([other_any]) == %{}
    end
  end

  describe "parse_error_details/1 — transient per-ack-id errors" do
    test "returns :transient for ack_ids with TRANSIENT_ reason" do
      error_info = %Google.Rpc.ErrorInfo{
        reason: "TRANSIENT_FAILURE_INVALID_ACK_ID",
        domain: "pubsub.googleapis.com",
        metadata: %{
          "ack-1" => "TRANSIENT_FAILURE_INVALID_ACK_ID",
          "ack-2" => "TRANSIENT_FAILURE_INVALID_ACK_ID"
        }
      }

      any_proto = build_error_info_any(error_info)
      result = AckResult.parse_error_details([any_proto])

      assert result == %{
               "ack-1" => :transient,
               "ack-2" => :transient
             }
    end

    test "returns :transient for all TRANSIENT_ variants" do
      error_info = %Google.Rpc.ErrorInfo{
        reason: "TRANSIENT_SOMETHING",
        domain: "pubsub.googleapis.com",
        metadata: %{"ack-x" => "TRANSIENT_SOMETHING"}
      }

      any_proto = build_error_info_any(error_info)
      result = AckResult.parse_error_details([any_proto])

      assert result["ack-x"] == :transient
    end
  end

  describe "parse_error_details/1 — permanent per-ack-id errors" do
    test "returns {:permanent, reason} for ack_ids with PERMANENT_ reason" do
      error_info = %Google.Rpc.ErrorInfo{
        reason: "PERMANENT_FAILURE_INVALID_ACK_ID",
        domain: "pubsub.googleapis.com",
        metadata: %{"ack-3" => "PERMANENT_FAILURE_INVALID_ACK_ID"}
      }

      any_proto = build_error_info_any(error_info)
      result = AckResult.parse_error_details([any_proto])

      assert result == %{"ack-3" => {:permanent, "PERMANENT_FAILURE_INVALID_ACK_ID"}}
    end

    test "returns {:permanent, reason} for unrecognised (non-TRANSIENT_) reason" do
      error_info = %Google.Rpc.ErrorInfo{
        reason: "SOMETHING_COMPLETELY_DIFFERENT",
        domain: "pubsub.googleapis.com",
        metadata: %{"ack-4" => "SOMETHING_COMPLETELY_DIFFERENT"}
      }

      any_proto = build_error_info_any(error_info)
      result = AckResult.parse_error_details([any_proto])

      assert result["ack-4"] == {:permanent, "SOMETHING_COMPLETELY_DIFFERENT"}
    end
  end

  describe "parse_error_details/1 — mixed transient and permanent" do
    test "correctly classifies each ack_id independently" do
      error_info = %Google.Rpc.ErrorInfo{
        reason: "MIXED",
        domain: "pubsub.googleapis.com",
        metadata: %{
          "ack-transient" => "TRANSIENT_FAILURE_INVALID_ACK_ID",
          "ack-permanent" => "PERMANENT_FAILURE_INVALID_ACK_ID"
        }
      }

      any_proto = build_error_info_any(error_info)
      result = AckResult.parse_error_details([any_proto])

      assert result["ack-transient"] == :transient
      assert result["ack-permanent"] == {:permanent, "PERMANENT_FAILURE_INVALID_ACK_ID"}
    end
  end

  describe "parse_error_details/1 — multiple Any details" do
    test "merges results from multiple ErrorInfo entries" do
      error_info_1 = %Google.Rpc.ErrorInfo{
        reason: "TRANSIENT_FAILURE_INVALID_ACK_ID",
        domain: "pubsub.googleapis.com",
        metadata: %{"ack-a" => "TRANSIENT_FAILURE_INVALID_ACK_ID"}
      }

      error_info_2 = %Google.Rpc.ErrorInfo{
        reason: "PERMANENT_FAILURE_INVALID_ACK_ID",
        domain: "pubsub.googleapis.com",
        metadata: %{"ack-b" => "PERMANENT_FAILURE_INVALID_ACK_ID"}
      }

      details = [
        build_error_info_any(error_info_1),
        build_error_info_any(error_info_2)
      ]

      result = AckResult.parse_error_details(details)

      assert result["ack-a"] == :transient
      assert result["ack-b"] == {:permanent, "PERMANENT_FAILURE_INVALID_ACK_ID"}
    end

    test "skips non-ErrorInfo entries between valid ones" do
      error_info = %Google.Rpc.ErrorInfo{
        reason: "TRANSIENT_FAILURE",
        domain: "pubsub.googleapis.com",
        metadata: %{"ack-z" => "TRANSIENT_FAILURE"}
      }

      other_any = %Google.Protobuf.Any{
        type_url: "type.googleapis.com/google.rpc.Status",
        value: <<1, 2, 3>>
      }

      result = AckResult.parse_error_details([other_any, build_error_info_any(error_info)])
      assert result == %{"ack-z" => :transient}
    end
  end

  describe "parse_error_details/1 — empty metadata" do
    test "returns empty map when ErrorInfo metadata is empty" do
      error_info = %Google.Rpc.ErrorInfo{
        reason: "SOME_ERROR",
        domain: "pubsub.googleapis.com",
        metadata: %{}
      }

      any_proto = build_error_info_any(error_info)
      result = AckResult.parse_error_details([any_proto])

      assert result == %{}
    end
  end

  # ============================================================
  # Helpers
  # ============================================================

  # Encodes a Google.Rpc.ErrorInfo struct into a Google.Protobuf.Any
  # with the correct type_url, matching what the Pub/Sub server sends.
  defp build_error_info_any(%Google.Rpc.ErrorInfo{} = error_info) do
    %Google.Protobuf.Any{
      type_url: "type.googleapis.com/google.rpc.ErrorInfo",
      value: Google.Rpc.ErrorInfo.encode(error_info)
    }
  end
end
