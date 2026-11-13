defmodule BroadwayCloudPubSub.Streaming.ErrorClassifierTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Streaming.ErrorClassifier

  # Helper to build a GRPC.RPCError with just status + message
  defp rpc_error(status, message \\ "") do
    %GRPC.RPCError{status: status, message: message}
  end

  describe "classify/1 — retryable gRPC status codes" do
    test "DEADLINE_EXCEEDED (4) is retryable" do
      assert ErrorClassifier.classify(rpc_error(4)) == :retryable
    end

    test "INTERNAL (13) is retryable" do
      assert ErrorClassifier.classify(rpc_error(13)) == :retryable
    end

    test "ABORTED (10) is retryable" do
      assert ErrorClassifier.classify(rpc_error(10)) == :retryable
    end

    test "UNAVAILABLE (14) without shutdown message is retryable" do
      assert ErrorClassifier.classify(rpc_error(14, "service unavailable")) == :retryable
      assert ErrorClassifier.classify(rpc_error(14, "")) == :retryable
      assert ErrorClassifier.classify(rpc_error(14)) == :retryable
    end

    test "UNAVAILABLE (14) with 'Server shutdownNow invoked' is retryable" do
      assert ErrorClassifier.classify(rpc_error(14, "Server shutdownNow invoked")) == :retryable
    end

    test "UNAVAILABLE (14) with message containing shutdown string is retryable" do
      assert ErrorClassifier.classify(rpc_error(14, "prefix Server shutdownNow invoked suffix")) ==
               :retryable
    end

    test "UNKNOWN (2) is retryable" do
      assert ErrorClassifier.classify(rpc_error(2)) == :retryable
    end

    test "RESOURCE_EXHAUSTED (8) is retryable" do
      assert ErrorClassifier.classify(rpc_error(8)) == :retryable
    end

    test "UNAUTHENTICATED (16) is retryable" do
      assert ErrorClassifier.classify(rpc_error(16)) == :retryable
    end
  end

  describe "classify/1 — terminal gRPC status codes" do
    test "NOT_FOUND (5) is terminal" do
      assert ErrorClassifier.classify(rpc_error(5)) == :terminal
    end

    test "PERMISSION_DENIED (7) is terminal" do
      assert ErrorClassifier.classify(rpc_error(7)) == :terminal
    end

    test "INVALID_ARGUMENT (3) is terminal" do
      assert ErrorClassifier.classify(rpc_error(3)) == :terminal
    end

    test "FAILED_PRECONDITION (9) is terminal" do
      assert ErrorClassifier.classify(rpc_error(9)) == :terminal
    end
  end

  describe "classify/1 — CANCELLED is retryable" do
    test "CANCELLED (1) is retryable (server-side stream teardown or client replacement)" do
      assert ErrorClassifier.classify(rpc_error(1)) == :retryable
    end
  end

  describe "classify/1 — non-gRPC errors" do
    test "plain atom is retryable" do
      assert ErrorClassifier.classify(:closed) == :retryable
    end

    test "arbitrary tuple is retryable" do
      assert ErrorClassifier.classify({:error, :econnrefused}) == :retryable
    end

    test "nil is retryable" do
      assert ErrorClassifier.classify(nil) == :retryable
    end
  end
end
