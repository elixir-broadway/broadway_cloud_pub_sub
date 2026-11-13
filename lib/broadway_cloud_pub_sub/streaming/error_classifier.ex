defmodule BroadwayCloudPubSub.Streaming.ErrorClassifier do
  @moduledoc false

  # Classifies gRPC errors into :retryable or :terminal categories.
  #
  # ## Retryable errors (reconnect the stream)
  #
  # These are transient conditions where the subscription is still valid and
  # reconnecting will eventually succeed:
  #
  #   - DEADLINE_EXCEEDED (4) — server-side idle timeout (the primary issue)
  #   - INTERNAL (13)         — transient server error
  #   - ABORTED (10)          — concurrent modification, retry
  #   - UNAVAILABLE (14)      — server temporarily unavailable or being drained.
  #                             Note: "Server shutdownNow invoked" is a routine
  #                             Google-side backend drain — the LB will route to
  #                             another backend immediately.
  #   - UNAUTHENTICATED (16)  — expired OAuth2 token. Reconnect fetches a fresh
  #                             token. Treating this as terminal would permanently
  #                             stop the producer on routine token rotation.
  #   - UNKNOWN (2)           — includes HTTP/2 GOAWAY frames on connection drain
  #   - RESOURCE_EXHAUSTED (8)— quota temporarily exceeded, retry with backoff
  #   - Non-gRPC errors       — connection resets, EOF, transport errors
  #
  # ## Terminal errors (stop the GenServer, let Broadway restart it)
  #
  # These indicate a permanent misconfiguration or missing resource where
  # reconnecting without a config change would loop forever:
  #
  #   - NOT_FOUND (5)         — subscription does not exist
  #   - PERMISSION_DENIED (7) — service account lacks Subscriber role
  #   - INVALID_ARGUMENT (3)  — bad subscription name or flow-control params
  #   - FAILED_PRECONDITION (9)— subscription in wrong state (e.g. detached or
  #                             seeking). Reconnecting without a config change
  #                             will not resolve the condition.
  #
  # CANCELLED (1) is classified as **retryable** because in practice it occurs
  # in two benign scenarios:
  #
  #   1. Server-side stream teardown — the server cancels the stream during load
  #      balancing or backend rotation.  Reconnecting immediately succeeds.
  #   2. Client-side stream replacement — some libraries (Node.js) cancel a
  #      stream to open a fresh one.  The cancellation is expected, not terminal.
  #
  # During graceful shutdown the StreamReader is killed before any CANCELLED
  # error can be forwarded, so a retryable classification does not cause an
  # unwanted reconnect during drain.

  @terminal_status_codes MapSet.new([
                           # NOT_FOUND — subscription does not exist
                           5,
                           # PERMISSION_DENIED — no IAM access
                           7,
                           # INVALID_ARGUMENT — bad config / subscription name
                           3,
                           # FAILED_PRECONDITION — subscription in wrong state
                           9
                         ])

  @type classification :: :retryable | :terminal

  @doc """
  Returns `:retryable` or `:terminal` for the given error.

  Any error not listed as terminal is classified as retryable, following the
  principle that it is safer to retry unknown errors than to permanently
  stop processing messages.
  """
  @spec classify(term()) :: classification()
  def classify(%GRPC.RPCError{status: status}) do
    if MapSet.member?(@terminal_status_codes, status) do
      :terminal
    else
      :retryable
    end
  end

  # Non-gRPC errors (transport failures, connection resets, etc.) are retryable
  def classify(_other), do: :retryable
end
