defmodule BroadwayCloudPubSub.Streaming.AckResult do
  @moduledoc false

  # Represents the outcome of an ack or nack operation in exactly-once delivery mode.
  #
  # In exactly-once mode the server guarantees at-most-once delivery only when the
  # ack is durably committed. To surface ack failures to callers, each ack/nack
  # operation resolves to an AckResult describing whether the operation succeeded
  # or failed and why.
  #
  # This matches the AckResult type pattern used in official client libraries:
  #
  #   type AckResult struct {
  #     ready  chan struct{}
  #     status AcknowledgeStatus
  #     err    error
  #   }
  #
  # ## Per-ack-ID error parsing (exactly-once)
  #
  # Google's Pub/Sub API returns per-ack-ID errors in the gRPC response metadata
  # when an Acknowledge or ModifyAckDeadline RPC partially fails. The errors are
  # encoded in a `google.rpc.Status` detail of type `google.rpc.ErrorInfo`:
  #
  #   - `reason` field: either "TRANSIENT_<details>" (retry) or "PERMANENT_<details>"
  #   - `metadata` map: keys are ack_ids, values are the per-ack-id error reason
  #
  # Transient errors should be retried; permanent errors indicate an invalid ack ID
  # and should be resolved immediately with the appropriate status.
  #
  # ## Status values
  #
  # | Status                | Meaning                                          |
  # |-----------------------|--------------------------------------------------|
  # | :success              | Ack/nack committed by the server                 |
  # | :permission_denied    | Service account lacks Subscriber role            |
  # | :failed_precondition  | Subscription not in exactly-once mode            |
  # | :invalid_ack_id       | Ack ID is invalid or has already expired         |
  # | :other                | Unrecognised permanent failure                   |

  @type status ::
          :success
          | :permission_denied
          | :failed_precondition
          | :invalid_ack_id
          | :other

  @type t :: %__MODULE__{
          ack_id: String.t(),
          status: status(),
          error: term() | nil
        }

  defstruct [:ack_id, :status, :error]

  @doc """
  Returns a successful AckResult for the given ack_id.
  """
  @spec success(String.t()) :: t()
  def success(ack_id), do: %__MODULE__{ack_id: ack_id, status: :success, error: nil}

  @doc """
  Returns a failed AckResult for the given ack_id with the given status and error.
  """
  @spec failure(String.t(), status(), term()) :: t()
  def failure(ack_id, status, error),
    do: %__MODULE__{ack_id: ack_id, status: status, error: error}

  @doc """
  Parses per-ack-ID error information from a `GRPC.RPCError`'s details list.

  Google's Pub/Sub API encodes per-ack-ID errors in `google.rpc.ErrorInfo` details
  when an Acknowledge or ModifyAckDeadline RPC partially fails. The `metadata` map
  in `ErrorInfo` maps ack_id => error_reason, where the reason has either a
  "TRANSIENT_" or "PERMANENT_" prefix.

  Returns a map of `ack_id => :transient | {:permanent, reason_string}`.

  If no per-ack-ID errors are present (e.g. the whole RPC failed), returns an
  empty map.
  """
  @spec parse_error_details([Google.Protobuf.Any.t()] | nil) ::
          %{String.t() => :transient | {:permanent, String.t()}}
  def parse_error_details(nil), do: %{}
  def parse_error_details([]), do: %{}

  def parse_error_details(details) when is_list(details) do
    Enum.reduce(details, %{}, fn any_proto, acc ->
      case decode_error_info(any_proto) do
        {:ok, %Google.Rpc.ErrorInfo{metadata: metadata}} when map_size(metadata) > 0 ->
          Enum.reduce(metadata, acc, fn {ack_id, reason}, inner_acc ->
            Map.put(inner_acc, ack_id, classify_reason(reason))
          end)

        _ ->
          acc
      end
    end)
  end

  @doc """
  Returns true if the given reason string indicates a transient (retryable) error.

  Transient errors from Google Pub/Sub have a "TRANSIENT_" prefix in the reason
  field, e.g. "TRANSIENT_FAILURE_INVALID_ACK_ID".
  """
  @spec transient_reason?(String.t()) :: boolean()
  def transient_reason?(reason) when is_binary(reason),
    do: String.starts_with?(reason, "TRANSIENT_")

  # --- Private ---

  defp decode_error_info(%Google.Protobuf.Any{type_url: type_url, value: value})
       when is_binary(value) do
    if type_url == "type.googleapis.com/google.rpc.ErrorInfo" do
      try do
        {:ok, Google.Rpc.ErrorInfo.decode(value)}
      rescue
        _ -> :error
      end
    else
      :error
    end
  end

  defp decode_error_info(_), do: :error

  defp classify_reason(reason) when is_binary(reason) do
    if String.starts_with?(reason, "TRANSIENT_") do
      :transient
    else
      {:permanent, reason}
    end
  end
end
