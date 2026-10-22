defmodule BroadwayCloudPubSub.Streaming.RetryTracker do
  @moduledoc false

  # Pure-data module for tracking per-ack-ID retry state.
  #
  # Tracks first-queued timestamps and attempt counts for ack/modack IDs
  # that need retry management (primarily for exactly-once delivery).
  #
  # Used by AckBatcher to enforce:
  #   - Retry deadlines: drop IDs older than `retry_deadline_ms`
  #   - Attempt limits: drop IDs that have been attempted `max_attempts` times
  #
  # All functions are pure — they accept and return data without side effects.
  # AckBatcher handles telemetry emission and state updates.

  @type t :: %__MODULE__{
          first_queued: %{String.t() => integer()},
          attempts: %{String.t() => non_neg_integer()},
          retry_deadline_ms: pos_integer() | nil,
          max_attempts: pos_integer() | nil
        }

  defstruct first_queued: %{}, attempts: %{}, retry_deadline_ms: nil, max_attempts: nil

  @doc """
  Creates a new RetryTracker.

  ## Options

    * `:retry_deadline_ms` — maximum time in milliseconds to keep retrying
      an ack_id before dropping it. `nil` disables deadline-based expiry.
    * `:max_attempts` — maximum number of flush attempts before dropping
      an ack_id. `nil` disables attempt-based limits.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      retry_deadline_ms: Keyword.get(opts, :retry_deadline_ms),
      max_attempts: Keyword.get(opts, :max_attempts)
    }
  end

  @doc """
  Registers ack_ids with a first-queued timestamp.

  Uses `Map.put_new` semantics — re-tracking an already-tracked ID does
  not overwrite its original timestamp or reset its attempt count.
  """
  @spec track(t(), [String.t()], integer()) :: t()
  def track(%__MODULE__{} = tracker, ack_ids, now_ms) do
    new_fq = Enum.reduce(ack_ids, tracker.first_queued, &Map.put_new(&2, &1, now_ms))
    new_att = Enum.reduce(ack_ids, tracker.attempts, &Map.put_new(&2, &1, 0))
    %{tracker | first_queued: new_fq, attempts: new_att}
  end

  @doc """
  Increments the attempt count for each ack_id.

  IDs not yet tracked get an initial count of 1.
  """
  @spec record_attempt(t(), [String.t()]) :: t()
  def record_attempt(%__MODULE__{} = tracker, ack_ids) do
    new_att =
      Enum.reduce(ack_ids, tracker.attempts, fn id, acc ->
        Map.update(acc, id, 1, &(&1 + 1))
      end)

    %{tracker | attempts: new_att}
  end

  @doc """
  Partitions ack_ids into live and expired based on `retry_deadline_ms`.

  Returns `{live_ids, expired_ids, updated_tracker}`. Expired IDs are
  removed from internal tracking state.

  When `retry_deadline_ms` is `nil`, returns all `ack_ids` as live.
  """
  @spec expire_stale(t(), [String.t()], integer()) ::
          {live :: [String.t()], expired :: [String.t()], t()}
  def expire_stale(%__MODULE__{retry_deadline_ms: nil} = tracker, ack_ids, _now_ms) do
    {ack_ids, [], tracker}
  end

  def expire_stale(%__MODULE__{} = tracker, ack_ids, now_ms) do
    {live, expired} =
      Enum.split_with(ack_ids, fn id ->
        case Map.get(tracker.first_queued, id) do
          nil -> true
          ts -> now_ms - ts < tracker.retry_deadline_ms
        end
      end)

    new_fq = Map.drop(tracker.first_queued, expired)
    new_att = Map.drop(tracker.attempts, expired)
    {live, expired, %{tracker | first_queued: new_fq, attempts: new_att}}
  end

  @doc """
  Partitions ack_ids into keep and drop based on `max_attempts`.

  Returns `{keep_ids, drop_ids, updated_tracker}`. Dropped IDs are
  removed from internal tracking state.

  When `max_attempts` is `nil`, returns all `ids` as keep.
  """
  @spec check_attempts(t(), [String.t()]) ::
          {keep :: [String.t()], drop :: [String.t()], t()}
  def check_attempts(%__MODULE__{max_attempts: nil} = tracker, ids) do
    {ids, [], tracker}
  end

  def check_attempts(%__MODULE__{} = tracker, ids) do
    {keep, drop} =
      Enum.split_with(ids, fn id ->
        Map.get(tracker.attempts, id, 0) < tracker.max_attempts
      end)

    new_fq = Map.drop(tracker.first_queued, drop)
    new_att = Map.drop(tracker.attempts, drop)
    {keep, drop, %{tracker | first_queued: new_fq, attempts: new_att}}
  end

  @doc """
  Cleanup sweep: removes all tracking state for IDs NOT in the given set.

  Called after a flush cycle to bound memory by removing IDs that have been
  successfully delivered and are no longer pending.
  """
  @spec retain_only(t(), MapSet.t()) :: t()
  def retain_only(%__MODULE__{} = tracker, id_set) do
    new_fq = Map.filter(tracker.first_queued, fn {id, _} -> MapSet.member?(id_set, id) end)
    new_att = Map.filter(tracker.attempts, fn {id, _} -> MapSet.member?(id_set, id) end)
    %{tracker | first_queued: new_fq, attempts: new_att}
  end

  @doc """
  Updates the retry deadline at runtime.

  Called by AckBatcher when StreamManager detects a change in exactly-once
  delivery status.
  """
  @spec update_retry_deadline(t(), pos_integer() | nil) :: t()
  def update_retry_deadline(%__MODULE__{} = tracker, deadline_ms) do
    %{tracker | retry_deadline_ms: deadline_ms}
  end
end
