defmodule BroadwayCloudPubSub.Streaming.LeaseManager do
  @moduledoc false

  # Pure-function module for lease extension and adaptive deadline computation.
  #
  # All functions accept explicit inputs and return plain data — no state structs,
  # no side effects (no AckBatcher calls, no Process.send_after, no telemetry).
  # StreamManager handles all side effects based on the returned results.

  alias BroadwayCloudPubSub.Streaming.AckTimeDistribution

  # Subtracted from the adaptive deadline when computing the lease extension interval.
  @grace_period_seconds 5

  # Minimum ack deadline enforced by the server for exactly-once subscriptions.
  @min_deadline_exactly_once_seconds 60

  # Default staleness threshold for pending receipt modacks (60 seconds).
  @default_receipt_modack_stale_ms 60_000

  # --- Deadline computation ---

  @doc """
  Returns the effective ack deadline in seconds, based on the adaptive p99
  percentile from recorded processing times. For exactly-once subscriptions,
  enforces the server's minimum of 60 seconds.
  """
  @spec effective_deadline(AckTimeDistribution.t(), boolean()) :: pos_integer()
  def effective_deadline(ack_time_dist, exactly_once_enabled) do
    adaptive = AckTimeDistribution.percentile(ack_time_dist, 0.99)

    if exactly_once_enabled,
      do: max(adaptive, @min_deadline_exactly_once_seconds),
      else: adaptive
  end

  # --- Lease extension ---

  @typedoc """
  Result of a lease extension cycle.

  * `valid` — outstanding messages whose `max_expiry` has not passed.
  * `expired_count` — number of messages whose `max_expiry` has passed.
  * `modack_ids` — ack_ids of valid messages that need a lease extension.
  * `modack_deadline` — the adaptive deadline to use for the modack.
  * `next_timer_ms` — milliseconds until the next extension tick (with jitter).
  """
  @type extend_result :: %{
          valid: %{String.t() => map()},
          expired_count: non_neg_integer(),
          modack_ids: [String.t()],
          modack_deadline: pos_integer(),
          next_timer_ms: pos_integer()
        }

  @doc """
  Runs a lease extension computation: partitions outstanding messages into valid
  and expired, computes the modack deadline and next timer interval.

  Returns an `extend_result` map. The caller is responsible for:
  - Sending modack requests for `modack_ids` at `modack_deadline`
  - Scheduling the next `:extend_leases` timer at `next_timer_ms`
  - Emitting telemetry
  - Updating state with `valid` as the new outstanding map
  """
  @spec extend_leases(
          outstanding :: %{String.t() => map()},
          ack_time_dist :: AckTimeDistribution.t(),
          exactly_once_enabled :: boolean(),
          now_ms :: integer()
        ) :: extend_result()
  def extend_leases(outstanding, ack_time_dist, exactly_once_enabled, now_ms) do
    deadline = effective_deadline(ack_time_dist, exactly_once_enabled)

    {valid, expired} =
      Map.split_with(outstanding, fn {_id, info} -> info.max_expiry > now_ms end)

    modack_ids = if map_size(valid) > 0, do: Map.keys(valid), else: []

    %{
      valid: valid,
      expired_count: map_size(expired),
      modack_ids: modack_ids,
      modack_deadline: deadline,
      next_timer_ms: compute_next_timer_ms(deadline)
    }
  end

  # --- Stale pending modack sweep ---

  @typedoc """
  Result of sweeping stale pending receipt modacks.

  * `fresh` — entries that are still within the staleness threshold.
  * `stale_ack_ids` — ack_ids from entries that exceeded the threshold.
  """
  @type sweep_result :: %{
          fresh: %{reference() => map()},
          stale_ack_ids: [String.t()]
        }

  @doc """
  Partitions pending receipt modacks into fresh and stale.

  Entries whose `received_at` is older than `stale_threshold_ms` from `now_ms`
  are considered stale. Returns stale ack_ids for nacking and the remaining
  fresh entries.

  The default `stale_threshold_ms` is #{@default_receipt_modack_stale_ms}ms (60 seconds).
  """
  @spec sweep_stale_pending_modacks(
          pending_receipt_modacks :: %{reference() => map()},
          now_ms :: integer(),
          stale_threshold_ms :: pos_integer()
        ) :: sweep_result()
  def sweep_stale_pending_modacks(
        pending_receipt_modacks,
        now_ms,
        stale_threshold_ms \\ @default_receipt_modack_stale_ms
      ) do
    cutoff = now_ms - stale_threshold_ms

    {stale, fresh} =
      Map.split_with(pending_receipt_modacks, fn {_ref, %{received_at: t}} ->
        t < cutoff
      end)

    stale_ack_ids =
      if map_size(stale) > 0 do
        stale |> Map.values() |> Enum.flat_map(& &1.ack_ids)
      else
        []
      end

    %{fresh: fresh, stale_ack_ids: stale_ack_ids}
  end

  # --- Timer computation ---

  @doc """
  Computes the initial lease extension timer interval in milliseconds
  from the configured stream ack deadline.

  The interval is `(deadline - grace_period) * 1000` with jitter in [0.8, 0.9),
  minimum 1000ms.
  """
  @spec initial_timer_ms(pos_integer()) :: pos_integer()
  def initial_timer_ms(stream_ack_deadline_seconds) do
    compute_next_timer_ms(stream_ack_deadline_seconds)
  end

  # --- Private ---

  # Computes the next timer interval with jitter.
  # base_interval = max(1s, (deadline - grace_period) * 1000)
  # jitter in [0.8, 0.9) to spread out concurrent StreamManagers.
  defp compute_next_timer_ms(deadline_seconds) do
    base_interval_ms = max(1_000, (deadline_seconds - @grace_period_seconds) * 1_000)
    jitter_factor = 0.8 + :rand.uniform() * 0.1
    round(base_interval_ms * jitter_factor)
  end
end
