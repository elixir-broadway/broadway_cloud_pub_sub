defmodule BroadwayCloudPubSub.Streaming.AckTimeDistribution do
  @moduledoc false

  # Fixed-bucket histogram for tracking message processing durations.
  #
  # One bucket per second in the range [0, 600]. Record is O(1) (tuple element
  # increment). Percentile is O(601) linear scan = effectively O(1). Counters
  # are monotonically growing — data is never evicted.
  #
  # This matches the algorithm used by all five official Google Cloud Pub/Sub
  # client libraries:
  #   - Go:      distribution/distribution.go (type D struct { buckets []uint64 })
  #   - Python:  histogram.py (601-bucket array)
  #   - Java:    Distribution class with fixed buckets
  #   - Node.js: histogram.ts with bucket array
  #   - Ruby:    windowed approach with bucket concept
  #
  # Before enough samples are collected (< @min_samples), the distribution
  # returns the configured `default_deadline` so behaviour is identical to the
  # old fixed-deadline strategy during cold start.
  #
  # All duration values are clamped to the valid Pub/Sub ack deadline range:
  # 10–600 seconds (matching the server-enforced limits).

  @min_deadline_seconds 10
  @max_deadline_seconds 600
  # 601 buckets: indices 0..600
  @num_buckets @max_deadline_seconds + 1
  @min_samples 10

  @typedoc "An AckTimeDistribution struct."
  @opaque t :: %__MODULE__{
            buckets: tuple(),
            total: non_neg_integer(),
            default_deadline: pos_integer()
          }

  defstruct buckets: nil, total: 0, default_deadline: 60

  @doc """
  Creates a new distribution with the given default deadline (seconds).

  The `default_deadline` is returned by `percentile/2` until at least
  #{@min_samples} samples have been recorded (cold-start fallback).

  The deadline is clamped to the valid Pub/Sub ack deadline range
  (#{@min_deadline_seconds}–#{@max_deadline_seconds} seconds).
  """
  @spec new(pos_integer()) :: t()
  def new(default_deadline) when is_integer(default_deadline) and default_deadline > 0 do
    clamped = clamp(default_deadline)

    %__MODULE__{
      buckets: :erlang.make_tuple(@num_buckets, 0),
      default_deadline: clamped
    }
  end

  @doc """
  Records a new processing duration (in seconds). O(1).

  The value is clamped to the valid Pub/Sub ack deadline range
  (#{@min_deadline_seconds}–#{@max_deadline_seconds} seconds).
  Unlike the previous circular-buffer implementation, data is never evicted —
  counters grow monotonically.
  """
  @spec record(t(), integer()) :: t()
  def record(%__MODULE__{buckets: buckets, total: total} = dist, duration_seconds) do
    idx = clamp(duration_seconds)
    new_buckets = put_elem(buckets, idx, elem(buckets, idx) + 1)
    %{dist | buckets: new_buckets, total: total + 1}
  end

  @doc """
  Returns the p-th percentile of recorded processing times (in seconds). O(601).

  `p` should be in the range `[0.0, 1.0]`. For example, `0.99` for the 99th
  percentile.

  Returns `default_deadline` if fewer than #{@min_samples} samples have been
  recorded (cold-start fallback).

  The result is always in the valid Pub/Sub ack deadline range
  (#{@min_deadline_seconds}–#{@max_deadline_seconds} seconds) because all
  recorded values are clamped on `record/2`.
  """
  @spec percentile(t(), float()) :: pos_integer()
  def percentile(%__MODULE__{total: total, default_deadline: default}, _p)
      when total < @min_samples do
    default
  end

  def percentile(%__MODULE__{buckets: buckets, total: total}, p)
      when is_number(p) and p >= 0.0 and p <= 1.0 do
    target = max(1, ceil(p * total))
    find_percentile_bucket(buckets, target, @min_deadline_seconds, 0)
  end

  @doc """
  Returns the total number of samples recorded (monotonically increasing).
  """
  @spec sample_count(t()) :: non_neg_integer()
  def sample_count(%__MODULE__{total: total}), do: total

  # Linear scan over the 601 buckets to find the bucket where the cumulative
  # count first reaches or exceeds `target`. Returns that bucket's index, which
  # is also the clamped duration in seconds.
  defp find_percentile_bucket(_buckets, _target, idx, _cumulative)
       when idx >= @num_buckets do
    @max_deadline_seconds
  end

  defp find_percentile_bucket(buckets, target, idx, cumulative) do
    new_cumulative = cumulative + elem(buckets, idx)

    if new_cumulative >= target do
      # idx is already in [@min_deadline_seconds, @max_deadline_seconds]
      # because we only ever write to clamped indices in record/2.
      idx
    else
      find_percentile_bucket(buckets, target, idx + 1, new_cumulative)
    end
  end

  # Clamp a duration value to the valid Pub/Sub ack deadline range.
  defp clamp(value) do
    value
    |> max(@min_deadline_seconds)
    |> min(@max_deadline_seconds)
  end
end
