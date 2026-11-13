defmodule BroadwayCloudPubSub.Streaming.RetryTrackerTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Streaming.RetryTracker

  # ============================================================
  # new/1
  # ============================================================

  describe "new/1" do
    test "creates empty tracker with default nil config" do
      tracker = RetryTracker.new()

      assert tracker.first_queued == %{}
      assert tracker.attempts == %{}
      assert tracker.retry_deadline_ms == nil
      assert tracker.max_attempts == nil
    end

    test "creates tracker with configured retry_deadline_ms" do
      tracker = RetryTracker.new(retry_deadline_ms: 600_000)

      assert tracker.retry_deadline_ms == 600_000
    end

    test "creates tracker with configured max_attempts" do
      tracker = RetryTracker.new(max_attempts: 3)

      assert tracker.max_attempts == 3
    end

    test "creates tracker with both options" do
      tracker = RetryTracker.new(retry_deadline_ms: 60_000, max_attempts: 3)

      assert tracker.retry_deadline_ms == 60_000
      assert tracker.max_attempts == 3
    end
  end

  # ============================================================
  # track/3
  # ============================================================

  describe "track/3" do
    test "registers ack_ids with first-queued timestamp" do
      tracker = RetryTracker.new()

      tracker = RetryTracker.track(tracker, ["id-1", "id-2"], 1000)

      assert tracker.first_queued == %{"id-1" => 1000, "id-2" => 1000}
      assert tracker.attempts == %{"id-1" => 0, "id-2" => 0}
    end

    test "put_new semantics — does not overwrite existing timestamp" do
      tracker = RetryTracker.new()

      tracker = RetryTracker.track(tracker, ["id-1"], 1000)
      tracker = RetryTracker.track(tracker, ["id-1", "id-2"], 2000)

      # id-1 keeps its original timestamp of 1000
      assert tracker.first_queued["id-1"] == 1000
      # id-2 gets the new timestamp of 2000
      assert tracker.first_queued["id-2"] == 2000
    end

    test "put_new semantics — does not reset attempt count" do
      tracker = RetryTracker.new()

      tracker = RetryTracker.track(tracker, ["id-1"], 1000)
      tracker = RetryTracker.record_attempt(tracker, ["id-1"])
      tracker = RetryTracker.track(tracker, ["id-1"], 2000)

      # Attempt count preserved at 1, not reset to 0
      assert tracker.attempts["id-1"] == 1
    end

    test "handles empty list" do
      tracker = RetryTracker.new()

      tracker = RetryTracker.track(tracker, [], 1000)

      assert tracker.first_queued == %{}
      assert tracker.attempts == %{}
    end
  end

  # ============================================================
  # record_attempt/2
  # ============================================================

  describe "record_attempt/2" do
    test "increments attempt count for tracked ids" do
      tracker = RetryTracker.new()
      tracker = RetryTracker.track(tracker, ["id-1"], 1000)

      tracker = RetryTracker.record_attempt(tracker, ["id-1"])

      assert tracker.attempts["id-1"] == 1
    end

    test "increments multiple times" do
      tracker = RetryTracker.new()
      tracker = RetryTracker.track(tracker, ["id-1"], 1000)

      tracker = RetryTracker.record_attempt(tracker, ["id-1"])
      tracker = RetryTracker.record_attempt(tracker, ["id-1"])
      tracker = RetryTracker.record_attempt(tracker, ["id-1"])

      assert tracker.attempts["id-1"] == 3
    end

    test "initializes count to 1 for untracked ids" do
      tracker = RetryTracker.new()

      tracker = RetryTracker.record_attempt(tracker, ["new-id"])

      assert tracker.attempts["new-id"] == 1
    end

    test "handles multiple ids at once" do
      tracker = RetryTracker.new()
      tracker = RetryTracker.track(tracker, ["id-1", "id-2"], 1000)

      tracker = RetryTracker.record_attempt(tracker, ["id-1", "id-2"])

      assert tracker.attempts["id-1"] == 1
      assert tracker.attempts["id-2"] == 1
    end

    test "handles empty list" do
      tracker = RetryTracker.new()
      tracker = RetryTracker.track(tracker, ["id-1"], 1000)

      tracker = RetryTracker.record_attempt(tracker, [])

      assert tracker.attempts["id-1"] == 0
    end
  end

  # ============================================================
  # expire_stale/3
  # ============================================================

  describe "expire_stale/3" do
    test "no-op when retry_deadline_ms is nil — all ids returned as live" do
      tracker = RetryTracker.new(retry_deadline_ms: nil)
      tracker = RetryTracker.track(tracker, ["id-1", "id-2"], 1000)

      {live, expired, updated} = RetryTracker.expire_stale(tracker, ["id-1", "id-2"], 999_999)

      assert live == ["id-1", "id-2"]
      assert expired == []
      # Internal state unchanged
      assert updated.first_queued == tracker.first_queued
      assert updated.attempts == tracker.attempts
    end

    test "partitions ids into live and expired" do
      tracker = RetryTracker.new(retry_deadline_ms: 5_000)

      tracker = RetryTracker.track(tracker, ["fresh"], 9_000)
      tracker = RetryTracker.track(tracker, ["stale"], 1_000)

      now = 10_000
      {live, expired, updated} = RetryTracker.expire_stale(tracker, ["fresh", "stale"], now)

      assert live == ["fresh"]
      assert expired == ["stale"]
      # Stale id removed from internal state
      refute Map.has_key?(updated.first_queued, "stale")
      refute Map.has_key?(updated.attempts, "stale")
      # Fresh id retained
      assert Map.has_key?(updated.first_queued, "fresh")
    end

    test "id with no first_queued entry is treated as live" do
      tracker = RetryTracker.new(retry_deadline_ms: 5_000)
      # Don't track "unknown" — it has no first_queued entry

      {live, expired, _tracker} = RetryTracker.expire_stale(tracker, ["unknown"], 10_000)

      assert live == ["unknown"]
      assert expired == []
    end

    test "all expired" do
      tracker = RetryTracker.new(retry_deadline_ms: 1_000)
      tracker = RetryTracker.track(tracker, ["id-1", "id-2"], 1_000)

      {live, expired, updated} = RetryTracker.expire_stale(tracker, ["id-1", "id-2"], 100_000)

      assert live == []
      assert Enum.sort(expired) == ["id-1", "id-2"]
      assert updated.first_queued == %{}
      assert updated.attempts == %{}
    end

    test "boundary: exactly at deadline is still live" do
      tracker = RetryTracker.new(retry_deadline_ms: 5_000)
      tracker = RetryTracker.track(tracker, ["id-1"], 1_000)

      # now - ts == 4999 < 5000 → live
      {live, expired, _} = RetryTracker.expire_stale(tracker, ["id-1"], 5_999)
      assert live == ["id-1"]
      assert expired == []

      # now - ts == 5000 → NOT < 5000 → expired
      {live, expired, _} = RetryTracker.expire_stale(tracker, ["id-1"], 6_000)
      assert live == []
      assert expired == ["id-1"]
    end

    test "handles empty id list" do
      tracker = RetryTracker.new(retry_deadline_ms: 5_000)
      tracker = RetryTracker.track(tracker, ["id-1"], 1_000)

      {live, expired, updated} = RetryTracker.expire_stale(tracker, [], 10_000)

      assert live == []
      assert expired == []
      # Internal state unchanged — "id-1" still tracked even though not in input list
      assert Map.has_key?(updated.first_queued, "id-1")
    end
  end

  # ============================================================
  # check_attempts/2
  # ============================================================

  describe "check_attempts/2" do
    test "no-op when max_attempts is nil — all ids returned as keep" do
      tracker = RetryTracker.new(max_attempts: nil)
      tracker = RetryTracker.track(tracker, ["id-1"], 1000)
      tracker = RetryTracker.record_attempt(tracker, ["id-1"])
      tracker = RetryTracker.record_attempt(tracker, ["id-1"])
      tracker = RetryTracker.record_attempt(tracker, ["id-1"])

      {keep, drop, _updated} = RetryTracker.check_attempts(tracker, ["id-1"])

      assert keep == ["id-1"]
      assert drop == []
    end

    test "keeps ids under the limit, drops ids at or over" do
      tracker = RetryTracker.new(max_attempts: 3)
      tracker = RetryTracker.track(tracker, ["ok", "exhausted"], 1000)

      # "ok" has 1 attempt, "exhausted" has 3
      tracker = RetryTracker.record_attempt(tracker, ["ok"])
      tracker = RetryTracker.record_attempt(tracker, ["exhausted"])
      tracker = RetryTracker.record_attempt(tracker, ["exhausted"])
      tracker = RetryTracker.record_attempt(tracker, ["exhausted"])

      {keep, drop, updated} = RetryTracker.check_attempts(tracker, ["ok", "exhausted"])

      assert keep == ["ok"]
      assert drop == ["exhausted"]
      # Dropped id removed from internal state
      refute Map.has_key?(updated.first_queued, "exhausted")
      refute Map.has_key?(updated.attempts, "exhausted")
      # Kept id retained
      assert Map.has_key?(updated.first_queued, "ok")
    end

    test "id with no attempts entry (0 by default) is kept" do
      tracker = RetryTracker.new(max_attempts: 3)

      {keep, drop, _} = RetryTracker.check_attempts(tracker, ["new-id"])

      assert keep == ["new-id"]
      assert drop == []
    end

    test "handles empty list" do
      tracker = RetryTracker.new(max_attempts: 3)

      {keep, drop, _} = RetryTracker.check_attempts(tracker, [])

      assert keep == []
      assert drop == []
    end
  end

  # ============================================================
  # retain_only/2
  # ============================================================

  describe "retain_only/2" do
    test "removes ids not in the given set" do
      tracker = RetryTracker.new()
      tracker = RetryTracker.track(tracker, ["keep-1", "keep-2", "remove"], 1000)

      tracker = RetryTracker.retain_only(tracker, MapSet.new(["keep-1", "keep-2"]))

      assert Map.keys(tracker.first_queued) |> Enum.sort() == ["keep-1", "keep-2"]
      assert Map.keys(tracker.attempts) |> Enum.sort() == ["keep-1", "keep-2"]
    end

    test "empty set removes all" do
      tracker = RetryTracker.new()
      tracker = RetryTracker.track(tracker, ["id-1", "id-2"], 1000)

      tracker = RetryTracker.retain_only(tracker, MapSet.new())

      assert tracker.first_queued == %{}
      assert tracker.attempts == %{}
    end

    test "retaining all is a no-op" do
      tracker = RetryTracker.new()
      tracker = RetryTracker.track(tracker, ["id-1", "id-2"], 1000)

      updated = RetryTracker.retain_only(tracker, MapSet.new(["id-1", "id-2"]))

      assert updated.first_queued == tracker.first_queued
      assert updated.attempts == tracker.attempts
    end
  end

  # ============================================================
  # update_retry_deadline/2
  # ============================================================

  describe "update_retry_deadline/2" do
    test "updates the retry_deadline_ms field" do
      tracker = RetryTracker.new(retry_deadline_ms: 60_000)

      tracker = RetryTracker.update_retry_deadline(tracker, 600_000)

      assert tracker.retry_deadline_ms == 600_000
    end

    test "can set to nil" do
      tracker = RetryTracker.new(retry_deadline_ms: 60_000)

      tracker = RetryTracker.update_retry_deadline(tracker, nil)

      assert tracker.retry_deadline_ms == nil
    end
  end

  # ============================================================
  # Integration: full pipeline
  # ============================================================

  describe "integration: track → record_attempt → check_attempts → retain_only" do
    test "full retry lifecycle for modack-style tracking" do
      tracker = RetryTracker.new(retry_deadline_ms: 60_000, max_attempts: 3)

      # Track 3 ids
      tracker = RetryTracker.track(tracker, ["a", "b", "c"], 1000)

      # First flush attempt
      tracker = RetryTracker.record_attempt(tracker, ["a", "b", "c"])
      # "a" succeeds (removed from pending), "b" and "c" fail
      tracker = RetryTracker.retain_only(tracker, MapSet.new(["b", "c"]))

      assert Map.keys(tracker.first_queued) |> Enum.sort() == ["b", "c"]

      # Second flush attempt
      tracker = RetryTracker.record_attempt(tracker, ["b", "c"])
      # "b" succeeds, "c" fails
      tracker = RetryTracker.retain_only(tracker, MapSet.new(["c"]))

      # Third flush attempt — "c" hits max_attempts (3)
      tracker = RetryTracker.record_attempt(tracker, ["c"])
      {keep, drop, tracker} = RetryTracker.check_attempts(tracker, ["c"])

      assert keep == []
      assert drop == ["c"]
      assert tracker.first_queued == %{}
      assert tracker.attempts == %{}
    end
  end
end
