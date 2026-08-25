defmodule BroadwayCloudPubSub.Streaming.LeaseManagerTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Streaming.{AckTimeDistribution, LeaseManager}

  # ============================================================
  # effective_deadline/2
  # ============================================================

  describe "effective_deadline/2" do
    test "returns default deadline when distribution has fewer than 10 samples" do
      dist = AckTimeDistribution.new(60)

      assert LeaseManager.effective_deadline(dist, false) == 60
    end

    test "returns p99 from distribution with enough samples" do
      dist = AckTimeDistribution.new(60)

      # Record 20 samples at 15 seconds
      dist = Enum.reduce(1..20, dist, fn _, d -> AckTimeDistribution.record(d, 15) end)

      assert LeaseManager.effective_deadline(dist, false) == 15
    end

    test "returns max(p99, 60) when exactly_once is true" do
      dist = AckTimeDistribution.new(30)

      # Record 20 samples at 15 seconds — p99 would be 15
      dist = Enum.reduce(1..20, dist, fn _, d -> AckTimeDistribution.record(d, 15) end)

      # Without exactly-once: 15
      assert LeaseManager.effective_deadline(dist, false) == 15
      # With exactly-once: max(15, 60) = 60
      assert LeaseManager.effective_deadline(dist, true) == 60
    end

    test "exactly-once with high p99 uses the p99" do
      dist = AckTimeDistribution.new(60)

      # Record 20 samples at 120 seconds — p99 is 120
      dist = Enum.reduce(1..20, dist, fn _, d -> AckTimeDistribution.record(d, 120) end)

      # max(120, 60) = 120
      assert LeaseManager.effective_deadline(dist, true) == 120
    end
  end

  # ============================================================
  # extend_leases/4
  # ============================================================

  describe "extend_leases/4" do
    test "all messages valid — no expired, all keys in modack_ids" do
      dist = AckTimeDistribution.new(60)
      now = 1_000_000

      outstanding = %{
        "id-1" => %{received_at: now - 1_000, max_expiry: now + 100_000},
        "id-2" => %{received_at: now - 2_000, max_expiry: now + 200_000}
      }

      result = LeaseManager.extend_leases(outstanding, dist, false, now)

      assert map_size(result.valid) == 2
      assert result.expired_count == 0
      assert Enum.sort(result.modack_ids) == ["id-1", "id-2"]
      assert result.modack_deadline == 60
      assert result.next_timer_ms > 0
    end

    test "mix of valid and expired — correct partition" do
      dist = AckTimeDistribution.new(60)
      now = 1_000_000

      outstanding = %{
        "valid-1" => %{received_at: now - 1_000, max_expiry: now + 100_000},
        "expired-1" => %{received_at: now - 500_000, max_expiry: now - 1}
      }

      result = LeaseManager.extend_leases(outstanding, dist, false, now)

      assert map_size(result.valid) == 1
      assert Map.has_key?(result.valid, "valid-1")
      assert result.expired_count == 1
      assert result.modack_ids == ["valid-1"]
    end

    test "all expired — valid is empty, modack_ids is empty" do
      dist = AckTimeDistribution.new(60)
      now = 1_000_000

      outstanding = %{
        "expired-1" => %{received_at: 0, max_expiry: now - 1},
        "expired-2" => %{received_at: 0, max_expiry: now - 100}
      }

      result = LeaseManager.extend_leases(outstanding, dist, false, now)

      assert result.valid == %{}
      assert result.expired_count == 2
      assert result.modack_ids == []
    end

    test "empty outstanding — no-op" do
      dist = AckTimeDistribution.new(60)
      now = 1_000_000

      result = LeaseManager.extend_leases(%{}, dist, false, now)

      assert result.valid == %{}
      assert result.expired_count == 0
      assert result.modack_ids == []
    end

    test "next_timer_ms is within expected jitter range" do
      dist = AckTimeDistribution.new(60)
      now = 1_000_000

      # With deadline=60, base = max(1000, (60-5)*1000) = 55_000
      # jitter factor in (0.8, 0.9), after round: [44_000, 49_500]
      results =
        for _ <- 1..50 do
          LeaseManager.extend_leases(%{}, dist, false, now).next_timer_ms
        end

      assert Enum.all?(results, fn ms -> ms >= 44_000 and ms <= 49_500 end)
    end

    test "modack_deadline equals the effective deadline" do
      dist = AckTimeDistribution.new(45)

      result = LeaseManager.extend_leases(%{}, dist, false, 0)

      assert result.modack_deadline == 45
    end

    test "exactly-once enabled — deadline is at least 60" do
      dist = AckTimeDistribution.new(30)

      result = LeaseManager.extend_leases(%{}, dist, true, 0)

      assert result.modack_deadline >= 60
    end

    test "boundary: max_expiry exactly at now — treated as expired" do
      dist = AckTimeDistribution.new(60)
      now = 1_000_000

      outstanding = %{
        "boundary" => %{received_at: 0, max_expiry: now}
      }

      # max_expiry > now is false when max_expiry == now
      result = LeaseManager.extend_leases(outstanding, dist, false, now)

      assert result.expired_count == 1
      assert result.valid == %{}
    end
  end

  # ============================================================
  # sweep_stale_pending_modacks/3
  # ============================================================

  describe "sweep_stale_pending_modacks/3" do
    test "all fresh — no stale ids" do
      now = 100_000
      ref = make_ref()

      pending = %{
        ref => %{ack_ids: ["id-1", "id-2"], received_at: now - 1_000}
      }

      result = LeaseManager.sweep_stale_pending_modacks(pending, now)

      assert result.stale_ack_ids == []
      assert map_size(result.fresh) == 1
    end

    test "all stale — all ack_ids returned" do
      now = 100_000
      ref = make_ref()

      pending = %{
        ref => %{ack_ids: ["id-1", "id-2"], received_at: now - 61_000}
      }

      result = LeaseManager.sweep_stale_pending_modacks(pending, now)

      assert Enum.sort(result.stale_ack_ids) == ["id-1", "id-2"]
      assert result.fresh == %{}
    end

    test "mix of fresh and stale" do
      now = 100_000
      fresh_ref = make_ref()
      stale_ref = make_ref()

      pending = %{
        fresh_ref => %{ack_ids: ["fresh-1"], received_at: now - 1_000},
        stale_ref => %{ack_ids: ["stale-1"], received_at: now - 61_000}
      }

      result = LeaseManager.sweep_stale_pending_modacks(pending, now)

      assert result.stale_ack_ids == ["stale-1"]
      assert map_size(result.fresh) == 1
      assert Map.has_key?(result.fresh, fresh_ref)
    end

    test "empty map — no-op" do
      result = LeaseManager.sweep_stale_pending_modacks(%{}, 100_000)

      assert result.stale_ack_ids == []
      assert result.fresh == %{}
    end

    test "custom threshold" do
      now = 100_000
      ref = make_ref()

      pending = %{
        ref => %{ack_ids: ["id-1"], received_at: now - 5_000}
      }

      # Default threshold (60s) — still fresh
      result = LeaseManager.sweep_stale_pending_modacks(pending, now)
      assert result.stale_ack_ids == []

      # Custom threshold (3s) — now stale
      result = LeaseManager.sweep_stale_pending_modacks(pending, now, 3_000)
      assert result.stale_ack_ids == ["id-1"]
    end
  end

  # ============================================================
  # initial_timer_ms/1
  # ============================================================

  describe "initial_timer_ms/1" do
    test "returns value in expected jitter range for deadline 60" do
      # base = max(1000, (60-5)*1000) = 55_000
      # jitter factor in (0.8, 0.9), after round: [44_000, 49_500]
      results = for _ <- 1..50, do: LeaseManager.initial_timer_ms(60)

      assert Enum.all?(results, fn ms -> ms >= 44_000 and ms <= 49_500 end)
    end

    test "minimum 1000ms regardless of deadline" do
      # With deadline=6, base = max(1000, (6-5)*1000) = max(1000, 1000) = 1000
      # jitter factor in (0.8, 0.9), after round: [800, 900]
      results = for _ <- 1..50, do: LeaseManager.initial_timer_ms(6)

      assert Enum.all?(results, fn ms -> ms >= 800 and ms <= 900 end)
    end

    test "very short deadline still respects minimum" do
      # With deadline=5, base = max(1000, (5-5)*1000) = max(1000, 0) = 1000
      # jitter factor in (0.8, 0.9), after round: [800, 900]
      results = for _ <- 1..50, do: LeaseManager.initial_timer_ms(5)

      assert Enum.all?(results, fn ms -> ms >= 800 and ms <= 900 end)
    end
  end
end
