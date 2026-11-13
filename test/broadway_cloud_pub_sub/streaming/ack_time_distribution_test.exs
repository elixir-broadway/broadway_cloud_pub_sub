defmodule BroadwayCloudPubSub.Streaming.AckTimeDistributionTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Streaming.AckTimeDistribution

  describe "new/1" do
    test "creates a distribution with the given default deadline" do
      dist = AckTimeDistribution.new(60)
      assert AckTimeDistribution.sample_count(dist) == 0
      # Before enough samples, returns the default
      assert AckTimeDistribution.percentile(dist, 0.99) == 60
    end

    test "clamps default deadline to minimum 10s" do
      dist = AckTimeDistribution.new(5)
      assert AckTimeDistribution.percentile(dist, 0.99) == 10
    end

    test "clamps default deadline to maximum 600s" do
      dist = AckTimeDistribution.new(9999)
      assert AckTimeDistribution.percentile(dist, 0.99) == 600
    end
  end

  describe "record/2" do
    test "records a sample and increments count" do
      dist = AckTimeDistribution.new(60)
      dist = AckTimeDistribution.record(dist, 30)
      assert AckTimeDistribution.sample_count(dist) == 1
    end

    test "clamps recorded value to minimum 10s" do
      dist = AckTimeDistribution.new(60)

      # Fill to min_samples (10) with clamped values
      dist = Enum.reduce(1..10, dist, fn _, d -> AckTimeDistribution.record(d, 2) end)
      # All 10 samples should be clamped to 10s
      assert AckTimeDistribution.percentile(dist, 0.99) == 10
    end

    test "clamps recorded value to maximum 600s" do
      dist = AckTimeDistribution.new(60)
      dist = Enum.reduce(1..10, dist, fn _, d -> AckTimeDistribution.record(d, 9999) end)
      assert AckTimeDistribution.percentile(dist, 0.99) == 600
    end

    test "counters grow monotonically — no eviction beyond 1000 samples" do
      dist = AckTimeDistribution.new(60)
      # Add 1500 samples — all should be counted (no 1000-cap eviction)
      dist = Enum.reduce(1..1500, dist, fn _, d -> AckTimeDistribution.record(d, 30) end)
      assert AckTimeDistribution.sample_count(dist) == 1500
    end
  end

  describe "percentile/2" do
    test "returns default before 10 samples are collected (cold start)" do
      dist = AckTimeDistribution.new(60)
      dist = Enum.reduce(1..9, dist, fn _, d -> AckTimeDistribution.record(d, 120) end)
      # 9 samples — still returns default
      assert AckTimeDistribution.percentile(dist, 0.99) == 60
    end

    test "uses real data after 10 samples" do
      dist = AckTimeDistribution.new(60)
      dist = Enum.reduce(1..10, dist, fn _, d -> AckTimeDistribution.record(d, 120) end)
      # 10 samples of 120s → p99 should be 120
      assert AckTimeDistribution.percentile(dist, 0.99) == 120
    end

    test "p99 is correct for a uniform distribution" do
      dist = AckTimeDistribution.new(60)
      # 100 samples: 1s, 2s, ..., 100s (each clamped to min 10s)
      dist = Enum.reduce(1..100, dist, fn i, d -> AckTimeDistribution.record(d, i) end)
      # p99 of the clamped distribution — 99th bucket >= 99th value
      p99 = AckTimeDistribution.percentile(dist, 0.99)
      assert p99 >= 95 and p99 <= 100
    end

    test "p50 (median) is correct for a uniform distribution" do
      dist = AckTimeDistribution.new(60)
      dist = Enum.reduce(1..100, dist, fn i, d -> AckTimeDistribution.record(d, i) end)
      p50 = AckTimeDistribution.percentile(dist, 0.50)
      # Median of 1..100 clamped to 10..100 → around 50
      assert p50 >= 45 and p50 <= 55
    end

    test "all same values returns that value" do
      dist = AckTimeDistribution.new(60)
      dist = Enum.reduce(1..20, dist, fn _, d -> AckTimeDistribution.record(d, 45) end)
      assert AckTimeDistribution.percentile(dist, 0.99) == 45
      assert AckTimeDistribution.percentile(dist, 0.50) == 45
    end

    test "result is always clamped to 10-600 range" do
      dist = AckTimeDistribution.new(60)
      dist = Enum.reduce(1..10, dist, fn _, d -> AckTimeDistribution.record(d, 300) end)
      p99 = AckTimeDistribution.percentile(dist, 0.99)
      assert p99 >= 10 and p99 <= 600
    end

    test "p0 returns the minimum value in the distribution" do
      dist = AckTimeDistribution.new(60)
      # Records 10s..200s; p0 should return the smallest bucket with data = 10
      dist = Enum.reduce(1..20, dist, fn i, d -> AckTimeDistribution.record(d, i * 10) end)
      p0 = AckTimeDistribution.percentile(dist, 0.0)
      assert p0 == 10
    end

    test "p100 returns the maximum value in the distribution" do
      dist = AckTimeDistribution.new(60)
      dist = Enum.reduce(1..20, dist, fn i, d -> AckTimeDistribution.record(d, i * 10) end)
      p100 = AckTimeDistribution.percentile(dist, 1.0)
      # 20 * 10 = 200s
      assert p100 == 200
    end

    test "monotonic: adding more data at a higher value raises p99" do
      dist = AckTimeDistribution.new(60)
      # 100 samples at 30s
      dist = Enum.reduce(1..100, dist, fn _, d -> AckTimeDistribution.record(d, 30) end)
      p99_before = AckTimeDistribution.percentile(dist, 0.99)
      # Add 10 samples at 300s — now 110 total; p99 = 109th sample = 300s
      dist = Enum.reduce(1..10, dist, fn _, d -> AckTimeDistribution.record(d, 300) end)
      p99_after = AckTimeDistribution.percentile(dist, 0.99)
      assert p99_before == 30
      assert p99_after == 300
    end
  end

  describe "sample_count/1" do
    test "zero for new distribution" do
      dist = AckTimeDistribution.new(60)
      assert AckTimeDistribution.sample_count(dist) == 0
    end

    test "grows monotonically without an upper cap" do
      dist = AckTimeDistribution.new(60)
      dist = Enum.reduce(1..2000, dist, fn _, d -> AckTimeDistribution.record(d, 30) end)
      assert AckTimeDistribution.sample_count(dist) == 2000
    end
  end
end
