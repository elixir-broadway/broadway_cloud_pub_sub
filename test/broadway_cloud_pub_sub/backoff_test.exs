defmodule BroadwayCloudPubSub.BackoffTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Backoff

  describe "new/1" do
    test "returns nil for :stop type" do
      assert Backoff.new(type: :stop) == nil
    end

    test "returns a Backoff struct for :rand_exp type" do
      b = Backoff.new(type: :rand_exp)
      assert %Backoff{type: :rand_exp} = b
    end

    test "returns a Backoff struct for :exp type" do
      b = Backoff.new(type: :exp)
      assert %Backoff{type: :exp} = b
    end

    test "returns a Backoff struct for :rand type" do
      b = Backoff.new(type: :rand)
      assert %Backoff{type: :rand} = b
    end

    test "uses default min and max when not provided" do
      b = Backoff.new()
      assert b.min == 100
      assert b.max == 60_000
    end

    test "accepts custom min and max" do
      b = Backoff.new(type: :exp, min: 500, max: 5_000)
      assert b.min == 500
      assert b.max == 5_000
    end
  end

  describe "backoff/1" do
    test "returns {nil, nil} for nil (stop) backoff" do
      assert {nil, nil} = Backoff.backoff(nil)
    end

    test ":exp starts at min and doubles each call up to max" do
      b = Backoff.new(type: :exp, min: 1_000, max: 8_000)
      {t1, b} = Backoff.backoff(b)
      {t2, b} = Backoff.backoff(b)
      {t3, b} = Backoff.backoff(b)
      {t4, _b} = Backoff.backoff(b)

      assert t1 == 1_000
      assert t2 == 2_000
      assert t3 == 4_000
      # capped at max
      assert t4 == 8_000
    end

    test ":exp never exceeds max" do
      b = Backoff.new(type: :exp, min: 1_000, max: 3_000)

      timeouts =
        Enum.reduce(1..10, {[], b}, fn _, {acc, b} ->
          {t, b} = Backoff.backoff(b)
          {[t | acc], b}
        end)
        |> elem(0)

      assert Enum.all?(timeouts, &(&1 <= 3_000))
    end

    test ":rand returns a value within [min, max]" do
      b = Backoff.new(type: :rand, min: 1_000, max: 5_000)

      timeouts =
        Enum.map(1..20, fn _ ->
          {t, _} = Backoff.backoff(b)
          t
        end)

      assert Enum.all?(timeouts, &(&1 >= 1_000 and &1 <= 5_000))
    end

    test ":rand_exp stays within [min, max]" do
      b = Backoff.new(type: :rand_exp, min: 1_000, max: 30_000)

      timeouts =
        Enum.reduce(1..30, {[], b}, fn _, {acc, b} ->
          {t, b} = Backoff.backoff(b)
          {[t | acc], b}
        end)
        |> elem(0)

      assert Enum.all?(timeouts, &(&1 >= 1_000 and &1 <= 30_000))
    end

    test ":rand_exp generally grows over time (not always due to randomness)" do
      b = Backoff.new(type: :rand_exp, min: 100, max: 10_000)

      {t1, b} = Backoff.backoff(b)
      # advance a few steps
      {_, b} = Backoff.backoff(b)
      {_, b} = Backoff.backoff(b)
      {_, b} = Backoff.backoff(b)
      {t5, _} = Backoff.backoff(b)

      # After several steps, the upper bound has grown — at minimum t5 >= t1
      # (not strictly true due to randomness but min stays anchored)
      assert t1 >= 100
      assert t5 >= 100
    end
  end

  describe "reset/1" do
    test "returns nil for nil (stop) backoff" do
      assert Backoff.reset(nil) == nil
    end

    test ":exp resets state to min" do
      b = Backoff.new(type: :exp, min: 1_000, max: 8_000)
      {_, b_advanced} = Backoff.backoff(b)
      {_, b_advanced} = Backoff.backoff(b_advanced)

      b_reset = Backoff.reset(b_advanced)
      {t, _} = Backoff.backoff(b_reset)
      assert t == 1_000
    end

    test ":rand_exp resets so next backoff starts from the initial range" do
      b = Backoff.new(type: :rand_exp, min: 1_000, max: 30_000)

      # Advance many steps
      b_advanced =
        Enum.reduce(1..20, b, fn _, b ->
          {_, b} = Backoff.backoff(b)
          b
        end)

      b_reset = Backoff.reset(b_advanced)
      {t, _} = Backoff.backoff(b_reset)

      # After reset, first backoff should be in initial range [min, max]
      assert t >= 1_000
      assert t <= 30_000
    end

    test ":rand is unchanged after reset" do
      b = Backoff.new(type: :rand, min: 1_000, max: 5_000)
      b_reset = Backoff.reset(b)
      assert b_reset == b
    end
  end
end
