defmodule BroadwayCloudPubSub.Backoff do
  @moduledoc false

  # Pure-functional backoff calculator for StreamingPull reconnection.
  # Supports :rand_exp (randomized exponential), :exp (pure exponential),
  # :rand (pure random), and :stop (no reconnect).

  # Aligned with Options defaults (backoff_min: 100, backoff_max: 60_000).
  @default_min 100
  @default_max 60_000

  @type type :: :rand_exp | :exp | :rand | :stop

  @type t :: %__MODULE__{
          type: type(),
          min: non_neg_integer(),
          max: non_neg_integer(),
          state: term()
        }

  defstruct [:type, :min, :max, :state]

  @doc """
  Creates a new Backoff struct.

  Returns `nil` if `type` is `:stop`, indicating no reconnection.

  ## Options

    * `:type` - `:rand_exp` (default), `:exp`, `:rand`, or `:stop`
    * `:min` - minimum backoff in milliseconds (default: 100)
    * `:max` - maximum backoff in milliseconds (default: 60000)

  """
  @spec new(keyword()) :: t() | nil
  def new(opts \\ []) do
    type = Keyword.get(opts, :type, :rand_exp)
    min = Keyword.get(opts, :min, @default_min)
    max = Keyword.get(opts, :max, @default_max)

    case type do
      :stop ->
        nil

      :rand_exp ->
        # `lower` prevents the randomized minimum from dropping below max/3,
        # ensuring backoff stays meaningful even after many retries. With
        # defaults (min=100, max=60_000), the minimum eventually floors at
        # 20s rather than continuing to start from 100ms. This matches the
        # gax library's backoff behavior.
        lower = max(min, div(max, 3))
        %__MODULE__{type: :rand_exp, min: min, max: max, state: {min, lower, seed()}}

      :exp ->
        %__MODULE__{type: :exp, min: min, max: max, state: min}

      :rand ->
        %__MODULE__{type: :rand, min: min, max: max, state: nil}
    end
  end

  @doc """
  Returns the next backoff timeout and an updated Backoff struct.

  Returns `{nil, nil}` if the Backoff is `nil` (`:stop` type).
  """
  @spec backoff(t() | nil) :: {non_neg_integer() | nil, t() | nil}
  def backoff(nil), do: {nil, nil}

  def backoff(%__MODULE__{type: :rand_exp, min: _min, max: max, state: {prev, lower, seed}} = b) do
    next_min = min(prev, lower)
    next_max = min(prev * 2, max)
    {timeout, seed} = rand(next_min, next_max, seed)
    {timeout, %{b | state: {min(next_max, max), lower, seed}}}
  end

  def backoff(%__MODULE__{type: :exp, min: _min, max: max, state: prev} = b) do
    timeout = min(prev, max)
    {timeout, %{b | state: min(prev * 2, max)}}
  end

  def backoff(%__MODULE__{type: :rand, min: min, max: max} = b) do
    {timeout, _} = rand(min, max, seed())
    {timeout, b}
  end

  @doc """
  Resets the backoff state to its initial value after a successful connection.
  """
  @spec reset(t() | nil) :: t() | nil
  def reset(nil), do: nil

  def reset(%__MODULE__{type: :rand_exp, min: min, max: max} = b) do
    lower = max(min, div(max, 3))
    %{b | state: {min, lower, seed()}}
  end

  def reset(%__MODULE__{type: :exp, min: min} = b) do
    %{b | state: min}
  end

  def reset(%__MODULE__{type: :rand} = b), do: b

  # --- Private helpers ---

  defp rand(min, max, seed) when min >= max do
    {min, seed}
  end

  defp rand(min, max, seed) do
    # :rand.uniform_s(N) returns a value in [1, N], so we use (max - min + 1)
    # and subtract 1 to get the correct range [min, max].
    {value, new_seed} = :rand.uniform_s(max - min + 1, seed)
    {value - 1 + min, new_seed}
  end

  defp seed do
    :rand.seed_s(:exsplus)
  end
end
