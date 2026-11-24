defmodule BroadwayCloudPubSub.Emulator do
  @moduledoc false

  # Knows the `PUBSUB_EMULATOR_HOST` convention shared by Google's client
  # libraries and the placeholder token the emulator accepts. Each producer's
  # options module decides how the host maps to its own options.

  @env_var "PUBSUB_EMULATOR_HOST"

  @doc """
  Returns the emulator's `host:port` from `PUBSUB_EMULATOR_HOST`, or `nil`
  when the variable is unset or empty.
  """
  @spec host() :: String.t() | nil
  def host do
    case System.get_env(@env_var) do
      host when host in [nil, ""] -> nil
      host -> host
    end
  end

  @doc """
  Returns an MFArgs token generator for the emulator, which does not check
  credentials.
  """
  @spec token_generator() :: {module(), atom(), list()}
  def token_generator, do: {__MODULE__, :generate_token, []}

  @doc false
  def generate_token, do: {:ok, "emulator"}
end
