defmodule BroadwayCloudPubSub.EmulatorTest do
  # Not async: these tests set the PUBSUB_EMULATOR_HOST environment variable,
  # which every producer reads when it starts.
  use ExUnit.Case, async: false

  alias BroadwayCloudPubSub.Emulator

  @env_var "PUBSUB_EMULATOR_HOST"

  setup do
    on_exit(fn -> System.delete_env(@env_var) end)
  end

  describe "host/0" do
    test "is nil when the variable is unset" do
      System.delete_env(@env_var)
      assert Emulator.host() == nil
    end

    test "is nil when the variable is empty" do
      System.put_env(@env_var, "")
      assert Emulator.host() == nil
    end

    test "is the variable's value when set" do
      System.put_env(@env_var, "localhost:8085")
      assert Emulator.host() == "localhost:8085"
    end
  end

  test "token_generator/0 returns a placeholder token" do
    {m, f, a} = Emulator.token_generator()
    assert apply(m, f, a) == {:ok, "emulator"}
  end

  describe "BroadwayCloudPubSub.Producer" do
    alias BroadwayCloudPubSub.Producer

    defp streaming_opts(producer_opts) do
      broadway_opts = [
        name: Module.concat(__MODULE__, "Streaming#{System.unique_integer([:positive])}"),
        producer: [
          module: {Producer, [subscription: "projects/p/subscriptions/s"] ++ producer_opts},
          concurrency: 1
        ],
        processors: [default: []]
      ]

      {_specs, updated} = Producer.prepare_for_start(Producer, broadway_opts)
      {_module, opts} = updated[:producer][:module]
      opts
    end

    test "connects to the emulator without TLS or Goth when the variable is set" do
      System.put_env(@env_var, "localhost:8085")

      opts = streaming_opts(goth: SomeGoth)

      assert opts[:grpc_endpoint] == "localhost:8085"
      assert opts[:use_ssl] == false
      assert opts[:token_generator] == Emulator.token_generator()
    end

    test "keeps options that were given" do
      System.put_env(@env_var, "localhost:8085")

      opts =
        streaming_opts(
          grpc_endpoint: "other:9000",
          use_ssl: true,
          token_generator: {__MODULE__, :custom_token, []}
        )

      assert opts[:grpc_endpoint] == "other:9000"
      assert opts[:use_ssl] == true
      assert opts[:token_generator] == {__MODULE__, :custom_token, []}
    end

    test "uses Google Cloud and Goth when the variable is unset" do
      System.delete_env(@env_var)

      opts = streaming_opts(goth: SomeGoth)

      assert opts[:grpc_endpoint] == "pubsub.googleapis.com:443"
      assert opts[:use_ssl] == true

      assert opts[:token_generator] ==
               {BroadwayCloudPubSub.Pull.Options, :generate_goth_token, [SomeGoth]}
    end
  end

  describe "BroadwayCloudPubSub.Pull.Producer" do
    alias BroadwayCloudPubSub.Pull.Producer

    defp pull_opts(producer_opts) do
      broadway_opts = [
        name: Module.concat(__MODULE__, "Pull#{System.unique_integer([:positive])}"),
        producer: [
          module: {Producer, [subscription: "projects/p/subscriptions/s"] ++ producer_opts},
          concurrency: 1
        ]
      ]

      {_specs, updated} = Producer.prepare_for_start(Producer, broadway_opts)
      {_module, opts} = updated[:producer][:module]
      opts
    end

    test "connects to the emulator over HTTP without Goth when the variable is set" do
      System.put_env(@env_var, "localhost:8085")

      opts = pull_opts(goth: SomeGoth)

      assert opts[:base_url] == "http://localhost:8085"
      assert opts[:token_generator] == Emulator.token_generator()
    end

    test "keeps options that were given" do
      System.put_env(@env_var, "localhost:8085")

      opts =
        pull_opts(
          base_url: "http://other:9000",
          token_generator: {__MODULE__, :custom_token, []}
        )

      assert opts[:base_url] == "http://other:9000"
      assert opts[:token_generator] == {__MODULE__, :custom_token, []}
    end

    test "uses Google Cloud and Goth when the variable is unset" do
      System.delete_env(@env_var)

      opts = pull_opts(goth: SomeGoth)

      assert opts[:base_url] == "https://pubsub.googleapis.com"

      assert opts[:token_generator] ==
               {BroadwayCloudPubSub.Pull.Options, :generate_goth_token, [SomeGoth]}
    end
  end

  def custom_token, do: {:ok, "custom"}
end
