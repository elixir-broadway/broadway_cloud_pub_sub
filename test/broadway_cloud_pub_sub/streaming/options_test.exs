defmodule BroadwayCloudPubSub.Streaming.OptionsTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Streaming.Options

  defp validate(opts) do
    # Inject required broadway key for the schema
    opts = Keyword.put_new(opts, :broadway, name: :TestPipeline)
    NimbleOptions.validate(opts, Options.definition())
  end

  describe "subscription" do
    test "is required" do
      assert {:error, err} = validate([])
      assert Exception.message(err) =~ "required"
      assert Exception.message(err) =~ "subscription"
    end

    test "accepts a valid subscription path" do
      assert {:ok, opts} =
               validate(subscription: "projects/my-project/subscriptions/my-sub")

      assert opts[:subscription] == "projects/my-project/subscriptions/my-sub"
    end

    test "rejects an empty string" do
      assert {:error, err} = validate(subscription: "")
      assert Exception.message(err) =~ "non-empty string"
    end

    test "rejects a non-string value" do
      assert {:error, err} = validate(subscription: 123)
      assert Exception.message(err) =~ "non-empty string"
    end
  end

  describe "max_outstanding_messages" do
    test "defaults to 1000" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:max_outstanding_messages] == 1_000
    end

    test "accepts a positive integer" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", max_outstanding_messages: 500)

      assert opts[:max_outstanding_messages] == 500
    end

    test "rejects zero" do
      assert {:error, _} =
               validate(subscription: "projects/p/subscriptions/s", max_outstanding_messages: 0)
    end
  end

  describe "stream_ack_deadline_seconds" do
    test "defaults to 60" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:stream_ack_deadline_seconds] == 60
    end

    test "accepts value within range" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", stream_ack_deadline_seconds: 120)

      assert opts[:stream_ack_deadline_seconds] == 120
    end

    test "rejects value below 10" do
      assert {:error, err} =
               validate(
                 subscription: "projects/p/subscriptions/s",
                 stream_ack_deadline_seconds: 5
               )

      assert Exception.message(err) =~ "between 10 and 600"
    end

    test "rejects value above 600" do
      assert {:error, err} =
               validate(
                 subscription: "projects/p/subscriptions/s",
                 stream_ack_deadline_seconds: 601
               )

      assert Exception.message(err) =~ "between 10 and 600"
    end
  end

  describe "on_success / on_failure" do
    test "on_success defaults to :ack" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:on_success] == :ack
    end

    test "on_failure defaults to {:nack, 0}" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:on_failure] == {:nack, 0}
    end

    test "accepts :ack" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_success: :ack)

      assert opts[:on_success] == :ack
    end

    test "accepts :noop" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_success: :noop)

      assert opts[:on_success] == :noop
    end

    test "normalises :nack to {:nack, 0}" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_success: :nack)

      assert opts[:on_success] == {:nack, 0}
    end

    test "accepts {:nack, integer} within range" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_failure: {:nack, 30})

      assert opts[:on_failure] == {:nack, 30}
    end

    test "rejects invalid on_success value" do
      assert {:error, err} =
               validate(subscription: "projects/p/subscriptions/s", on_success: :bad)

      assert Exception.message(err) =~ "on_success"
    end
  end

  describe "on_shutdown" do
    test "defaults to {:nack, 5}" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:on_shutdown] == {:nack, 5}
    end

    test "accepts :noop" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_shutdown: :noop)

      assert opts[:on_shutdown] == :noop
    end

    test "normalises :nack to {:nack, 0}" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_shutdown: :nack)

      assert opts[:on_shutdown] == {:nack, 0}
    end

    test "accepts {:nack, N}" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_shutdown: {:nack, 10})

      assert opts[:on_shutdown] == {:nack, 10}
    end

    test "rejects invalid shutdown option" do
      assert {:error, err} =
               validate(subscription: "projects/p/subscriptions/s", on_shutdown: :ack)

      assert Exception.message(err) =~ "on_shutdown"
    end
  end

  describe "backoff_type" do
    test "defaults to :rand_exp" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:backoff_type] == :rand_exp
    end

    test "accepts all valid types" do
      for type <- [:rand_exp, :exp, :rand, :stop] do
        assert {:ok, _} =
                 validate(subscription: "projects/p/subscriptions/s", backoff_type: type)
      end
    end

    test "rejects unknown type" do
      assert {:error, _} =
               validate(subscription: "projects/p/subscriptions/s", backoff_type: :linear)
    end
  end

  describe "grpc_endpoint" do
    test "defaults to pubsub.googleapis.com:443" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:grpc_endpoint] == "pubsub.googleapis.com:443"
    end

    test "accepts custom endpoint" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", grpc_endpoint: "localhost:8085")

      assert opts[:grpc_endpoint] == "localhost:8085"
    end

    test "rejects empty string" do
      assert {:error, err} =
               validate(subscription: "projects/p/subscriptions/s", grpc_endpoint: "")

      assert Exception.message(err) =~ "non-empty string"
    end
  end

  describe "type_ack_option/2" do
    test "converts :nack atom to {:nack, 0}" do
      assert {:ok, {:nack, 0}} = Options.type_ack_option(:nack, [])
    end

    test "passes through :ack" do
      assert {:ok, :ack} = Options.type_ack_option(:ack, [])
    end

    test "passes through :noop" do
      assert {:ok, :noop} = Options.type_ack_option(:noop, [])
    end

    test "passes through {:nack, N} within range" do
      assert {:ok, {:nack, 300}} = Options.type_ack_option({:nack, 300}, [])
    end

    test "rejects {:nack, N} outside range" do
      assert {:error, _} = Options.type_ack_option({:nack, 601}, name: :on_success)
    end
  end

  describe "type_shutdown_option/2" do
    test "converts :nack to {:nack, 0}" do
      assert {:ok, {:nack, 0}} = Options.type_shutdown_option(:nack, [])
    end

    test "accepts :noop" do
      assert {:ok, :noop} = Options.type_shutdown_option(:noop, [])
    end

    test "accepts {:nack, N}" do
      assert {:ok, {:nack, 5}} = Options.type_shutdown_option({:nack, 5}, [])
    end

    test "rejects :ack" do
      assert {:error, _} =
               Options.type_shutdown_option(:ack, name: :on_shutdown)
    end
  end

  describe "adapter" do
    test "defaults to GRPC.Client.Adapters.Gun" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:adapter] == GRPC.Client.Adapters.Gun
    end

    test ":gun resolves to GRPC.Client.Adapters.Gun" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s", adapter: :gun)
      assert opts[:adapter] == GRPC.Client.Adapters.Gun
    end

    test ":mint resolves to GRPC.Client.Adapters.Mint" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s", adapter: :mint)
      assert opts[:adapter] == GRPC.Client.Adapters.Mint
    end

    test "accepts a custom module that exports connect/2" do
      {:ok, opts} =
        validate(
          subscription: "projects/p/subscriptions/s",
          adapter: BroadwayCloudPubSub.Test.GrpcTestAdapter
        )

      assert opts[:adapter] == BroadwayCloudPubSub.Test.GrpcTestAdapter
    end

    test "rejects a non-atom value" do
      assert {:error, err} =
               validate(subscription: "projects/p/subscriptions/s", adapter: "gun")

      assert Exception.message(err) =~ "adapter"
    end

    test "rejects an atom that is not a loaded module" do
      assert {:error, err} =
               validate(
                 subscription: "projects/p/subscriptions/s",
                 adapter: VeryUnlikelyToExist.ModuleAtom
               )

      assert Exception.message(err) =~ "adapter"
    end
  end

  describe "enable_message_ordering" do
    test "defaults to false" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:enable_message_ordering] == false
    end

    test "accepts true" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", enable_message_ordering: true)

      assert opts[:enable_message_ordering] == true
    end

    test "accepts false explicitly" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", enable_message_ordering: false)

      assert opts[:enable_message_ordering] == false
    end

    test "rejects non-boolean" do
      assert {:error, _} =
               validate(subscription: "projects/p/subscriptions/s", enable_message_ordering: 1)
    end
  end

  describe "telemetry_metadata" do
    test "is optional — omitting it leaves the key absent" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      refute Keyword.has_key?(opts, :telemetry_metadata)
    end

    test "accepts a static map" do
      {:ok, opts} =
        validate(
          subscription: "projects/p/subscriptions/s",
          telemetry_metadata: %{tenant_id: "acme", env: :prod}
        )

      assert opts[:telemetry_metadata] == %{tenant_id: "acme", env: :prod}
    end

    test "accepts any static term (keyword list, atom, string)" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", telemetry_metadata: [a: 1])

      assert opts[:telemetry_metadata] == [a: 1]

      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", telemetry_metadata: :my_tag)

      assert opts[:telemetry_metadata] == :my_tag

      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", telemetry_metadata: "label")

      assert opts[:telemetry_metadata] == "label"
    end

    test "accepts a valid MFA tuple" do
      {:ok, opts} =
        validate(
          subscription: "projects/p/subscriptions/s",
          telemetry_metadata: {__MODULE__, :sample_meta, []}
        )

      assert opts[:telemetry_metadata] == {__MODULE__, :sample_meta, []}
    end

    test "rejects an MFA whose module is not loaded" do
      assert {:error, err} =
               validate(
                 subscription: "projects/p/subscriptions/s",
                 telemetry_metadata: {NotLoadedModuleXYZ, :some_fun, []}
               )

      assert Exception.message(err) =~ "could not be loaded"
    end

    test "rejects an MFA whose function is not exported" do
      assert {:error, err} =
               validate(
                 subscription: "projects/p/subscriptions/s",
                 telemetry_metadata: {__MODULE__, :nonexistent_fun, []}
               )

      assert Exception.message(err) =~ "not exported"
    end
  end

  describe "interceptors" do
    test "defaults to []" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:interceptors] == []
    end

    test "accepts a bare module list" do
      {:ok, opts} =
        validate(
          subscription: "projects/p/subscriptions/s",
          interceptors: [GRPC.Client.Interceptors.Logger]
        )

      assert opts[:interceptors] == [GRPC.Client.Interceptors.Logger]
    end

    test "accepts a {module, opts} tuple list" do
      {:ok, opts} =
        validate(
          subscription: "projects/p/subscriptions/s",
          interceptors: [{GRPC.Client.Interceptors.Logger, level: :warning}]
        )

      assert opts[:interceptors] == [{GRPC.Client.Interceptors.Logger, level: :warning}]
    end

    test "accepts a mixed list of bare modules and {module, opts} tuples" do
      {:ok, opts} =
        validate(
          subscription: "projects/p/subscriptions/s",
          interceptors: [
            GRPC.Client.Interceptors.Logger,
            {GRPC.Client.Interceptors.Logger, level: :debug}
          ]
        )

      assert length(opts[:interceptors]) == 2
    end

    test "rejects a non-list value" do
      assert {:error, err} =
               validate(
                 subscription: "projects/p/subscriptions/s",
                 interceptors: GRPC.Client.Interceptors.Logger
               )

      assert Exception.message(err) =~ "interceptors"
      assert Exception.message(err) =~ "list"
    end

    test "rejects an entry whose module is not loaded" do
      assert {:error, err} =
               validate(
                 subscription: "projects/p/subscriptions/s",
                 interceptors: [VeryUnlikelyToExist.InterceptorXYZ]
               )

      assert Exception.message(err) =~ "could not be loaded"
    end

    test "rejects an entry that is not a module or {module, opts} tuple" do
      assert {:error, err} =
               validate(
                 subscription: "projects/p/subscriptions/s",
                 interceptors: [:not_valid]
               )

      assert Exception.message(err) =~ "interceptor"
    end

    test "rejects a module that does not implement GRPC.Client.Interceptor" do
      # String module is loaded but doesn't export init/1 or call/4
      assert {:error, err} =
               validate(
                 subscription: "projects/p/subscriptions/s",
                 interceptors: [String]
               )

      assert Exception.message(err) =~ "GRPC.Client.Interceptor"
    end
  end

  # Used in telemetry_metadata MFA tests above.
  def sample_meta, do: %{node: node()}
end

defmodule BroadwayCloudPubSub.Streaming.ProducerPrepareForStartTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Producer

  # Minimal broadway_opts that satisfies prepare_for_start/2.
  defp broadway_opts(producer_opts \\ []) do
    base_producer_opts = [
      subscription: "projects/test-project/subscriptions/test-sub",
      token_generator: {__MODULE__, :noop_token, []},
      grpc_endpoint: "localhost:8085",
      use_ssl: false
    ]

    [
      name: TestPipeline,
      producer: [
        module: {Producer, Keyword.merge(base_producer_opts, producer_opts)},
        concurrency: 1
      ],
      processors: [default: []]
    ]
  end

  def noop_token, do: {:ok, "test-token"}

  describe "prepare_for_start/2" do
    test "grpc_client_config contains :broadway_name so GrpcClient telemetry does not crash" do
      # GrpcClient.acknowledge/3 and modify_ack_deadline/3 read config.broadway_name
      # from grpc_client_config for telemetry. This verifies that broadway_name is
      # injected into opts *before* grpc_client.init/1 is called, so it ends up in
      # the returned config map.
      {_specs, updated_opts} = Producer.prepare_for_start(Producer, broadway_opts())

      {_module, producer_opts} = updated_opts[:producer][:module]
      grpc_client_config = producer_opts[:grpc_client_config]

      assert is_map(grpc_client_config),
             "expected grpc_client_config to be a map, got: #{inspect(grpc_client_config)}"

      assert Map.has_key?(grpc_client_config, :broadway_name),
             ":broadway_name missing from grpc_client_config — GrpcClient telemetry would crash"

      assert grpc_client_config.broadway_name == TestPipeline
    end

    test "returns one child spec: UnaryAckSupervisor (StreamManagers are started per-producer in init)" do
      {specs, _opts} = Producer.prepare_for_start(Producer, broadway_opts())

      assert length(specs) == 1
      [sup_spec] = specs
      assert sup_spec.type == :supervisor
    end

    test ":grpc_client accepts a bare module" do
      {_specs, updated_opts} = Producer.prepare_for_start(Producer, broadway_opts())
      {_module, producer_opts} = updated_opts[:producer][:module]

      # Default GrpcClient stored as bare module
      assert producer_opts[:grpc_client] == BroadwayCloudPubSub.Streaming.GrpcClient
    end

    test ":grpc_client accepts {Module, inner_opts} and merges inner_opts into producer opts" do
      defmodule FakeGrpcClient do
        @behaviour BroadwayCloudPubSub.Streaming.Client

        @impl true
        def init(opts), do: {:ok, Map.new(opts)}

        @impl true
        def connect(_opts), do: {:ok, :fake_channel}

        @impl true
        def recv_messages(_channel, _opts), do: []

        @impl true
        def acknowledge(_channel, _ack_ids, _opts), do: :ok

        @impl true
        def modify_ack_deadline(_channel, _ack_ids, _deadline, _opts), do: :ok
      end

      opts = broadway_opts(grpc_client: {FakeGrpcClient, custom_opt: :hello})
      {_specs, updated_opts} = Producer.prepare_for_start(Producer, opts)
      {_module, producer_opts} = updated_opts[:producer][:module]

      # Module extracted from tuple
      assert producer_opts[:grpc_client] == FakeGrpcClient
      # Inner opts merged into producer opts
      assert producer_opts[:custom_opt] == :hello
    end
  end
end
