defmodule BroadwayCloudPubSub.Pull.ProducerTest do
  use ExUnit.Case

  alias Broadway.Message
  alias NimbleOptions.ValidationError

  defmodule MessageServer do
    use GenServer

    def start_link do
      GenServer.start_link(__MODULE__, [])
    end

    def push_messages(server, messages, timeout \\ 5000) do
      GenServer.call(server, {:messages, messages}, timeout)
    end

    def take_messages(server, amount, timeout \\ 5000) do
      GenServer.call(server, {:take, amount}, timeout)
    end

    def init(_opts) do
      {:ok, %{queue: [], awaiting: nil}}
    end

    def handle_call({:messages, messages}, _from, state) do
      messages = Enum.to_list(messages)

      if state.awaiting do
        {to_return, to_keep} = Enum.split(state.queue ++ messages, state.awaiting.amount)
        GenServer.reply(state.awaiting.from, to_return)

        {:reply, :ok, %{state | awaiting: nil, queue: state.queue ++ to_keep}}
      else
        {:reply, :ok, %{state | queue: state.queue ++ messages}}
      end
    end

    def handle_call({:take, amount}, from, state) do
      if length(state.queue) != 0 do
        {to_return, to_keep} = Enum.split(state.queue, amount)

        {:reply, to_return, %{state | queue: to_keep}}
      else
        state = %{state | awaiting: %{from: from, amount: amount}}
        {:noreply, state}
      end
    end
  end

  defmodule FakeClient do
    alias BroadwayCloudPubSub.Pull.Client
    alias Broadway.Acknowledger

    @behaviour Client
    @behaviour Acknowledger

    @impl Client
    def init(opts), do: {:ok, opts}

    @impl Client
    def receive_messages(amount, _builder, opts) do
      messages = MessageServer.take_messages(opts[:message_server], amount)
      send(opts[:test_pid], {:messages_received, length(messages)})

      for msg <- messages do
        ack_data = %{
          receipt: %{id: "Id_#{msg}", receipt_handle: "ReceiptHandle_#{msg}"},
          test_pid: opts[:test_pid]
        }

        %Message{data: msg, acknowledger: {__MODULE__, :ack_ref, ack_data}}
      end
    end

    @impl Acknowledger
    def ack(_ack_ref, successful, _failed) do
      [%Message{acknowledger: {_, _, %{test_pid: test_pid}}} | _] = successful
      send(test_pid, {:messages_deleted, length(successful)})
    end
  end

  defmodule FakePrepareToConnectClient do
    alias BroadwayCloudPubSub.Pull.Client

    @behaviour Client

    @impl Client
    def prepare_to_connect(_module, opts) do
      ref =
        opts[:prepare_to_connect_ref] ||
          raise "expected :prepare_to_connect_ref option to be set, but it was not"

      test_pid = opts[:test_pid]

      task =
        {Task,
         fn ->
           send(test_pid, {:prepare_to_connect_spec, ref})
           Process.sleep(:infinity)
         end}

      {[task], Keyword.put(opts, :prepare_to_connect_ref, ref)}
    end

    @impl Client
    def init(opts) do
      send(opts[:test_pid], {:prepare_to_connect_opts, opts[:prepare_to_connect_ref]})

      {:ok, opts}
    end

    @impl Client
    def receive_messages(_amount, _builder, _opts), do: []
  end

  defmodule Forwarder do
    use Broadway

    def handle_message(_, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data})
      message
    end

    def handle_batch(_, messages, _, _) do
      messages
    end
  end

  defp prepare_for_start_module_opts(module_opts) do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)

    try do
      BroadwayCloudPubSub.Pull.Producer.prepare_for_start(Forwarder,
        producer: [
          module: {BroadwayCloudPubSub.Pull.Producer, module_opts},
          concurrency: 1
        ],
        name: __MODULE__
      )
    after
      stop_broadway(pid)
    end
  end

  describe "prepare_for_start/2 validation" do
    test ":subscription should be a string" do
      assert_raise(
        ValidationError,
        ~r/required :subscription option not found, received options: /,
        fn ->
          prepare_for_start_module_opts([])
        end
      )

      assert_raise(
        ValidationError,
        ~r/expected :subscription to be a non-empty string, got: nil/,
        fn ->
          prepare_for_start_module_opts(subscription: nil)
        end
      )

      assert_raise(
        ValidationError,
        ~r/expected :subscription to be a non-empty string, got: \"\"/,
        fn ->
          prepare_for_start_module_opts(subscription: "")
        end
      )

      assert_raise(
        ValidationError,
        ~r/expected :subscription to be a non-empty string, got: :foo/,
        fn ->
          prepare_for_start_module_opts(subscription: :foo)
        end
      )

      assert {
               _,
               [
                 producer: [
                   module: {BroadwayCloudPubSub.Pull.Producer, producer_opts},
                   concurrency: 1
                 ],
                 name: __MODULE__
               ]
             } =
               prepare_for_start_module_opts(
                 goth: FakeAuth,
                 subscription: "projects/foo/subscriptions/bar"
               )

      assert producer_opts[:subscription] == "projects/foo/subscriptions/bar"
    end

    test ":goth is optional, but logs an error with the default token generator" do
      assert ExUnit.CaptureLog.capture_log(fn ->
               assert {_,
                       [
                         producer: [
                           module: {BroadwayCloudPubSub.Pull.Producer, producer_opts},
                           concurrency: 1
                         ],
                         name: __MODULE__
                       ]} =
                        prepare_for_start_module_opts(
                          subscription: "projects/foo/subscriptions/bar"
                        )

               assert Keyword.fetch(producer_opts, :goth) == :error
             end) =~ "the :goth option is required for the default authentication token generator"
    end

    test ":max_number_of_messages is optional with default value 10" do
      assert {_,
              [
                producer: [
                  module: {BroadwayCloudPubSub.Pull.Producer, producer_opts},
                  concurrency: 1
                ],
                name: __MODULE__
              ]} =
               prepare_for_start_module_opts(
                 goth: FakeAuth,
                 subscription: "projects/foo/subscriptions/bar"
               )

      assert producer_opts[:max_number_of_messages] == 10
    end

    test ":max_number_of_messages should be a positive integer" do
      assert {_,
              [
                producer: [
                  module: {BroadwayCloudPubSub.Pull.Producer, result_module_opts},
                  concurrency: 1
                ],
                name: __MODULE__
              ]} =
               prepare_for_start_module_opts(
                 goth: FakeAuth,
                 subscription: "projects/foo/subscriptions/bar",
                 max_number_of_messages: 1
               )

      assert result_module_opts[:max_number_of_messages] == 1

      assert {_,
              [
                producer: [
                  module: {BroadwayCloudPubSub.Pull.Producer, result_module_opts},
                  concurrency: 1
                ],
                name: __MODULE__
              ]} =
               prepare_for_start_module_opts(
                 goth: FakeAuth,
                 subscription: "projects/foo/subscriptions/bar",
                 max_number_of_messages: 10
               )

      assert result_module_opts[:max_number_of_messages] == 10

      assert_raise(
        ValidationError,
        ~r/invalid value for :max_number_of_messages option: expected positive integer, got: 0/,
        fn ->
          prepare_for_start_module_opts(
            goth: FakeAuth,
            subscription: "projects/foo/subscriptions/bar",
            max_number_of_messages: 0
          )
        end
      )

      assert_raise(
        ValidationError,
        ~r/invalid value for :max_number_of_messages option: expected positive integer, got: -1/,
        fn ->
          prepare_for_start_module_opts(
            goth: FakeAuth,
            subscription: "projects/foo/subscriptions/bar",
            max_number_of_messages: -1
          )
        end
      )
    end

    test ":token_generator defaults to using Goth with the :goth option" do
      assert {_,
              [
                producer: [
                  module: {BroadwayCloudPubSub.Pull.Producer, producer_opts},
                  concurrency: 1
                ],
                name: __MODULE__
              ]} =
               prepare_for_start_module_opts(
                 goth: FakeAuth,
                 subscription: "projects/foo/subscriptions/bar"
               )

      assert producer_opts[:token_generator] ==
               {BroadwayCloudPubSub.Pull.Options, :generate_goth_token, [FakeAuth]}
    end

    test ":token_generator should be a tuple {Mod, Fun, Args}" do
      token_generator = {Token, :fetch, []}

      assert {_,
              [
                producer: [
                  module: {BroadwayCloudPubSub.Pull.Producer, producer_opts},
                  concurrency: 1
                ],
                name: __MODULE__
              ]} =
               prepare_for_start_module_opts(
                 subscription: "projects/foo/subscriptions/bar",
                 token_generator: token_generator
               )

      assert producer_opts[:token_generator] == token_generator

      assert_raise ValidationError,
                   ~r/invalid value for :token_generator option: expected tuple {mod, fun, args}, got: {1, 1, 1}/,
                   fn ->
                     prepare_for_start_module_opts(
                       subscription: "projects/foo/subscriptions/bar",
                       token_generator: {1, 1, 1}
                     )
                   end

      assert_raise ValidationError,
                   ~r/invalid value for :token_generator option: expected tuple {mod, fun, args}, got: SomeModule/,
                   fn ->
                     prepare_for_start_module_opts(
                       subscription: "projects/foo/subscriptions/bar",
                       token_generator: SomeModule
                     )
                   end
    end

    test ":receive_timeout is optional with default value :infinity" do
      assert {_,
              [
                producer: [
                  module: {BroadwayCloudPubSub.Pull.Producer, producer_opts},
                  concurrency: 1
                ],
                name: __MODULE__
              ]} =
               prepare_for_start_module_opts(
                 goth: FakeAuth,
                 subscription: "projects/foo/subscriptions/bar"
               )

      assert producer_opts[:receive_timeout] == :infinity
    end

    test ":receive_timeout should be a positive integer or :infinity" do
      for value <- [0, -1, :an_atom, SomeModule] do
        assert_raise ValidationError,
                     ~r/expected :receive_timeout to be a positive integer or :infinity, got: #{inspect(value)}/,
                     fn ->
                       prepare_for_start_module_opts(
                         goth: FakeAuth,
                         subscription: "projects/foo/subscriptions/bar",
                         receive_timeout: value
                       )
                     end
      end

      assert {_,
              [
                producer: [
                  module: {BroadwayCloudPubSub.Pull.Producer, producer_opts},
                  concurrency: 1
                ],
                name: __MODULE__
              ]} =
               prepare_for_start_module_opts(
                 goth: FakeAuth,
                 subscription: "projects/foo/subscriptions/bar",
                 receive_timeout: 15_000
               )

      assert producer_opts[:receive_timeout] == 15_000
    end

    test ":on_success defaults to :ack" do
      assert {_,
              [
                producer: [
                  module: {BroadwayCloudPubSub.Pull.Producer, producer_opts},
                  concurrency: 1
                ],
                name: __MODULE__
              ]} =
               prepare_for_start_module_opts(
                 goth: FakeAuth,
                 subscription: "projects/foo/subscriptions/bar"
               )

      assert producer_opts[:on_success] == :ack
    end

    test ":on_failure defaults to {:nack, 0}" do
      assert {_,
              [
                producer: [
                  module: {BroadwayCloudPubSub.Pull.Producer, producer_opts},
                  concurrency: 1
                ],
                name: __MODULE__
              ]} =
               prepare_for_start_module_opts(
                 goth: FakeAuth,
                 subscription: "projects/foo/subscriptions/bar"
               )

      assert producer_opts[:on_failure] == {:nack, 0}
    end

    test ":on_success should be a valid action" do
      for action <- [:ack, :noop, {:nack, 0}, {:nack, 100}, {:nack, 600}] do
        assert {_,
                [
                  producer: [
                    module: {BroadwayCloudPubSub.Pull.Producer, producer_opts},
                    concurrency: 1
                  ],
                  name: __MODULE__
                ]} =
                 prepare_for_start_module_opts(
                   goth: FakeAuth,
                   subscription: "projects/foo/subscriptions/bar",
                   on_success: action
                 )

        assert producer_opts[:on_success] == action
      end

      assert_raise ValidationError,
                   ~r/expected :on_success to be one of :ack, :noop, :nack, or {:nack, integer} where integer is between 0 and 600, got: :foo/,
                   fn ->
                     prepare_for_start_module_opts(
                       goth: FakeAuth,
                       subscription: "projects/foo/subscriptions/bar",
                       on_success: :foo
                     )
                   end

      assert_raise ValidationError,
                   ~r/expected :on_success to be one of :ack, :noop, :nack, or {:nack, integer} where integer is between 0 and 600, got: "foo"/,
                   fn ->
                     prepare_for_start_module_opts(
                       goth: FakeAuth,
                       subscription: "projects/foo/subscriptions/bar",
                       on_success: "foo"
                     )
                   end

      assert_raise ValidationError,
                   ~r/expected :on_success to be one of :ack, :noop, :nack, or {:nack, integer} where integer is between 0 and 600, got: 1/,
                   fn ->
                     prepare_for_start_module_opts(
                       goth: FakeAuth,
                       subscription: "projects/foo/subscriptions/bar",
                       on_success: 1
                     )
                   end

      assert_raise ValidationError,
                   ~r/expected :on_success to be one of :ack, :noop, :nack, or {:nack, integer} where integer is between 0 and 600, got: SomeModule/,
                   fn ->
                     prepare_for_start_module_opts(
                       goth: FakeAuth,
                       subscription: "projects/foo/subscriptions/bar",
                       on_success: SomeModule
                     )
                   end
    end

    test ":on_failure should be a valid action" do
      for action <- [:ack, :noop, {:nack, 0}, {:nack, 100}, {:nack, 600}] do
        assert {_,
                [
                  producer: [
                    module: {BroadwayCloudPubSub.Pull.Producer, producer_opts},
                    concurrency: 1
                  ],
                  name: __MODULE__
                ]} =
                 prepare_for_start_module_opts(
                   goth: FakeAuth,
                   subscription: "projects/foo/subscriptions/bar",
                   on_failure: action
                 )

        assert producer_opts[:on_failure] == action
      end

      assert_raise ValidationError,
                   ~r/expected :on_failure to be one of :ack, :noop, :nack, or {:nack, integer} where integer is between 0 and 600, got: :foo/,
                   fn ->
                     prepare_for_start_module_opts(
                       goth: FakeAuth,
                       subscription: "projects/foo/subscriptions/bar",
                       on_failure: :foo
                     )
                   end

      assert_raise ValidationError,
                   ~r/expected :on_failure to be one of :ack, :noop, :nack, or {:nack, integer} where integer is between 0 and 600, got: "foo"/,
                   fn ->
                     prepare_for_start_module_opts(
                       goth: FakeAuth,
                       subscription: "projects/foo/subscriptions/bar",
                       on_failure: "foo"
                     )
                   end

      assert_raise ValidationError,
                   ~r/expected :on_failure to be one of :ack, :noop, :nack, or {:nack, integer} where integer is between 0 and 600, got: 1/,
                   fn ->
                     prepare_for_start_module_opts(
                       goth: FakeAuth,
                       subscription: "projects/foo/subscriptions/bar",
                       on_failure: 1
                     )
                   end

      assert_raise ValidationError,
                   ~r/expected :on_failure to be one of :ack, :noop, :nack, or {:nack, integer} where integer is between 0 and 600, got: SomeModule/,
                   fn ->
                     prepare_for_start_module_opts(
                       goth: FakeAuth,
                       subscription: "projects/foo/subscriptions/bar",
                       on_failure: SomeModule
                     )
                   end
    end

    test "custom action :nack casts to {:nack, 0}" do
      assert {_,
              [
                producer: [
                  module: {BroadwayCloudPubSub.Pull.Producer, producer_opts},
                  concurrency: 1
                ],
                name: __MODULE__
              ]} =
               prepare_for_start_module_opts(
                 goth: FakeAuth,
                 on_failure: :nack,
                 on_success: :nack,
                 subscription: "projects/foo/subscriptions/bar"
               )

      assert producer_opts[:on_success] == {:nack, 0}
      assert producer_opts[:on_failure] == {:nack, 0}
    end

    test "with :client PullClient returns a child_spec for starting a Finch pool" do
      assert {
               [
                 {Finch,
                  name: BroadwayCloudPubSub.Pull.ProducerTest.BroadwayCloudPubSub.Pull.FinchClient}
               ],
               [
                 producer: [
                   module: {BroadwayCloudPubSub.Pull.Producer, _producer_opts},
                   concurrency: 1
                 ],
                 name: __MODULE__
               ]
             } =
               prepare_for_start_module_opts(
                 goth: FakeAuth,
                 subscription: "projects/foo/subscriptions/bar"
               )
    end

    test "with :client PullClient and :finch returns empty specs" do
      assert {
               [],
               [
                 producer: [
                   module: {BroadwayCloudPubSub.Pull.Producer, producer_opts},
                   concurrency: 1
                 ],
                 name: __MODULE__
               ]
             } =
               prepare_for_start_module_opts(
                 goth: FakeAuth,
                 subscription: "projects/foo/subscriptions/bar",
                 finch: MyFinch
               )

      assert producer_opts[:finch] == MyFinch
    end
  end

  test "receive messages when the queue has less than the demand" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)

    MessageServer.push_messages(message_server, 1..5)

    assert_receive {:messages_received, 5}

    for msg <- 1..5 do
      assert_receive {:message_handled, ^msg}
    end

    stop_broadway(pid)
  end

  test "keep receiving messages when the queue has more than the demand" do
    {:ok, message_server} = MessageServer.start_link()
    MessageServer.push_messages(message_server, 1..20)
    {:ok, pid} = start_broadway(message_server)

    assert_receive {:messages_received, 10}

    for msg <- 1..10 do
      assert_receive {:message_handled, ^msg}
    end

    assert_receive {:messages_received, 5}

    for msg <- 11..15 do
      assert_receive {:message_handled, ^msg}
    end

    assert_receive {:messages_received, 5}

    for msg <- 16..20 do
      assert_receive {:message_handled, ^msg}
    end

    stop_broadway(pid)
  end

  test "keep trying to receive new messages when the queue is empty" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)

    MessageServer.push_messages(message_server, [13])
    assert_receive {:messages_received, 1}
    assert_receive {:message_handled, 13}

    refute_receive {:message_handled, _}

    MessageServer.push_messages(message_server, [14, 15])
    assert_receive {:messages_received, 2}
    assert_receive {:message_handled, 14}
    assert_receive {:message_handled, 15}

    stop_broadway(pid)
  end

  test "stop trying to receive new messages after start draining" do
    {:ok, message_server} = MessageServer.start_link()
    broadway_name = new_unique_name()
    {:ok, pid} = start_broadway(broadway_name, message_server)

    [producer] = Broadway.producer_names(broadway_name)

    Broadway.Topology.ProducerStage.drain(producer)
    :sys.get_state(producer)

    MessageServer.push_messages(message_server, [14, 15])

    refute_receive {:messages_received, _}, 10

    stop_broadway(pid)
  end

  test "delete acknowledged messages" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)

    MessageServer.push_messages(message_server, 1..20)

    assert_receive {:messages_deleted, 10}
    assert_receive {:messages_deleted, 10}

    stop_broadway(pid)
  end

  describe "calling Client.prepare_to_connect/2" do
    test "with default options" do
      {:ok, message_server} = MessageServer.start_link()
      broadway_name = new_unique_name()
      ref = make_ref()

      {:ok, pid} =
        start_broadway(broadway_name, message_server, FakePrepareToConnectClient,
          prepare_to_connect_ref: ref
        )

      assert_receive {:prepare_to_connect_spec, ^ref}, 500
      assert_receive {:prepare_to_connect_opts, ^ref}, 500

      stop_broadway(pid)
    end
  end

  test "passing extra options to client" do
    {:ok, message_server} = MessageServer.start_link()
    broadway_name = new_unique_name()

    {:ok, pid_1} =
      start_broadway(
        broadway_name,
        message_server,
        {FakeClient, max_retries: 3, retry_codes: [502, 503], retry_delay_ms: 300}
      )

    stop_broadway(pid_1)
  end

  test "support multiple topologies" do
    {:ok, message_server} = MessageServer.start_link()
    broadway_name = new_unique_name()
    {:ok, pid_1} = start_broadway(broadway_name, message_server, FakeClient)
    {:ok, message_server} = MessageServer.start_link()
    broadway_name = new_unique_name()
    {:ok, pid_2} = start_broadway(broadway_name, message_server, FakeClient)

    stop_broadway(pid_1)
    stop_broadway(pid_2)
  end

  defp start_broadway(
         broadway_name \\ new_unique_name(),
         message_server,
         client \\ FakeClient,
         opts \\ []
       ) do
    Broadway.start_link(
      Forwarder,
      build_broadway_opts(broadway_name, opts,
        client: client,
        goth: DefaultGothProducerOption,
        subscription: "projects/my-project/subscriptions/my-subscription",
        receive_interval: 0,
        test_pid: self(),
        message_server: message_server
      )
    )
  end

  defp build_broadway_opts(broadway_name, opts, producer_opts) do
    [
      name: broadway_name,
      context: %{test_pid: self()},
      producer: [
        module: {BroadwayCloudPubSub.Pull.Producer, Keyword.merge(producer_opts, opts)},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 1]
      ],
      batchers: [
        default: [
          batch_size: 10,
          batch_timeout: 50,
          concurrency: 1
        ]
      ]
    ]
  end

  defp new_unique_name() do
    :"Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  defp stop_broadway(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end
