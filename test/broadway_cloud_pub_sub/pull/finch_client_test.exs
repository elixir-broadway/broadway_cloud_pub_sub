defmodule BroadwayCloudPubSub.Pull.FinchClientTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias BroadwayCloudPubSub.Pull.Acknowledger
  alias BroadwayCloudPubSub.Pull.FinchClient
  alias BroadwayCloudPubSub.Test.TelemetryHelper
  alias Broadway.Message

  @pull_response """
  {
    "receivedMessages": [
      {
        "ackId": "1",
        "deliveryAttempt": 1,
        "message": {
          "data": "TWVzc2FnZTE=",
          "messageId": "19917247034",
          "attributes": {
            "foo": "bar",
            "qux": ""
          },
          "publishTime": "2014-02-14T00:00:01Z"
        }
      },
      {
        "ackId": "2",
        "deliveryAttempt": 2,
        "message": {
          "data": "TWVzc2FnZTI=",
          "messageId": "19917247035",
          "attributes": {},
          "publishTime": "2014-02-14T00:00:01Z"
        }
      },
      {
        "ackId": "3",
        "deliveryAttempt": 3,
        "message": {
          "data": null,
          "messageId": "19917247036",
          "attributes": {
            "number": "three"
          },
          "publishTime": "2014-02-14T00:00:02Z"
        }
      },
      {
        "ackId": "4",
        "message": {
          "data": null,
          "messageId": "19917247037",
          "attributes": {},
          "publishTime": null
        }
      }
    ]
  }
  """

  @empty_response """
  {}
  """

  @ordered_response """
  {
    "receivedMessages": [
      {
        "ackId": "1",
        "deliveryAttempt": 1,
        "message": {
          "data": "TWVzc2FnZTE=",
          "messageId": "19917247038",
          "publishTime": "2014-02-14T00:00:03Z",
          "orderingKey": "key1"
        }
      }
    ]
  }
  """

  @no_payload_response """
  {
    "receivedMessages": [
      {
        "ackId": "1",
        "message": {
          "attributes": {
            "payloadFormat": "NONE"
          },
          "messageId": "20240501001",
          "publishTime": "2024-05-01T13:07:41.716Z"
        }
      }
    ]
  }
  """

  setup do
    server = Bypass.open()
    base_url = "http://localhost:#{server.port}"

    finch = __MODULE__.Finch
    _ = start_supervised({Finch, name: finch})

    {:ok, server: server, base_url: base_url, finch: finch}
  end

  def on_pubsub_request(server, fun) when is_function(fun, 2) do
    test_pid = self()

    Bypass.expect(server, fn conn ->
      url = Plug.Conn.request_url(conn)
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      body = Jason.decode!(body)

      send(test_pid, {:http_request_called, %{url: url, body: body}})

      case fun.(url, body) do
        {:ok, resp_body} -> Plug.Conn.resp(conn, 200, resp_body)
        {:error, resp_body} -> Plug.Conn.resp(conn, 500, resp_body)
        {:error, status, resp_body} -> Plug.Conn.resp(conn, status, resp_body)
      end
    end)
  end

  def multiple_errors_on_pubsub(server, error_count: total_errors, error_status: error_status) do
    {:ok, agent} = Agent.start_link(fn -> 1 end)

    Bypass.expect(server, fn conn ->
      attempt = Agent.get_and_update(agent, fn num -> {num, num + 1} end)

      if attempt <= total_errors do
        Plug.Conn.resp(conn, error_status, @empty_response)
      else
        Agent.stop(agent)
        Plug.Conn.resp(conn, 200, @ordered_response)
      end
    end)
  end

  defp init_with_ack_builder(opts) do
    # mimics workflow from Producer.prepare_for_start/2
    ack_ref = opts[:broadway][:name]
    fill_persistent_term(ack_ref, opts)

    {:ok, config} = FinchClient.init(opts)
    {ack_ref, Acknowledger.builder(ack_ref), config}
  end

  describe "receive_messages/3" do
    setup %{server: server, base_url: base_url, finch: finch} do
      test_pid = self()

      on_pubsub_request(server, fn _url, _body ->
        {:ok, @pull_response}
      end)

      %{
        pid: test_pid,
        opts: [
          # will be injected by Broadway at runtime
          broadway: [name: :Broadway3],
          base_url: base_url,
          finch: finch,
          max_number_of_messages: 10,
          subscription: "projects/foo/subscriptions/bar",
          token_generator: {__MODULE__, :generate_token, []},
          receive_timeout: :infinity,
          max_retries: 0
        ]
      }
    end

    test "returns a list of ordered Broadway.Message with orderingKey in the :metadata", %{
      opts: base_opts,
      server: server
    } do
      on_pubsub_request(server, fn _url, _body ->
        {:ok, @ordered_response}
      end)

      {:ok, opts} = FinchClient.init(base_opts)

      assert [message] = FinchClient.receive_messages(10, & &1, opts)

      assert message.metadata.messageId == "19917247038"
      assert message.metadata.orderingKey == "key1"
    end

    test "retries if the option is set", %{
      opts: base_opts,
      server: server
    } do
      multiple_errors_on_pubsub(server, error_count: 3, error_status: 502)

      {:ok, opts} =
        base_opts
        |> Keyword.put(:max_retries, 3)
        |> Keyword.put(:retry_delay_ms, 0)
        |> Keyword.put(:retry_codes, [502])
        |> FinchClient.init()

      assert [_message] = FinchClient.receive_messages(10, & &1, opts)
    end

    test "returns a list of Broadway.Message when payloadFormat is NONE", %{
      opts: base_opts,
      server: server
    } do
      on_pubsub_request(server, fn _, _ ->
        {:ok, @no_payload_response}
      end)

      {:ok, opts} = FinchClient.init(base_opts)

      assert [message] = FinchClient.receive_messages(10, & &1, opts)
      assert message.metadata.messageId == "20240501001"
    end

    test "returns a list of Broadway.Message with :data and :metadata set", %{
      opts: base_opts
    } do
      {:ok, opts} = FinchClient.init(base_opts)

      [message1, message2, message3, message4] = FinchClient.receive_messages(10, & &1, opts)

      assert %Message{data: "Message1", metadata: %{publishTime: %DateTime{}}} = message1

      assert message1.metadata.messageId == "19917247034"
      assert message1.metadata.deliveryAttempt == 1

      assert %{
               "foo" => "bar",
               "qux" => ""
             } = message1.metadata.attributes

      assert message2.data == "Message2"
      assert message2.metadata.messageId == "19917247035"
      assert message2.metadata.attributes == %{}
      assert message2.metadata.deliveryAttempt == 2

      assert %Message{data: nil} = message3
      assert message3.metadata.deliveryAttempt == 3

      assert %{
               "number" => "three"
             } = message3.metadata.attributes

      assert message4.metadata.publishTime == nil
      assert message4.metadata.deliveryAttempt == nil
    end

    test "returns an empty list when an empty response is returned by the server", %{
      opts: base_opts,
      server: server
    } do
      on_pubsub_request(server, fn _, _ ->
        {:ok, @empty_response}
      end)

      {:ok, opts} = FinchClient.init(base_opts)

      assert [] == FinchClient.receive_messages(10, & &1, opts)
    end

    test "if the request fails, returns an empty list and log the error", %{
      opts: base_opts,
      server: server
    } do
      on_pubsub_request(server, fn _, _ -> {:error, 403, @empty_response} end)

      {:ok, opts} = FinchClient.init(base_opts)

      assert capture_log(fn ->
               assert FinchClient.receive_messages(10, & &1, opts) == []
             end) =~ "[error] Unable to fetch events from Cloud Pub/Sub - reason: "
    end

    test "send a projects.subscriptions.pull request with default options", %{opts: base_opts} do
      {:ok, opts} = FinchClient.init(base_opts)
      FinchClient.receive_messages(10, & &1, opts)

      assert_received {:http_request_called, %{body: body, url: url}}
      assert body == %{"maxMessages" => 10}
      assert url == base_opts[:base_url] <> "/v1/projects/foo/subscriptions/bar:pull"
    end

    test "request with custom :max_number_of_messages", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:max_number_of_messages, 5) |> FinchClient.init()
      FinchClient.receive_messages(10, & &1, opts)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert body["maxMessages"] == 5
    end

    test "exposes telemetry for pull requests", %{opts: base_opts} do
      test_pid = self()

      :telemetry.attach(
        :start_handler,
        [:broadway_cloud_pub_sub, :pull_client, :receive_messages, :start],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :start}
      )

      :telemetry.attach(
        :stop_handler,
        [:broadway_cloud_pub_sub, :pull_client, :receive_messages, :stop],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :stop}
      )

      {:ok, opts} = base_opts |> Keyword.put(:max_number_of_messages, 5) |> FinchClient.init()
      FinchClient.receive_messages(10, & &1, opts)

      assert_received {:start, _measurements, metadata}
      assert_received {:stop, measurements, _metadata}
      assert metadata.demand == 10
      assert metadata.max_messages == 5
      assert is_integer(measurements.duration)

      :telemetry.detach(:start_handler)
      :telemetry.detach(:stop_handler)
    end
  end

  describe "acknowledge/2" do
    setup %{server: server, base_url: base_url, finch: finch} do
      test_pid = self()

      on_pubsub_request(server, fn _, _ ->
        {:ok, @empty_response}
      end)

      %{
        pid: test_pid,
        opts: [
          # will be injected by Broadway at runtime
          broadway: [name: :Broadway3],
          base_url: base_url,
          finch: finch,
          max_number_of_messages: 10,
          subscription: "projects/foo/subscriptions/bar",
          token_generator: {__MODULE__, :generate_token, []},
          receive_timeout: :infinity,
          topology_name: Broadway3,
          max_retries: 0
        ]
      }
    end

    test "makes a projects.subscriptions.acknowledge request", %{opts: base_opts} do
      {:ok, opts} = FinchClient.init(base_opts)

      FinchClient.acknowledge(["1", "2", "3"], opts)

      assert_received {:http_request_called, %{body: body, url: url}}

      assert body == %{"ackIds" => ["1", "2", "3"]}
      base_url = base_opts[:base_url]
      assert url == base_url <> "/v1/projects/foo/subscriptions/bar:acknowledge"
    end

    test "if the request fails, returns :ok and logs an error", %{
      opts: base_opts,
      server: server
    } do
      on_pubsub_request(server, fn _, _ ->
        {:error, 503, @empty_response}
      end)

      {:ok, opts} = FinchClient.init(base_opts)

      assert capture_log(fn ->
               assert FinchClient.acknowledge(["1", "2"], opts) == :ok
             end) =~ "[error] Unable to acknowledge messages with Cloud Pub/Sub - reason: "
    end

    test "emits telemetry events", %{opts: base_opts} do
      test_pid = self()

      :telemetry.attach(
        :start_handler,
        [:broadway_cloud_pub_sub, :pull_client, :ack, :start],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :start}
      )

      :telemetry.attach(
        :stop_handler,
        [:broadway_cloud_pub_sub, :pull_client, :ack, :stop],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :stop}
      )

      {:ok, opts} = FinchClient.init(base_opts)

      FinchClient.acknowledge(["1", "2", "3"], opts)

      assert_received {:start, _measurements, metadata}
      assert metadata.name == Broadway3
      assert_received {:stop, measurements, metadata}
      assert is_integer(measurements.duration)
      assert metadata.name == Broadway3

      :telemetry.detach(:start_handler)
      :telemetry.detach(:stop_handler)
    end
  end

  describe "put_deadline/3" do
    setup %{server: server, base_url: base_url, finch: finch} do
      test_pid = self()

      on_pubsub_request(server, fn _, _ ->
        {:ok, @empty_response}
      end)

      %{
        pid: test_pid,
        opts: [
          # will be injected by Broadway at runtime
          broadway: [name: :Broadway3],
          base_url: base_url,
          finch: finch,
          max_number_of_messages: 10,
          subscription: "projects/foo/subscriptions/bar",
          token_generator: {__MODULE__, :generate_token, []},
          receive_timeout: :infinity,
          topology_name: Broadway3,
          max_retries: 0
        ]
      }
    end

    test "makes a projects.subscriptions.modifyAckDeadline request", %{
      opts: base_opts
    } do
      {:ok, opts} = FinchClient.init(base_opts)

      ack_ids = ["1", "2"]
      FinchClient.put_deadline(ack_ids, 30, opts)

      assert_received {:http_request_called, %{body: body, url: url}}
      assert body == %{"ackIds" => ack_ids, "ackDeadlineSeconds" => 30}

      assert url == base_opts[:base_url] <> "/v1/projects/foo/subscriptions/bar:modifyAckDeadline"
    end

    test "if the request fails, returns :ok and logs an error",
         %{opts: base_opts, server: server} do
      on_pubsub_request(server, fn _, _ ->
        {:error, 503, @empty_response}
      end)

      {:ok, opts} = FinchClient.init(base_opts)

      assert capture_log(fn ->
               assert FinchClient.put_deadline(["1", "2"], 60, opts) == :ok
             end) =~ "[error] Unable to put new ack deadline with Cloud Pub/Sub - reason: "
    end
  end

  describe "prepare_to_connect/2" do
    test "returns a child_spec for starting a Finch http pool " do
      {[pool_spec], opts} = FinchClient.prepare_to_connect(SomePipeline, [])
      assert pool_spec == {Finch, name: SomePipeline.BroadwayCloudPubSub.Pull.FinchClient}
      assert opts == [finch: SomePipeline.BroadwayCloudPubSub.Pull.FinchClient]
    end

    test "allows custom finch" do
      {specs, opts} = FinchClient.prepare_to_connect(SomePipeline, finch: Foo)

      assert specs == []
      assert opts == [finch: Foo]
    end
  end

  describe "integration with BroadwayCloudPubSub.Pull.Acknowledger" do
    setup %{server: server, base_url: base_url, finch: finch} do
      test_pid = self()

      on_pubsub_request(server, fn url, body ->
        action =
          url
          |> String.split(":")
          |> List.last()

        case action do
          "pull" ->
            send(test_pid, {:pull_dispatched, %{url: url, body: body}})
            {:ok, @pull_response}

          "acknowledge" ->
            %{"ackIds" => ack_ids} = body

            send(test_pid, {:acknowledge_dispatched, length(ack_ids), ack_ids})
            {:ok, @empty_response}

          "modifyAckDeadline" ->
            %{"ackIds" => ack_ids, "ackDeadlineSeconds" => deadline} = body

            send(
              test_pid,
              {:modack_dispatched, length(ack_ids), deadline}
            )

            {:ok, @empty_response}
        end
      end)

      {:ok,
       %{
         pid: test_pid,
         opts: [
           # will be injected by Broadway at runtime
           broadway: [name: :Broadway3],
           base_url: base_url,
           client: FinchClient,
           finch: finch,
           max_number_of_messages: 10,
           subscription: "projects/foo/subscriptions/bar",
           token_generator: {__MODULE__, :generate_token, []},
           receive_timeout: :infinity,
           toplogy_name: Broadway3
         ]
       }}
    end

    test "returns a list of Broadway.Message structs with ack builder", %{
      opts: base_opts
    } do
      {:ok, opts} = FinchClient.init(base_opts)

      [message1, message2, message3, message4] =
        FinchClient.receive_messages(10, &{:ack, &1}, opts)

      assert {:ack, _} = message1.acknowledger
      assert {:ack, _} = message2.acknowledger
      assert {:ack, _} = message3.acknowledger
      assert {:ack, _} = message4.acknowledger
    end

    test "with defaults successful messages are acknowledged, and failed messages are ignored", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} = init_with_ack_builder(base_opts)

      messages = FinchClient.receive_messages(10, builder, opts)

      {successful, failed} = Enum.split(messages, 1)

      Acknowledger.ack(ack_ref, successful, failed)

      assert_receive {:acknowledge_dispatched, 1, ["1"]}
    end

    test "when :on_success is :noop, acknowledgement is a no-op", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_success, :noop)
        |> init_with_ack_builder()

      [_, _, _, _] = messages = FinchClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, messages, [])

      refute_receive {:acknowledge_dispatched, 4, _}
    end

    test "when :on_success is :nack, dispatches modifyAckDeadline", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_success, :nack)
        |> init_with_ack_builder()

      [_, _, _, _] = messages = FinchClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, messages, [])

      assert_receive {:modack_dispatched, 4, 0}
      refute_receive {:acknowledge_dispatched, 4, _}
    end

    test "when :on_success is {:nack, integer}, dispatches modifyAckDeadline", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_success, {:nack, 300})
        |> init_with_ack_builder()

      [_, _, _, _] = messages = FinchClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, messages, [])

      assert_receive {:modack_dispatched, 4, 300}
      refute_receive {:acknowledge_dispatched, 4, _}
    end

    test "with default :on_failure, failed messages are ignored", %{opts: base_opts} do
      {ack_ref, builder, opts} = init_with_ack_builder(base_opts)

      [_, _, _, _] = messages = FinchClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, [], messages)

      refute_receive {:acknowledge_dispatched, 4, _}
    end

    test "when :on_failure is :nack, dispatches modifyAckDeadline", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_failure, :nack)
        |> init_with_ack_builder()

      [_, _, _, _] = messages = FinchClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, [], messages)

      assert_receive {:modack_dispatched, 4, 0}
    end

    test "when :on_failure is {:nack, integer}, dispatches modifyAckDeadline", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_failure, {:nack, 60})
        |> init_with_ack_builder()

      [_, _, _, _] = messages = FinchClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, [], messages)

      assert_receive {:modack_dispatched, 4, 60}
    end
  end

  def generate_token, do: {:ok, "token.#{System.os_time(:second)}"}

  defp fill_persistent_term(ack_ref, base_opts) do
    :persistent_term.put(ack_ref, %{
      base_url: Keyword.fetch!(base_opts, :base_url),
      client: FinchClient,
      finch: Keyword.fetch!(base_opts, :finch),
      on_failure: base_opts[:on_failure] || {:nack, 0},
      on_success: base_opts[:on_success] || :ack,
      subscription: "projects/test/subscriptions/test-subscription",
      token_generator: {__MODULE__, :generate_token, []},
      receive_timeout: base_opts[:receive_timeout] || :infinity,
      topology_name: Broadway3
    })
  end
end
