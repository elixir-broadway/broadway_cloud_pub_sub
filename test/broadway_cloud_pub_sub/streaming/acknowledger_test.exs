defmodule BroadwayCloudPubSub.Streaming.AcknowledgerTest do
  use ExUnit.Case, async: true

  alias Broadway.Message
  alias BroadwayCloudPubSub.Streaming.Acknowledger

  # Stub StreamManager: just sends messages to the test process instead of
  # calling gRPC. We monkey-patch via :persistent_term — the ack_ref maps to
  # {stub_pid, config} where stub_pid is our test process.
  # (Alias: Acknowledger refers to BroadwayCloudPubSub.Streaming.Acknowledger)
  #
  # Acknowledger calls:
  #   StreamManager.acknowledge(pid, ack_ids)   → GenServer.cast({:acknowledge, ack_ids})
  #   StreamManager.modify_deadline(pid, ack_ids, deadline) → GenServer.cast(...)
  #
  # We spin up a tiny GenServer that forwards casts to the test process.

  defmodule StubManager do
    use GenServer

    def start_link(test_pid) do
      GenServer.start_link(__MODULE__, test_pid)
    end

    def init(test_pid), do: {:ok, test_pid}

    def handle_cast({:acknowledge, ack_ids}, test_pid) do
      send(test_pid, {:acknowledge, ack_ids})
      {:noreply, test_pid}
    end

    def handle_cast({:modify_deadline, ack_ids, deadline}, test_pid) do
      send(test_pid, {:modify_deadline, ack_ids, deadline})
      {:noreply, test_pid}
    end
  end

  setup do
    {:ok, stub_pid} = StubManager.start_link(self())
    ack_ref = make_ref()

    config = %{on_success: :ack, on_failure: {:nack, 0}}
    :persistent_term.put(ack_ref, {stub_pid, config})

    on_exit(fn -> :persistent_term.erase(ack_ref) end)

    {:ok, ack_ref: ack_ref, stub_pid: stub_pid}
  end

  defp build_message(ack_id, ack_ref, overrides \\ %{}) do
    base = %{ack_id: ack_id}
    ack_data = Map.merge(base, overrides)
    %Message{data: "data_#{ack_id}", acknowledger: {Acknowledger, ack_ref, ack_data}}
  end

  describe "builder/1" do
    test "returns a function that builds acknowledger tuples", %{ack_ref: ack_ref} do
      builder = Acknowledger.builder(ack_ref)
      {mod, ref, data} = builder.("ack-123")

      assert mod == Acknowledger
      assert ref == ack_ref
      assert data == %{ack_id: "ack-123"}
    end
  end

  describe "configure/3" do
    test "raises on unknown option", %{ack_ref: ack_ref} do
      assert_raise NimbleOptions.ValidationError, ~r/unknown options/, fn ->
        Acknowledger.configure(ack_ref, %{ack_id: "x"}, on_other: :ack)
      end
    end

    test "merges on_success into ack_data", %{ack_ref: ack_ref} do
      {:ok, data} = Acknowledger.configure(ack_ref, %{ack_id: "x"}, on_success: :noop)
      assert data == %{ack_id: "x", on_success: :noop, on_failure: {:nack, 0}}
    end

    test "normalises :nack on_failure to {:nack, 0}", %{ack_ref: ack_ref} do
      {:ok, data} = Acknowledger.configure(ack_ref, %{ack_id: "x"}, on_failure: :nack)
      assert data.on_failure == {:nack, 0}
    end

    test "accepts {:nack, N} on_failure", %{ack_ref: ack_ref} do
      {:ok, data} =
        Acknowledger.configure(ack_ref, %{ack_id: "x"}, on_failure: {:nack, 60})

      assert data.on_failure == {:nack, 60}
    end
  end

  describe "ack/3 — success path" do
    test "acks successful messages by default", %{ack_ref: ack_ref} do
      msgs = [build_message("id-1", ack_ref), build_message("id-2", ack_ref)]
      Acknowledger.ack(ack_ref, msgs, [])

      assert_receive {:acknowledge, ack_ids}
      assert Enum.sort(ack_ids) == ["id-1", "id-2"]
    end

    test "does not ack failed messages when on_failure is :noop", %{
      ack_ref: ack_ref,
      stub_pid: stub_pid
    } do
      :persistent_term.put(ack_ref, {stub_pid, %{on_success: :ack, on_failure: :noop}})
      success = [build_message("ok-1", ack_ref)]
      failure = [build_message("fail-1", ack_ref)]

      Acknowledger.ack(ack_ref, success, failure)

      assert_receive {:acknowledge, ["ok-1"]}
      refute_receive {:acknowledge, _}
      refute_receive {:modify_deadline, _, _}
    end

    test "nacks failed messages by default (on_failure: {:nack, 0})", %{ack_ref: ack_ref} do
      success = [build_message("ok-1", ack_ref)]
      failure = [build_message("fail-1", ack_ref)]

      Acknowledger.ack(ack_ref, success, failure)

      assert_receive {:acknowledge, ["ok-1"]}
      assert_receive {:modify_deadline, ["fail-1"], 0}
    end

    test "does not send anything when on_success is :noop", %{
      ack_ref: ack_ref,
      stub_pid: stub_pid
    } do
      :persistent_term.put(ack_ref, {stub_pid, %{on_success: :noop, on_failure: :noop}})
      msgs = [build_message("id-1", ack_ref)]
      Acknowledger.ack(ack_ref, msgs, [])

      refute_receive {:acknowledge, _}
      refute_receive {:modify_deadline, _, _}
    end
  end

  describe "ack/3 — failure path" do
    test "nacks failed messages when on_failure is :nack", %{ack_ref: ack_ref, stub_pid: stub_pid} do
      :persistent_term.put(ack_ref, {stub_pid, %{on_success: :ack, on_failure: {:nack, 0}}})
      failure = [build_message("fail-1", ack_ref), build_message("fail-2", ack_ref)]

      Acknowledger.ack(ack_ref, [], failure)

      assert_receive {:modify_deadline, ack_ids, 0}
      assert Enum.sort(ack_ids) == ["fail-1", "fail-2"]
    end

    test "nacks with custom deadline when on_failure is {:nack, N}", %{
      ack_ref: ack_ref,
      stub_pid: stub_pid
    } do
      :persistent_term.put(ack_ref, {stub_pid, %{on_success: :ack, on_failure: {:nack, 30}}})
      failure = [build_message("fail-1", ack_ref)]

      Acknowledger.ack(ack_ref, [], failure)

      assert_receive {:modify_deadline, ["fail-1"], 30}
    end

    test "acks failed messages when on_failure is :ack", %{ack_ref: ack_ref, stub_pid: stub_pid} do
      :persistent_term.put(ack_ref, {stub_pid, %{on_success: :noop, on_failure: :ack}})
      failure = [build_message("fail-1", ack_ref)]

      Acknowledger.ack(ack_ref, [], failure)

      assert_receive {:acknowledge, ["fail-1"]}
    end
  end

  describe "ack/3 — per-message overrides" do
    test "respects per-message on_success override", %{ack_ref: ack_ref} do
      noop_msg = build_message("noop-id", ack_ref, %{on_success: :noop})
      ack_msg = build_message("ack-id", ack_ref)

      Acknowledger.ack(ack_ref, [noop_msg, ack_msg], [])

      assert_receive {:acknowledge, ["ack-id"]}
      # noop-id must not appear in any acknowledge message
      refute_receive {:acknowledge, _}
    end

    test "respects per-message on_failure override", %{ack_ref: ack_ref} do
      nack_msg = build_message("nack-id", ack_ref, %{on_failure: {:nack, 10}})
      noop_msg = build_message("noop-id", ack_ref, %{on_failure: :noop})

      Acknowledger.ack(ack_ref, [], [nack_msg, noop_msg])

      assert_receive {:modify_deadline, ["nack-id"], 10}
      refute_receive {:modify_deadline, _, _}
    end
  end

  describe "ack/3 — batching" do
    test "chunks ack_ids at #{Acknowledger |> inspect()}'s @max_ack_ids_per_request" do
      # Acknowledger chunks at 2500
      ack_ref = make_ref()
      {:ok, stub_pid} = StubManager.start_link(self())
      :persistent_term.put(ack_ref, {stub_pid, %{on_success: :ack, on_failure: :noop}})

      on_exit(fn -> :persistent_term.erase(ack_ref) end)

      msgs = Enum.map(1..3000, &build_message("id-#{&1}", ack_ref))
      Acknowledger.ack(ack_ref, msgs, [])

      assert_receive {:acknowledge, batch1}
      assert_receive {:acknowledge, batch2}
      assert length(batch1) + length(batch2) == 3000
      assert length(batch1) == 2500
      assert length(batch2) == 500
    end
  end

  describe "ack_id_from/1" do
    test "extracts ack_id from a Broadway.Message" do
      ack_ref = make_ref()
      msg = build_message("test-ack-id", ack_ref)

      assert Acknowledger.ack_id_from(msg) == "test-ack-id"
    end

    test "works with different ack_ids" do
      ack_ref = make_ref()

      for id <- ["a", "b", "long-ack-id-with-many-chars"] do
        msg = build_message(id, ack_ref)
        assert Acknowledger.ack_id_from(msg) == id
      end
    end
  end
end
