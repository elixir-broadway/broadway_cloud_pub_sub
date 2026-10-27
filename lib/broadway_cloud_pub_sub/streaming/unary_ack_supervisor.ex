defmodule BroadwayCloudPubSub.Streaming.UnaryAckSupervisor do
  @moduledoc false

  # Supervisor that owns the AckBatcher and UnaryRpcClient for a single Broadway
  # BroadwayCloudPubSub.Producer pipeline.
  #
  # Uses :one_for_one so each child restarts independently. AckBatcher accumulates
  # pending ack_ids in its state; restarting it when UnaryRpcClient crashes would
  # permanently lose those buffered acks (messages would be redelivered by the
  # server after deadline expiry, causing duplicate processing).
  #
  # AckBatcher references UnaryRpcClient by its registered name derived from the
  # Broadway pipeline name, so it survives UnaryRpcClient restarts automatically.
  # Any flush attempted while UnaryRpcClient is down is deferred to the next
  # timer tick.
  #
  # Started by prepare_for_start/2 as a Broadway supervisor child before
  # StreamManager, guaranteeing the batcher and RPC client are available when
  # StreamManager first processes ack requests.

  use Supervisor

  alias BroadwayCloudPubSub.Streaming.{AckBatcher, UnaryRpcClient}

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)

    if name do
      Supervisor.start_link(__MODULE__, opts, name: name)
    else
      Supervisor.start_link(__MODULE__, opts)
    end
  end

  @impl Supervisor
  def init(config) do
    broadway_name = Keyword.fetch!(config, :broadway_name)

    rpc_client_name = Module.concat(broadway_name, UnaryRpcClient)
    batcher_name = Module.concat(broadway_name, AckBatcher)
    task_sup_name = Module.concat(broadway_name, ReceiptModackTaskSupervisor)

    # Each child's child_opts/1 selects only the keys it needs from the
    # full config and validates that all required keys are present.
    rpc_client_opts =
      config
      |> UnaryRpcClient.child_opts()
      |> Keyword.put(:name, rpc_client_name)

    batcher_opts =
      config
      |> Keyword.put(:rpc_client, rpc_client_name)
      |> Keyword.put(:task_supervisor, task_sup_name)
      |> AckBatcher.child_opts()
      |> Keyword.put(:name, batcher_name)

    children = [
      %{
        id: UnaryRpcClient,
        start: {UnaryRpcClient, :start_link, [rpc_client_opts]},
        restart: :permanent
      },
      # Task.Supervisor for receipt modack RPCs (exactly-once delivery).
      # Started before AckBatcher so it's available when AckBatcher spawns tasks.
      # Tasks are :temporary — they run once and are not restarted on failure.
      {Task.Supervisor, name: task_sup_name},
      %{
        id: AckBatcher,
        start: {AckBatcher, :start_link, [batcher_opts]},
        restart: :permanent,
        # AckBatcher.flush/1 uses GenServer.call with a 15_000ms timeout.
        # The OTP default shutdown of 5_000ms would kill AckBatcher before
        # an in-progress flush can complete, dropping buffered acks.
        shutdown: 20_000
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
