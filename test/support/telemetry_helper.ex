defmodule BroadwayCloudPubSub.Test.TelemetryHelper do
  @moduledoc """
  Shared telemetry handler for tests.

  Provides a named public function to use with `:telemetry.attach/4` instead of
  anonymous functions, avoiding the "local function" performance-penalty warning
  that telemetry emits when a handler cannot be resolved to an explicit module.

  ## Usage

      :telemetry.attach(
        handler_id,
        [:some, :event],
        &TelemetryHelper.handle_event_forward_test/4,
        %{pid: test_pid, msg: :my_tag}
      )

      assert_receive {:my_tag, _measurements, metadata}
  """

  @doc """
  Telemetry handler that forwards the event to a test process.

  Expects the handler config to be a map with:
    - `:pid`  — the test process pid to send the message to
    - `:msg`  — the atom tag to use as the first element of the sent tuple

  The test process receives `{msg, measurements, metadata}`.
  """
  def handle_event_forward_test(_event, measurements, metadata, %{pid: pid, msg: msg}) do
    send(pid, {msg, measurements, metadata})
  end
end
