defmodule BroadwayCloudPubSub.Streaming.Telemetry do
  @moduledoc false

  # Centralised telemetry helpers for the streaming Pub/Sub producer.
  #
  # All streaming telemetry events share the top-level prefix
  # `[:broadway_cloud_pub_sub, :streaming]`, then a sub-prefix identifying
  # the emitting layer:
  #
  #   :stream      — StreamManager (stream lifecycle, message flow, acks)
  #   :ack_batcher — AckBatcher (batch flush behaviour, retry exhaustion)
  #   :unary       — UnaryRpcClient (channel lifecycle, permanent failures)
  #   :grpc_client — GrpcClient (unary RPC spans for ack and modack)
  #
  # Usage:
  #
  #   Telemetry.execute(:stream, :connect, %{}, %{name: name, subscription: sub})
  #   Telemetry.span(:grpc_client, :ack, %{name: name, subscription: sub}, fn -> ... end)
  #
  #   # For async spans whose start and stop/exception are emitted separately:
  #   mono = Telemetry.emit_span_start(:stream, :drain, %{...}, config)
  #   Telemetry.emit_span_stop(:stream, :drain, mono, %{...}, config)
  #   Telemetry.emit_span_exception(:stream, :drain, mono, %{kind: ..., ...}, config)

  @base [:broadway_cloud_pub_sub, :streaming]

  @doc """
  Executes a telemetry event under `[:broadway_cloud_pub_sub, :streaming, layer, event]`.

  `telemetry_metadata` is the raw `:telemetry_metadata` option value from the producer
  config (a static term, an `{m, f, a}` tuple, or `nil`). When non-nil, its resolved
  value is merged into `metadata` under the `:extra` key.
  """
  @spec execute(atom(), atom(), map(), map(), term()) :: :ok
  def execute(layer, event, measurements, metadata, telemetry_metadata) do
    :telemetry.execute(
      @base ++ [layer, event],
      measurements,
      maybe_put_extra(metadata, resolve_extra(telemetry_metadata))
    )
  end

  @doc """
  Wraps `fun` in a telemetry span under `[:broadway_cloud_pub_sub, :streaming, layer, event]`.

  Emits `:start`, `:stop`, and `:exception` events as per `:telemetry.span/3` semantics.
  `fun` must return `{result, stop_metadata}`.

  `telemetry_metadata` is resolved once and merged into both the start metadata and the
  stop metadata returned by `fun`, under the `:extra` key.
  """
  @spec span(atom(), atom(), map(), (-> {term(), map()}), term()) :: term()
  def span(layer, event, start_metadata, fun, telemetry_metadata) do
    extra = resolve_extra(telemetry_metadata)
    enriched_start = maybe_put_extra(start_metadata, extra)

    :telemetry.span(@base ++ [layer, event], enriched_start, fn ->
      {result, stop_metadata} = fun.()
      {result, maybe_put_extra(stop_metadata, extra)}
    end)
  end

  @doc """
  Emits the `:start` event for an async span under
  `[:broadway_cloud_pub_sub, :streaming, layer, event, :start]`.

  Measurements follow `:telemetry.span/3` conventions:
  `%{system_time: System.system_time(), monotonic_time: monotonic_now}`.
  Any `extra_measurements` are merged into the measurements map.

  Returns the monotonic start time (nanoseconds) so the caller can compute
  `duration` when emitting the matching `:stop` or `:exception`.
  """
  @spec emit_span_start(atom(), atom(), map(), map(), term()) :: integer()
  def emit_span_start(layer, event, metadata, extra_measurements \\ %{}, telemetry_metadata) do
    now_mono = System.monotonic_time()

    measurements =
      Map.merge(
        %{system_time: System.system_time(), monotonic_time: now_mono},
        extra_measurements
      )

    :telemetry.execute(
      @base ++ [layer, event, :start],
      measurements,
      maybe_put_extra(metadata, resolve_extra(telemetry_metadata))
    )

    now_mono
  end

  @doc """
  Emits the `:stop` event for an async span under
  `[:broadway_cloud_pub_sub, :streaming, layer, event, :stop]`.

  `start_mono` must be the value returned by the matching `emit_span_start/5` call.
  `duration` is computed as `now - start_mono` in native time units.
  """
  @spec emit_span_stop(atom(), atom(), integer(), map(), term()) :: :ok
  def emit_span_stop(layer, event, start_mono, metadata, telemetry_metadata) do
    now_mono = System.monotonic_time()

    :telemetry.execute(
      @base ++ [layer, event, :stop],
      %{duration: now_mono - start_mono, monotonic_time: now_mono},
      maybe_put_extra(metadata, resolve_extra(telemetry_metadata))
    )
  end

  @doc """
  Emits the `:exception` event for an async span under
  `[:broadway_cloud_pub_sub, :streaming, layer, event, :exception]`.

  `start_mono` must be the value returned by the matching `emit_span_start/5` call.
  `duration` is computed as `now - start_mono` in native time units.
  Any `extra_measurements` are merged into the measurements map.
  """
  @spec emit_span_exception(atom(), atom(), integer(), map(), map(), term()) :: :ok
  def emit_span_exception(
        layer,
        event,
        start_mono,
        metadata,
        extra_measurements \\ %{},
        telemetry_metadata
      ) do
    now_mono = System.monotonic_time()

    measurements =
      Map.merge(
        %{duration: now_mono - start_mono, monotonic_time: now_mono},
        extra_measurements
      )

    :telemetry.execute(
      @base ++ [layer, event, :exception],
      measurements,
      maybe_put_extra(metadata, resolve_extra(telemetry_metadata))
    )
  end

  # --- Private ---

  defp resolve_extra(nil), do: nil
  defp resolve_extra({m, f, a}), do: apply(m, f, a)
  defp resolve_extra(term), do: term

  defp maybe_put_extra(metadata, nil), do: metadata
  defp maybe_put_extra(metadata, extra), do: Map.put(metadata, :extra, extra)
end
