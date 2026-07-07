defmodule Genswarms.Observer.Detectors do
  @moduledoc """
  Deterministic health detectors over one observed swarm's dashboard data.

  PURE: no HTTP, no clock, no LLM. The caller (`Objects.Scope`) fetches the
  dashboard envelope and the events list, and folds this module per tick:

      {alerts, det_state} = Detectors.detect(swarm, data, thresholds, det_state, now_ms)

  `data` carries the raw fetch results so a transport failure is itself a
  signal (`endpoint_down`):

      %{dashboard: {:ok, envelope} | {:error, reason},
        events:    {:ok, [event]}  | {:error, reason}}

  Envelope and event shapes are the dashboard wire contract (pinned by the
  golden contract test in genswarms-dashboard/backend) — string keys, ISO8601
  timestamps.

  `det_state` is the detector's own memory (e.g. since-when the pool has been
  saturated). It is per-swarm, opaque to the caller, and threading it back in
  keeps the function pure. Start with `initial_state/0`.

  Dedupe/cooldown across ticks is NOT done here — that's Scope's job.

  Also implements `Genswarms.Observer.Detector`, so it can run under
  `DetectorRunner` alongside custom detectors — `detect/2` is a thin
  adapter onto the `detect/5` shape above, which stays the primary/tested
  entry point.
  """

  @behaviour Genswarms.Observer.Detector

  @default_thresholds %{
    "stall_minutes" => 10,
    "error_burst_count" => 5,
    "error_burst_window_s" => 60,
    "pool_saturated_s" => 120
  }

  @impl true
  def default_thresholds, do: @default_thresholds

  def initial_state, do: %{saturated_since_ms: nil}

  @impl true
  def init, do: initial_state()

  @doc """
  `Detector` callback adapter: delegates onto `detect/5` using the fields
  carried in `ctx` (see `Genswarms.Observer.Detector.ctx/0`).
  """
  @impl true
  def detect(fetched, ctx), do: detect(ctx.swarm, fetched, ctx.thresholds, ctx.state, ctx.now_ms)

  @doc """
  Runs every detector for one swarm. Returns `{alerts, det_state}`.

  Each alert: `%{type, swarm, at_ms, summary, evidence}` (atoms for `type`,
  the rest JSON-friendly).
  """
  def detect(swarm, data, thresholds, det_state \\ nil, now_ms) do
    thresholds = Map.merge(@default_thresholds, thresholds || %{})
    det_state = det_state || initial_state()

    case data do
      %{dashboard: {:error, reason}} ->
        alert =
          alert(:endpoint_down, swarm, now_ms, "dashboard fetch failed: #{inspect(reason)}", %{
            "reason" => inspect(reason)
          })

        # No dashboard -> nothing else is observable; reset sustained counters
        # so a recovering swarm doesn't instantly fire pool_saturated.
        {[alert], initial_state()}

      %{dashboard: {:ok, envelope}} = data ->
        events = elem_events(data)

        {pool_alerts, det_state} =
          pool_saturated(swarm, envelope, thresholds, det_state, now_ms)

        alerts =
          budget_block(swarm, events, now_ms) ++
            error_burst(swarm, events, thresholds, now_ms) ++
            stall(swarm, envelope, events, thresholds, now_ms) ++
            pool_alerts

        {alerts, det_state}
    end
  end

  defp elem_events(%{events: {:ok, events}}) when is_list(events), do: events
  defp elem_events(_), do: []

  # ── stall ────────────────────────────────────────────────────────────────
  # Active work but no engine events for stall_minutes: something is wedged.
  # "Active" = any leased pool slot or any agent node reported non-idle.

  defp stall(swarm, envelope, events, thresholds, now_ms) do
    window_ms = thresholds["stall_minutes"] * 60_000
    last_ms = newest_event_ms(events)

    cond do
      not active?(envelope) ->
        []

      last_ms == nil ->
        []

      now_ms - last_ms < window_ms ->
        []

      true ->
        silent_min = div(now_ms - last_ms, 60_000)

        [
          alert(:stall, swarm, now_ms, "active agents but no events for #{silent_min} min", %{
            "last_event_at_ms" => last_ms,
            "silent_minutes" => silent_min,
            "active" => active_evidence(envelope)
          })
        ]
    end
  end

  defp active?(envelope) do
    leased = get_in(envelope, ["summary", "pool", "leased"]) || 0

    busy_agents =
      envelope
      |> Map.get("nodes", [])
      |> Enum.any?(fn node ->
        node["type"] == "agent" and node["state"] not in [nil, "idle"]
      end)

    leased > 0 or busy_agents
  end

  defp active_evidence(envelope) do
    %{
      "leased" => get_in(envelope, ["summary", "pool", "leased"]) || 0,
      "busy_agents" =>
        envelope
        |> Map.get("nodes", [])
        |> Enum.filter(&(&1["type"] == "agent" and &1["state"] not in [nil, "idle"]))
        |> Enum.map(& &1["name"])
    }
  end

  # ── error_burst ──────────────────────────────────────────────────────────
  # >= error_burst_count error-level events inside error_burst_window_s.

  defp error_burst(swarm, events, thresholds, now_ms) do
    window_ms = thresholds["error_burst_window_s"] * 1_000
    count = thresholds["error_burst_count"]

    recent_errors =
      events
      |> Enum.filter(&(&1["level"] == "error"))
      |> Enum.filter(fn ev ->
        case event_ms(ev) do
          nil -> false
          ms -> now_ms - ms <= window_ms
        end
      end)

    if length(recent_errors) >= count do
      sample = recent_errors |> Enum.take(3) |> Enum.map(&Map.take(&1, ["event_type", "agent", "message"]))

      [
        alert(
          :error_burst,
          swarm,
          now_ms,
          "#{length(recent_errors)} error events in #{thresholds["error_burst_window_s"]}s",
          %{"count" => length(recent_errors), "sample" => sample}
        )
      ]
    else
      []
    end
  end

  # ── budget_block ─────────────────────────────────────────────────────────
  # The llm proxy reported a global budget block: everything downstream is
  # silently degraded. One sighting is enough.

  defp budget_block(swarm, events, now_ms) do
    case Enum.find(events, &budget_block_event?/1) do
      nil ->
        []

      ev ->
        [
          alert(:budget_block, swarm, now_ms, "llm proxy global budget block seen", %{
            "event" => Map.take(ev, ["event_type", "message", "timestamp", "agent"])
          })
        ]
    end
  end

  defp budget_block_event?(ev) do
    ev["event_type"] == "llm_proxy_global_block" or
      String.contains?(to_string(ev["message"] || ""), "llm_proxy_global_block")
  end

  # ── pool_saturated ───────────────────────────────────────────────────────
  # leased == size (> 0) sustained for pool_saturated_s: no slot headroom.
  # Sustainment lives in det_state (saturated_since_ms), threaded by the caller.

  defp pool_saturated(swarm, envelope, thresholds, det_state, now_ms) do
    pool = get_in(envelope, ["summary", "pool"]) || %{}
    size = pool["size"] || 0
    leased = pool["leased"] || 0
    saturated? = size > 0 and leased >= size

    cond do
      not saturated? ->
        {[], %{det_state | saturated_since_ms: nil}}

      det_state.saturated_since_ms == nil ->
        {[], %{det_state | saturated_since_ms: now_ms}}

      now_ms - det_state.saturated_since_ms >= thresholds["pool_saturated_s"] * 1_000 ->
        held_s = div(now_ms - det_state.saturated_since_ms, 1_000)

        {[
           alert(:pool_saturated, swarm, now_ms, "pool #{leased}/#{size} saturated for #{held_s}s", %{
             "leased" => leased,
             "size" => size,
             "saturated_for_s" => held_s
           })
         ], det_state}

      true ->
        {[], det_state}
    end
  end

  # ── helpers ──────────────────────────────────────────────────────────────

  defp alert(type, swarm, now_ms, summary, evidence) do
    %{type: type, swarm: swarm, at_ms: now_ms, summary: summary, evidence: evidence}
  end

  defp newest_event_ms(events) do
    events
    |> Enum.map(&event_ms/1)
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> nil
      list -> Enum.max(list)
    end
  end

  defp event_ms(%{"timestamp" => ts}) when is_binary(ts) do
    case DateTime.from_iso8601(ts) do
      {:ok, dt, _offset} -> DateTime.to_unix(dt, :millisecond)
      _ -> nil
    end
  end

  defp event_ms(_), do: nil
end
