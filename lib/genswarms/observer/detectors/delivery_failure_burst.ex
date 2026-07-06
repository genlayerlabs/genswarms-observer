defmodule Genswarms.Observer.Detectors.DeliveryFailureBurst do
  @moduledoc """
  Alerts on repeated delivery failures inside a rolling window:

  - Per-cid: `>= "delivery_failure.count"` `reply_sent` events with `"ok" =>
    false` for the SAME cid inside `"delivery_failure.window_s"` seconds ->
    `:delivery_failure_burst`, keyed `{swarm, :delivery_failure_burst, cid}`
    with `cids: [cid]` so the swarm-level cooldown (Scope) dedupes repeats
    across ticks instead of this detector tracking its own history.
  - Swarm-level: `>= "delivery_failure.count"` `reply_failed` events (no
    `cid` — a `reply_sent` NOT tied to any request) inside the same window
    -> `:reply_failed_burst`, no cids.

  A missing `"ok"` on `reply_sent` counts as delivered (does not count
  towards the burst). Either `"kind"` or `"event_type"` names the event.

  Stateless: recomputed fresh from the fetched event window every tick, so
  `ctx.state` passes through unchanged.
  """

  @behaviour Genswarms.Observer.Detector

  @impl true
  def default_thresholds,
    do: %{"delivery_failure.count" => 3, "delivery_failure.window_s" => 600}

  @impl true
  def init, do: nil

  @impl true
  def detect(fetched, ctx) do
    count_threshold = ctx.thresholds["delivery_failure.count"]
    window_s = ctx.thresholds["delivery_failure.window_s"]
    window_ms = window_s * 1_000

    recent =
      fetched
      |> events()
      |> Enum.filter(&within_window?(&1, ctx.now_ms, window_ms))

    cid_alerts = cid_burst_alerts(recent, ctx.swarm, ctx.now_ms, count_threshold, window_s)

    swarm_alerts =
      reply_failed_burst_alerts(recent, ctx.swarm, ctx.now_ms, count_threshold, window_s)

    {cid_alerts ++ swarm_alerts, ctx.state}
  end

  defp events(%{events: {:ok, events}}) when is_list(events), do: events
  defp events(_), do: []

  defp within_window?(ev, now_ms, window_ms) do
    case event_ms(ev) do
      nil -> false
      ms -> now_ms - ms <= window_ms
    end
  end

  defp cid_burst_alerts(events, swarm, now_ms, count_threshold, window_s) do
    events
    |> Enum.filter(&failed_reply_sent?/1)
    |> Enum.group_by(& &1["cid"])
    |> Enum.filter(fn {_cid, evs} -> length(evs) >= count_threshold end)
    |> Enum.map(fn {cid, evs} ->
      %{
        type: :delivery_failure_burst,
        swarm: swarm,
        at_ms: now_ms,
        summary: "#{length(evs)} failed reply deliveries for #{cid} in #{window_s}s",
        evidence: %{"cid" => cid, "count" => length(evs), "window_s" => window_s},
        key: {swarm, :delivery_failure_burst, cid},
        cids: [cid]
      }
    end)
  end

  defp failed_reply_sent?(ev),
    do: kind(ev) == "reply_sent" and is_binary(ev["cid"]) and Map.get(ev, "ok", true) == false

  defp reply_failed_burst_alerts(events, swarm, now_ms, count_threshold, window_s) do
    failures = Enum.filter(events, &(kind(&1) == "reply_failed"))

    if length(failures) >= count_threshold do
      [
        %{
          type: :reply_failed_burst,
          swarm: swarm,
          at_ms: now_ms,
          summary: "#{length(failures)} reply_failed events in #{window_s}s",
          evidence: %{"count" => length(failures), "window_s" => window_s}
        }
      ]
    else
      []
    end
  end

  defp kind(ev), do: ev["kind"] || ev["event_type"]

  defp event_ms(%{"timestamp" => ts}) when is_binary(ts) do
    case DateTime.from_iso8601(ts) do
      {:ok, dt, _offset} -> DateTime.to_unix(dt, :millisecond)
      _ -> nil
    end
  end

  defp event_ms(_), do: nil
end
