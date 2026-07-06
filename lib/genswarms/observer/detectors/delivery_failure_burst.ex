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

  Consumes `fetched.feed` (`GET /api/swarms/:name/events/feed`) — the only
  surface carrying `reply_sent`/`reply_failed`. Real wire shape (identical
  on both known hosts; full provenance in detectors_ux_test.exs):
  `%{"kind" => k, "cid" => c?, "ok" => bool?, "ts" => float unix SECONDS}` —
  wingston `objects/event_feed.ex:164-177` (+ registry `:38-39`),
  micromarkets `dashboard/feed/event_feed.ex:317-329` (`"ts"` via `:479-480`).

  A missing `"ok"` on `reply_sent` counts as delivered (does not count
  towards the burst). `"kind"` names the event on both wires.

  `feed` `:unavailable` / `{:error, _}` / absent is a no-op with prior
  state — no window, no verdict.

  Stateless: recomputed fresh from the fetched event window every tick, so
  `ctx.state` passes through unchanged. Window filtering is count-based and
  order-insensitive, so unlike `Unanswered` no defensive sort is needed.
  """

  @behaviour Genswarms.Observer.Detector

  @impl true
  def default_thresholds,
    do: %{"delivery_failure.count" => 3, "delivery_failure.window_s" => 600}

  @impl true
  def init, do: nil

  @impl true
  def detect(fetched, ctx) do
    case fetched do
      %{feed: {:ok, events}} when is_list(events) ->
        count_threshold = ctx.thresholds["delivery_failure.count"]
        window_s = ctx.thresholds["delivery_failure.window_s"]
        window_ms = window_s * 1_000

        recent = Enum.filter(events, &within_window?(&1, ctx.now_ms, window_ms))

        cid_alerts = cid_burst_alerts(recent, ctx.swarm, ctx.now_ms, count_threshold, window_s)

        swarm_alerts =
          reply_failed_burst_alerts(recent, ctx.swarm, ctx.now_ms, count_threshold, window_s)

        {cid_alerts ++ swarm_alerts, ctx.state}

      # :unavailable / {:error, _} / no :feed key — no window, no verdict.
      _ ->
        {[], ctx.state}
    end
  end

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

  # "kind" on both wires; no "event_type" fallback — see Unanswered.kind/1.
  defp kind(ev) when is_map(ev), do: ev["kind"]
  defp kind(_), do: nil

  # "ts" = float unix SECONDS on both wires (wingston event_feed.ex:171,
  # micromarkets event_feed.ex:479-480).
  defp event_ms(%{"ts" => ts}) when is_number(ts), do: round(ts * 1000)
  defp event_ms(_), do: nil
end
