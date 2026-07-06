defmodule Genswarms.Observer.Detectors.DeliveryFailureBurst do
  @moduledoc """
  Alerts on repeated delivery failures inside a rolling window:

  - Per-cid: `>= "delivery_failure.count"` `reply_sent` events with `"ok" =>
    false` for the SAME cid inside `"delivery_failure.window_s"` seconds ->
    `:delivery_failure_burst`, keyed `{swarm, :delivery_failure_burst, cid}`
    with `cids: [cid]` so the swarm-level cooldown (Scope) dedupes repeats
    across ticks.
  - Swarm-level: `>= "delivery_failure.count"` `reply_failed` events inside
    the same window -> `:reply_failed_burst`, no cids. On wingston a
    `reply_failed` never carries a cid (the target could not be resolved);
    on micromarkets it sometimes does — either way it is counted at swarm
    level and any cid is ignored.

  Consumes `fetched.feed` (`GET /api/swarms/:name/events/feed`) — the only
  surface carrying `reply_sent`/`reply_failed`. Real wire shape (identical
  on both known hosts; full provenance in detectors_ux_test.exs):
  `%{"kind" => k, "cid" => c?, "ok" => bool?, "ts" => float unix SECONDS}` —
  wingston `objects/event_feed.ex:164-177` (+ registry `:38-39`),
  micromarkets `dashboard/feed/event_feed.ex:317-329` (`"ts"` via `:479-480`).

  A missing `"ok"` on `reply_sent` counts as delivered (does not count
  towards the burst). `"kind"` names the event on both wires. Events with
  a non-number `"ts"` are skipped — they cannot participate in a time
  window.

  STATEFUL — the feed is INCREMENTAL: Scope's session cursor advances every
  tick, so each `detect/2` sees only the events since the last tick. A
  stateless recompute would be blind to any burst spread across ticks
  (3 failures one minute apart, ticked minutely, would never fire). So the
  detector accumulates failure timestamps in `ctx.state` and prunes them to
  the alert window every tick:

      %{fail_ts: %{cid => [ts_ms]}, reply_failed_ts: [ts_ms]}

  Pruning to exactly the window bounds state growth (a failure ages out at
  the alert horizon — beyond it, it can never contribute to an alert), and
  cids whose list empties are dropped entirely (no unbounded cid
  accumulation). Appends are deduped by exact `ts_ms` per list: `det` state
  is PERSISTED across observer restarts (Scope's store) while the feed
  cursor is session-local, so a restart replays the host's ring into state
  that already counted those failures — without the dedupe, 2 real failures
  would replay into 4 and cross the threshold falsely.

  `feed` `:unavailable` / `{:error, _}` / absent is a no-op with prior
  state — no window, no verdict, nothing ages out (the missed window is
  re-read next tick: Scope leaves the cursor untouched on those answers).
  """

  @behaviour Genswarms.Observer.Detector

  @impl true
  def default_thresholds,
    do: %{"delivery_failure.count" => 3, "delivery_failure.window_s" => 600}

  @impl true
  def init, do: empty_state()

  @impl true
  def detect(fetched, ctx) do
    case fetched do
      %{feed: {:ok, events}} when is_list(events) ->
        count_threshold = ctx.thresholds["delivery_failure.count"]
        window_s = ctx.thresholds["delivery_failure.window_s"]
        window_ms = window_s * 1_000

        state =
          ctx.state
          |> normalize_state()
          |> ingest(events)
          |> prune(ctx.now_ms, window_ms)

        cid_alerts = cid_burst_alerts(state, ctx.swarm, ctx.now_ms, count_threshold, window_s)

        swarm_alerts =
          reply_failed_burst_alerts(state, ctx.swarm, ctx.now_ms, count_threshold, window_s)

        {cid_alerts ++ swarm_alerts, state}

      # :unavailable / {:error, _} / no :feed key — no window, no verdict.
      _ ->
        {[], ctx.state}
    end
  end

  defp empty_state, do: %{fail_ts: %{}, reply_failed_ts: []}

  # Prior state may be nil (pre-stateful init, or a fresh DetectorRunner
  # default) or malformed (a poisoned store entry) — restart clean rather
  # than crash the tick.
  defp normalize_state(%{fail_ts: fail_ts, reply_failed_ts: rf})
       when is_map(fail_ts) and is_list(rf) do
    %{
      fail_ts: Map.new(fail_ts, fn {cid, list} -> {cid, migrate_entries(list)} end),
      reply_failed_ts: migrate_entries(rf)
    }
  end

  defp normalize_state(_), do: empty_state()

  # Old persisted format: bare ms integers. New: {dedupe_key, ms}. Old
  # entries keep ts-keyed dedupe (exactly the old behavior) until they age
  # out of the window.
  defp migrate_entries(list) when is_list(list) do
    Enum.flat_map(list, fn
      {_key, ms} = entry when is_integer(ms) -> [entry]
      ms when is_integer(ms) -> [{{:ts, ms}, ms}]
      _ -> []
    end)
  end

  defp migrate_entries(_), do: []

  defp ingest(state, events) do
    Enum.reduce(events, state, fn ev, st ->
      case event_ms(ev) do
        nil ->
          st

        ms ->
          entry = {dedupe_key(ev, ms), ms}

          cond do
            failed_reply_sent?(ev) ->
              cid = ev["cid"]
              update_in(st.fail_ts[cid], &append_dedup(&1, entry))

            kind(ev) == "reply_failed" ->
              %{st | reply_failed_ts: append_dedup(st.reply_failed_ts, entry)}

            true ->
              st
          end
      end
    end)
  end

  # F9: dedupe by the wire's seq (unique + monotonic on both known wires:
  # wingston event_feed.ex stamps seq on every display event, micromarkets
  # base/2 ditto) — exact-ts collapsed two DISTINCT same-millisecond
  # reply_faileds into one. ts remains the fallback key for a seq-less host.
  defp dedupe_key(%{"seq" => seq}, _ms) when is_integer(seq), do: {:seq, seq}
  defp dedupe_key(_ev, ms), do: {:ts, ms}

  defp append_dedup(nil, entry), do: [entry]

  defp append_dedup(list, {key, ms} = entry) do
    cond do
      List.keymember?(list, key, 0) ->
        list

      # Restart-over-old-store safety: a legacy migrated entry ({:ts, ms})
      # and a seq-keyed replay of the SAME event carry the same ms —
      # supersede the legacy entry instead of double-counting. Ambiguity is
      # resolved conservatively (a distinct new same-ms event would also
      # supersede): exactly the old exact-ts semantics until legacy entries
      # age out of the window.
      match?({:seq, _}, key) and List.keymember?(list, {:ts, ms}, 0) ->
        List.keyreplace(list, {:ts, ms}, 0, entry)

      true ->
        [entry | list]
    end
  end

  defp prune(state, now_ms, window_ms) do
    fail_ts =
      state.fail_ts
      |> Enum.flat_map(fn {cid, ts_list} ->
        case prune_list(ts_list, now_ms, window_ms) do
          [] -> []
          kept -> [{cid, kept}]
        end
      end)
      |> Map.new()

    %{
      state
      | fail_ts: fail_ts,
        reply_failed_ts: prune_list(state.reply_failed_ts, now_ms, window_ms)
    }
  end

  defp prune_list(entries, now_ms, window_ms),
    do: Enum.filter(entries, fn {_key, ms} -> now_ms - ms <= window_ms end)

  defp cid_burst_alerts(state, swarm, now_ms, count_threshold, window_s) do
    state.fail_ts
    |> Enum.filter(fn {_cid, ts_list} -> length(ts_list) >= count_threshold end)
    |> Enum.map(fn {cid, ts_list} ->
      %{
        type: :delivery_failure_burst,
        swarm: swarm,
        at_ms: now_ms,
        summary: "#{length(ts_list)} failed reply deliveries for #{cid} in #{window_s}s",
        evidence: %{"cid" => cid, "count" => length(ts_list), "window_s" => window_s},
        key: {swarm, :delivery_failure_burst, cid},
        cids: [cid]
      }
    end)
  end

  defp failed_reply_sent?(ev),
    do: kind(ev) == "reply_sent" and is_binary(ev["cid"]) and Map.get(ev, "ok", true) == false

  defp reply_failed_burst_alerts(state, swarm, now_ms, count_threshold, window_s) do
    count = length(state.reply_failed_ts)

    if count >= count_threshold do
      [
        %{
          type: :reply_failed_burst,
          swarm: swarm,
          at_ms: now_ms,
          summary: "#{count} reply_failed events in #{window_s}s",
          evidence: %{"count" => count, "window_s" => window_s}
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
