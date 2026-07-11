defmodule Genswarms.Observer.Detectors.Restarted do
  @moduledoc """
  POSITIVE restart detection — the pod actually booted, not "unreachable,
  probably a restart".

  The signal is `feed_rehydrated`: the dashboard package's DisplayFeed emits
  it exactly once per host boot, after reloading the durable display rows
  (genswarms-dashboard `display_feed.ex` — `%{"kind" => "feed_rehydrated",
  "count" => rows}`). Until now the observer only INFERRED restarts from
  unreachability blips (`endpoint_down` + "swarm_not_found", the
  restart-*shaped* alert) — a fast rollout between two ticks was invisible,
  and the `unanswered` correlation ("their reply died with the old pod")
  missed exactly those. Hosts whose feed never carries the kind (micromarkets
  synthesizes its feed from the log store) simply never fire it.

  Two alerts:

  - `:swarm_restarted`, keyed `{swarm, :swarm_restarted}` — at least one FRESH
    boot this tick (younger than `"restart.fresh_window_s"`). The freshness
    gate matters: a newly registered swarm replays its whole ring into the
    session cursor, and alerting a three-day-old boot as "pod restarted" is
    noise. Evidence carries the boot count this tick and the latest boot's
    rehydrated row count.
  - `:restart_loop`, keyed `{swarm, :restart_loop}` — `>= "restart.loop_count"`
    boots inside `"restart.loop_window_s"`. One restart is a deploy; several
    in a short window is an incident. Raised only on a tick that also saw a
    fresh boot (news-driven; Scope's per-(swarm, type) cooldown handles the
    rest).

  STATEFUL, same discipline as `DeliveryFailureBurst`: the feed is
  incremental per session but `det` state is PERSISTED while the cursor is
  not, so an observer restart replays the host's ring — entries are deduped
  by the wire's `seq` (ts fallback) and pruned to the loop window, or a
  single real boot would re-alert on every observer restart.

  `feed` `:unavailable` / `{:error, _}` / absent is a no-op with prior state.
  """

  @behaviour Genswarms.Observer.Detector

  @impl true
  def default_thresholds,
    do: %{
      "restart.fresh_window_s" => 900,
      "restart.loop_count" => 3,
      "restart.loop_window_s" => 1800
    }

  @impl true
  def init, do: empty_state()

  @impl true
  def detect(fetched, ctx) do
    case fetched do
      %{feed: {:ok, events}} when is_list(events) ->
        fresh_window_ms = ctx.thresholds["restart.fresh_window_s"] * 1_000
        loop_count = ctx.thresholds["restart.loop_count"]
        loop_window_s = ctx.thresholds["restart.loop_window_s"]
        loop_window_ms = loop_window_s * 1_000

        prior = normalize_state(ctx.state)
        boots = Enum.filter(events, &(kind(&1) == "feed_rehydrated"))
        {state, new_entries} = ingest(prior, boots)
        state = prune(state, ctx.now_ms, max(loop_window_ms, fresh_window_ms))

        fresh = Enum.filter(new_entries, fn {_key, ms, _ev} -> ctx.now_ms - ms <= fresh_window_ms end)

        {alerts(fresh, state, ctx, loop_count, loop_window_s, loop_window_ms), state}

      # :unavailable / {:error, _} / no :feed key — no window, no verdict.
      _ ->
        {[], ctx.state}
    end
  end

  defp alerts([], _state, _ctx, _loop_count, _loop_window_s, _loop_window_ms), do: []

  defp alerts(fresh, state, ctx, loop_count, loop_window_s, loop_window_ms) do
    {_key, _ms, latest_ev} = Enum.max_by(fresh, fn {_key, ms, _ev} -> ms end)
    rows = rehydrated_rows(latest_ev)

    restarted = %{
      type: :swarm_restarted,
      swarm: ctx.swarm,
      at_ms: ctx.now_ms,
      summary:
        "pod restarted" <>
          if(length(fresh) > 1, do: " ×#{length(fresh)}", else: "") <>
          if(is_integer(rows), do: " (rehydrated #{rows} feed rows)", else: ""),
      evidence:
        %{"count" => length(fresh)}
        |> then(&if is_integer(rows), do: Map.put(&1, "rehydrated_rows", rows), else: &1),
      key: {ctx.swarm, :swarm_restarted}
    }

    in_loop_window =
      Enum.count(state.seen, fn {_key, ms} -> ctx.now_ms - ms <= loop_window_ms end)

    loop =
      if in_loop_window >= loop_count do
        [
          %{
            type: :restart_loop,
            swarm: ctx.swarm,
            at_ms: ctx.now_ms,
            summary: "#{in_loop_window} pod restarts in #{loop_window_s}s",
            evidence: %{"count" => in_loop_window, "window_s" => loop_window_s},
            key: {ctx.swarm, :restart_loop}
          }
        ]
      else
        []
      end

    [restarted | loop]
  end

  defp empty_state, do: %{seen: []}

  # Prior state may be nil (fresh runner default) or malformed (a poisoned
  # store entry) — restart clean rather than crash the tick (house F2 guard).
  defp normalize_state(%{seen: seen}) when is_list(seen) do
    %{seen: Enum.filter(seen, &match?({_key, ms} when is_integer(ms), &1))}
  end

  defp normalize_state(_), do: empty_state()

  # Returns {state, new_entries} — new_entries as {key, ms, event} so the
  # alert can decode the latest boot's evidence without re-scanning.
  defp ingest(state, boots) do
    Enum.reduce(boots, {state, []}, fn ev, {st, new} ->
      case event_ms(ev) do
        nil ->
          {st, new}

        ms ->
          key = dedupe_key(ev, ms)

          if List.keymember?(st.seen, key, 0) do
            {st, new}
          else
            {%{st | seen: [{key, ms} | st.seen]}, [{key, ms, ev} | new]}
          end
      end
    end)
  end

  defp dedupe_key(%{"seq" => seq}, _ms) when is_integer(seq), do: {:seq, seq}
  defp dedupe_key(_ev, ms), do: {:ts, ms}

  defp prune(state, now_ms, window_ms),
    do: %{state | seen: Enum.filter(state.seen, fn {_key, ms} -> now_ms - ms <= window_ms end)}

  defp rehydrated_rows(%{"count" => n}) when is_integer(n), do: n
  defp rehydrated_rows(_), do: nil

  # "kind" on both wires; no "event_type" fallback — see Unanswered.kind/1.
  defp kind(ev) when is_map(ev), do: ev["kind"]
  defp kind(_), do: nil

  # "ts" = float unix SECONDS on both reference wires (provenance:
  # detectors_ux_test.exs).
  defp event_ms(%{"ts" => ts}) when is_number(ts), do: round(ts * 1000)
  defp event_ms(_), do: nil
end
