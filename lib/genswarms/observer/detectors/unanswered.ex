defmodule Genswarms.Observer.Detectors.Unanswered do
  @moduledoc """
  Alerts when a `request_open` has no matching ok `reply_sent` within
  `"unanswered.minutes"`.

  Consumes `fetched.feed` — the DISPLAY event feed
  (`GET /api/swarms/:name/events/feed`), the only surface that carries the
  `request_open`/`reply_sent` vocabulary. NOT `fetched.events` (engine-raw
  LogStore), which the legacy health detectors keep.

  Real wire event shape, identical across both known hosts (see the
  provenance block in detectors_ux_test.exs):

  - wingston `objects/event_feed.ex:164-177` — `%{kind, cid, ok?, seq, ts}`
    with `ts = System.system_time(:millisecond) / 1000`; string keys after
    the JSON hop.
  - micromarkets `dashboard/feed/event_feed.ex:317-329` (`base/2`) —
    `%{"kind", "cid", "ok"?, "seq", "ts", ...log-store extras}` with `"ts"`
    from `unix_ts/1` (`:479-480`).

  So on our side: `%{"kind" => "request_open", "cid" => c, "ts" => secs}`,
  `%{"kind" => "reply_sent", "cid" => c, "ok" => bool, "ts" => secs}` —
  `"ts"` is FLOAT UNIX SECONDS on both wires (no per-swarm divergence).
  A missing `"ok"` on `reply_sent` counts as delivered (`true`).

  `feed` `:unavailable` / `{:error, _}` / absent is a NO-OP with prior
  state: without the window we cannot know whether replies happened, and a
  false `:unanswered` on an answered pair is worse than a late one.

  State: `%{cid => %{opened_ms: integer, alerted: bool}}`. Events may arrive
  as an overlapping window across ticks — `request_open` only opens a cid
  the FIRST time it's seen (`Map.put_new/3`), so a re-delivered open in a
  later window neither resets the clock nor causes a second alert. An ok
  reply clears the cid entirely (a later re-open starts fresh). Once alerted,
  a still-open cid is not re-alerted while it remains open. The feed cursor
  itself is Scope's (session-local): after a restart the ring replays
  ascending from 0 and every answered open/reply pair cancels out here.
  """

  @behaviour Genswarms.Observer.Detector

  # Once alerted, a cid that's never replied to stays in state forever
  # (the "reply clears it" path never fires) — evict it after this long so
  # state doesn't grow unbounded.
  @alerted_ttl_ms 24 * 60 * 60 * 1000

  @impl true
  def default_thresholds, do: %{"unanswered.minutes" => 15}

  @impl true
  def init, do: %{}

  @impl true
  def detect(fetched, ctx) do
    case fetched do
      %{feed: {:ok, events}} when is_list(events) ->
        minutes = ctx.thresholds["unanswered.minutes"]
        tracked = apply_events(sort_by_ts(events), normalize_state(ctx.state))

        {alerts, new_state} = scan(tracked, ctx.swarm, minutes, ctx.now_ms)

        {alerts, prune_stale_alerted(new_state, ctx.now_ms)}

      # :unavailable / {:error, _} / no :feed key at all — no window, no
      # verdict: no-op with prior state (see moduledoc).
      _ ->
        {[], ctx.state}
    end
  end

  # The EventsSource contract guarantees oldest-first ascending (wingston
  # vendor/genswarms-dashboard/backend README §EventsSource: "Events with
  # seq > since, oldest first"), so the fold below is order-correct BY
  # CONTRACT. This sort is cheap insurance against a non-compliant host:
  # folding a reply BEFORE its open would delete a not-yet-tracked cid and
  # then false-alert the answered pair. Malformed-ts events are DROPPED
  # here, not coerced (`|| 0` would sort them before every valid event,
  # defeating the very order this insures): an event without a numeric ts
  # cannot participate in time-based tracking at all — it neither opens a
  # cid (the fold already skipped that) nor clears one (an untimed "ok
  # reply" from a corrupt host is as suspect as its missing ts; both real
  # wires stamp "ts" unconditionally, so only a malformed host is affected).
  defp sort_by_ts(events) do
    events
    |> Enum.filter(&event_ms/1)
    |> Enum.sort_by(&event_ms/1)
  end

  # F2 guard: state is a map of cid => %{opened_ms:, alerted:}; a poisoned
  # store entry (any other shape, or entries with the wrong inner shape)
  # restarts clean rather than crash-looping the tick forever.
  defp normalize_state(state) when is_map(state) do
    Map.filter(state, fn
      {cid, %{opened_ms: ms, alerted: a}} when is_binary(cid) and is_integer(ms) and is_boolean(a) ->
        true

      _ ->
        false
    end)
  end

  defp normalize_state(_), do: %{}

  defp prune_stale_alerted(tracked, now_ms) do
    tracked
    |> Enum.reject(fn {_cid, info} ->
      info.alerted and now_ms - info.opened_ms > @alerted_ttl_ms
    end)
    |> Map.new()
  end

  defp apply_events(events, tracked) do
    Enum.reduce(events, tracked, fn ev, acc ->
      case {kind(ev), ev["cid"]} do
        {"request_open", cid} when is_binary(cid) ->
          case event_ms(ev) do
            nil -> acc
            ms -> Map.put_new(acc, cid, %{opened_ms: ms, alerted: false})
          end

        {"reply_sent", cid} when is_binary(cid) ->
          if Map.get(ev, "ok", true), do: Map.delete(acc, cid), else: acc

        _ ->
          acc
      end
    end)
  end

  defp scan(tracked, swarm, minutes, now_ms) do
    window_ms = minutes * 60_000

    {alerts, new_state} =
      Enum.reduce(tracked, {[], %{}}, fn {cid, info}, {alerts, acc} ->
        cond do
          info.alerted ->
            {alerts, Map.put(acc, cid, info)}

          is_integer(info.opened_ms) and now_ms - info.opened_ms > window_ms ->
            {[unanswered_alert(swarm, now_ms, cid, info.opened_ms) | alerts],
             Map.put(acc, cid, %{info | alerted: true})}

          true ->
            {alerts, Map.put(acc, cid, info)}
        end
      end)

    {Enum.reverse(alerts), new_state}
  end

  defp unanswered_alert(swarm, now_ms, cid, opened_ms) do
    waited_minutes = div(now_ms - opened_ms, 60_000)

    %{
      type: :unanswered,
      swarm: swarm,
      at_ms: now_ms,
      summary: "request #{cid} unanswered for #{waited_minutes} min",
      evidence: %{"opened_at_ms" => opened_ms, "waited_minutes" => waited_minutes},
      key: {swarm, :unanswered, cid},
      cids: [cid]
    }
  end

  # Feed events name themselves via "kind" on both wires (wingston event
  # registry / micromarkets base/2). No "event_type" fallback: on the
  # micromarkets wire that key carries the RAW log type ("telegram_reply",
  # "message_received", ...), never the display vocabulary.
  defp kind(ev) when is_map(ev), do: ev["kind"]
  defp kind(_), do: nil

  # "ts" = float unix SECONDS on both wires (wingston event_feed.ex:171,
  # micromarkets event_feed.ex:479-480) — the feed carries no ISO8601
  # "timestamp"; that field belongs to the LogStore /events surface.
  defp event_ms(%{"ts" => ts}) when is_number(ts), do: round(ts * 1000)
  defp event_ms(_), do: nil
end
