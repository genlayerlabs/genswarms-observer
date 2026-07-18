defmodule Genswarms.Observer.Detectors.Unanswered do
  @moduledoc """
  Alerts when a `request_open` has no matching ok `reply_sent` within
  `"unanswered.minutes"`.

  Consumes `fetched.feed` — the DISPLAY event feed
  (`GET /api/swarms/:name/events/feed`), the only surface that carries the
  `request_open`/`reply_sent` vocabulary. NOT `fetched.events` (engine-raw
  LogStore), which the legacy health detectors keep.

  Real wire event shape, identical across both reference hosts (see the
  provenance block in detectors_ux_test.exs): `%{kind, cid, ok?, seq, ts}`
  with `ts` = float unix seconds; string keys after the JSON hop. Hosts may
  append log-store extras — ignored here.

  So on our side: `%{"kind" => "request_open", "cid" => c, "ts" => secs}`,
  `%{"kind" => "reply_sent", "cid" => c, "ok" => bool, "ts" => secs}` —
  `"ts"` is FLOAT UNIX SECONDS on both wires (no per-swarm divergence).
  A missing `"ok"` on `reply_sent` counts as delivered (`true`).

  `feed` `:unavailable` / `{:error, _}` / absent is a NO-OP with prior
  state: without the window we cannot know whether replies happened, and a
  false `:unanswered` on an answered pair is worse than a late one.

  State: `%{cid => %{opened_ms: integer, alerted: bool, blocked: nil | reason}}`.
  Events may arrive as an overlapping window across ticks — `request_open`
  only opens a cid the FIRST time it's seen (`Map.put_new/3`), so a
  re-delivered open in a later window neither resets the clock nor causes a
  second alert. An ok reply clears the cid entirely (a later re-open starts
  fresh). Once an alert for a cid has actually EMITTED (post-budget — see
  on_emitted/2), the still-open cid is not re-alerted while it remains open.
  The feed cursor itself is Scope's (session-local): after a restart the ring
  replays ascending from 0 and every answered open/reply pair cancels out here.

  Cause attribution: an `llm_proxy_block` event (`%{"kind", "cid",
  "reason"}` — emitted by the LLM proxy on every budget/quota/global block)
  on a TRACKED cid stamps `blocked: reason` on the entry, so the eventual
  alert names the cause instead of the generic "no reply". Blocks on
  untracked cids are ignored: no pending user request means nobody is
  waiting (this also filters background sessions like summarizers).

  Wave aggregation: when `"unanswered.wave_min"` (default 2) or more
  overdue cids share the blocked cause, they collapse into ONE
  `:budget_block_wave` alert keyed `{swarm, :budget_block_wave}` — a mass
  budget exhaustion is one incident, not N cards. Below the minimum,
  blocked cids alert individually with the cause attributed.
  """

  @behaviour Genswarms.Observer.Detector

  # Once alerted, a cid that's never replied to stays in state forever
  # (the "reply clears it" path never fires) — evict it after this long so
  # state doesn't grow unbounded.
  @alerted_ttl_ms 24 * 60 * 60 * 1000

  @impl true
  def default_thresholds, do: %{"unanswered.minutes" => 15, "unanswered.wave_min" => 2}

  @impl true
  def init, do: %{}

  @impl true
  def detect(fetched, ctx) do
    case fetched do
      %{feed: {:ok, events}} when is_list(events) ->
        minutes = ctx.thresholds["unanswered.minutes"]
        wave_min = Map.get(ctx.thresholds, "unanswered.wave_min", 2)
        tracked = apply_events(sort_by_ts(events), normalize_state(ctx.state))

        {alerts, new_state} = scan(tracked, ctx.swarm, minutes, wave_min, ctx.now_ms)

        {alerts, prune_stale_alerted(new_state, ctx.now_ms)}

      # :unavailable / {:error, _} / no :feed key at all — no window, no
      # verdict: no-op with prior state (see moduledoc).
      _ ->
        {[], ctx.state}
    end
  end

  @impl true
  # F4: the alerted flag is the re-fire guard — it must reflect what the
  # OPERATOR saw, not what detect/2 generated. Applied only for alerts that
  # actually emitted. A wave alert carries every collapsed cid — mark all.
  def on_emitted(state, %{cids: cids}) when is_map(state) and is_list(cids) do
    Enum.reduce(cids, state, fn cid, acc ->
      case acc do
        %{^cid => info} -> Map.put(acc, cid, %{info | alerted: true})
        _ -> acc
      end
    end)
  end

  def on_emitted(state, _alert), do: state

  # The EventsSource contract guarantees oldest-first ascending (dashboard
  # backend README §EventsSource: "Events with
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

  # F2 guard: state is a map of cid => %{opened_ms:, alerted:, blocked:}; a
  # poisoned store entry (any other shape, or entries with the wrong inner
  # shape) restarts clean rather than crash-looping the tick forever.
  # Pre-cause-attribution entries (no :blocked key) are UPGRADED, not
  # dropped — a restart must not lose in-flight tracking.
  defp normalize_state(state) when is_map(state) do
    state
    |> Enum.flat_map(fn
      {cid, %{opened_ms: ms, alerted: a} = info}
      when is_binary(cid) and is_integer(ms) and is_boolean(a) ->
        case Map.get(info, :blocked) do
          b when is_binary(b) or is_nil(b) -> [{cid, %{opened_ms: ms, alerted: a, blocked: b}}]
          _ -> []
        end

      _ ->
        []
    end)
    |> Map.new()
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
            ms -> Map.put_new(acc, cid, %{opened_ms: ms, alerted: false, blocked: nil})
          end

        {"reply_sent", cid} when is_binary(cid) ->
          if Map.get(ev, "ok", true), do: Map.delete(acc, cid), else: acc

        # LLM-proxy block on a TRACKED cid stamps the cause. Untracked cids
        # are ignored: no open request means nobody is waiting (also filters
        # background sessions — summarizers block without a request_open).
        {"llm_proxy_block", cid} when is_binary(cid) ->
          case acc do
            %{^cid => info} -> Map.put(acc, cid, %{info | blocked: block_reason(ev)})
            _ -> acc
          end

        _ ->
          acc
      end
    end)
  end

  defp scan(tracked, swarm, minutes, wave_min, now_ms) do
    window_ms = minutes * 60_000

    overdue =
      tracked
      |> Enum.filter(fn {_cid, info} ->
        not info.alerted and is_integer(info.opened_ms) and
          now_ms - info.opened_ms > window_ms
      end)
      |> Enum.sort_by(fn {_cid, info} -> info.opened_ms end)

    {blocked, plain} = Enum.split_with(overdue, fn {_cid, info} -> is_binary(info.blocked) end)

    blocked_alerts =
      if length(blocked) >= wave_min do
        [wave_alert(swarm, now_ms, blocked)]
      else
        Enum.map(blocked, fn {cid, info} ->
          unanswered_alert(swarm, now_ms, cid, info.opened_ms, info.blocked)
        end)
      end

    plain_alerts =
      Enum.map(plain, fn {cid, info} ->
        unanswered_alert(swarm, now_ms, cid, info.opened_ms, nil)
      end)

    {blocked_alerts ++ plain_alerts, tracked}
  end

  defp unanswered_alert(swarm, now_ms, cid, opened_ms, blocked_reason) do
    waited_minutes = div(now_ms - opened_ms, 60_000)

    {summary, evidence} =
      if is_binary(blocked_reason) do
        {"request #{cid} unanswered for #{waited_minutes} min — LLM blocked (#{blocked_reason})",
         %{
           "opened_at_ms" => opened_ms,
           "waited_minutes" => waited_minutes,
           "blocked_reason" => blocked_reason
         }}
      else
        {"request #{cid} unanswered for #{waited_minutes} min",
         %{"opened_at_ms" => opened_ms, "waited_minutes" => waited_minutes}}
      end

    %{
      type: :unanswered,
      swarm: swarm,
      at_ms: now_ms,
      summary: summary,
      evidence: evidence,
      key: {swarm, :unanswered, cid},
      cids: [cid]
    }
  end

  # A mass budget exhaustion is ONE incident: a single swarm-keyed card that
  # the 30-min cooldown naturally rate-limits, instead of per-cid flood +
  # budget-coalesce noise. Cooldown-dropped waves leave every cid unmarked
  # (on_emitted never ran), so newly blocked cids join the next emitted wave.
  defp wave_alert(swarm, now_ms, blocked) do
    cids = Enum.map(blocked, fn {cid, _info} -> cid end)
    reasons = blocked |> Enum.map(fn {_cid, info} -> info.blocked end) |> Enum.uniq()

    oldest_wait =
      blocked
      |> Enum.map(fn {_cid, info} -> div(now_ms - info.opened_ms, 60_000) end)
      |> Enum.max()

    %{
      type: :budget_block_wave,
      swarm: swarm,
      at_ms: now_ms,
      summary:
        "#{length(cids)} conversations unanswered and LLM-blocked (#{Enum.join(reasons, ", ")}) — oldest waiting #{oldest_wait} min",
      evidence: %{
        "count" => length(cids),
        "cids" => cids,
        "reasons" => reasons,
        "oldest_waited_minutes" => oldest_wait
      },
      key: {swarm, :budget_block_wave},
      cids: cids
    }
  end

  defp block_reason(ev) do
    case ev["reason"] do
      r when is_binary(r) and r != "" -> r
      _ -> "unknown"
    end
  end

  # Feed events name themselves via "kind" on both reference wires. No
  # "event_type" fallback: on one of them that key carries the RAW log
  # type ("telegram_reply", "message_received", ...), never the display
  # vocabulary.
  defp kind(ev) when is_map(ev), do: ev["kind"]
  defp kind(_), do: nil

  # "ts" = float unix SECONDS on both reference wires (provenance:
  # detectors_ux_test.exs) — the feed carries no ISO8601
  # "timestamp"; that field belongs to the LogStore /events surface.
  defp event_ms(%{"ts" => ts}) when is_number(ts), do: round(ts * 1000)
  defp event_ms(_), do: nil
end
