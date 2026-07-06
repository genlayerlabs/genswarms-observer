defmodule Genswarms.Observer.Detectors.Unanswered do
  @moduledoc """
  Alerts when a `request_open` has no matching ok `reply_sent` within
  `"unanswered.minutes"`.

  Event shapes (string-keyed, dashboard/MM wire): `%{"kind" => "request_open",
  "cid" => c, "timestamp" => iso}`, `%{"kind" => "reply_sent", "cid" => c,
  "ok" => bool, "timestamp" => iso}`. Either `"kind"` or `"event_type"` names
  the event; a missing `"ok"` on `reply_sent` counts as delivered (`true`).

  State: `%{cid => %{opened_ms: integer, alerted: bool}}`. Events may arrive
  as an overlapping window across ticks — `request_open` only opens a cid
  the FIRST time it's seen (`Map.put_new/3`), so a re-delivered open in a
  later window neither resets the clock nor causes a second alert. An ok
  reply clears the cid entirely (a later re-open starts fresh). Once alerted,
  a still-open cid is not re-alerted while it remains open.
  """

  @behaviour Genswarms.Observer.Detector

  @impl true
  def default_thresholds, do: %{"unanswered.minutes" => 15}

  @impl true
  def init, do: %{}

  @impl true
  def detect(fetched, ctx) do
    minutes = ctx.thresholds["unanswered.minutes"]
    tracked = apply_events(events(fetched), ctx.state || %{})

    scan(tracked, ctx.swarm, minutes, ctx.now_ms)
  end

  defp events(%{events: {:ok, events}}) when is_list(events), do: events
  defp events(_), do: []

  defp apply_events(events, tracked) do
    Enum.reduce(events, tracked, fn ev, acc ->
      case {kind(ev), ev["cid"]} do
        {"request_open", cid} when is_binary(cid) ->
          Map.put_new(acc, cid, %{opened_ms: event_ms(ev), alerted: false})

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

  defp kind(ev), do: ev["kind"] || ev["event_type"]

  defp event_ms(%{"timestamp" => ts}) when is_binary(ts) do
    case DateTime.from_iso8601(ts) do
      {:ok, dt, _offset} -> DateTime.to_unix(dt, :millisecond)
      _ -> nil
    end
  end

  defp event_ms(_), do: nil
end
