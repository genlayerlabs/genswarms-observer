defmodule Genswarms.Observer.Lifecycle do
  @moduledoc """
  The pure alert-lifecycle policy for one swarm's tick: cooldown filter →
  same-key in-batch dedupe → per-swarm budget (overflow coalesces into one
  summary, itself cooldown-gated) → stamp emitted keys → evict dead keys.

  Pure: `%{emit:, suppressed:, last_alert:}` out, nothing else touched.
  `suppressed` counts EVERY dropped alert — cooldown-filtered, in-batch
  same-key dedupe drops, and budget-dropped — so `emit + suppressed`
  always equals the input count (plus the coalesced summary when it
  emits). Only EMITTED keys are stamped into last_alert — a budget-dropped
  alert stays eligible next tick (the F4 fix builds on this: its
  detector-side `alerted` flag is also only applied on emit, via
  on_emitted/2).

  F10: an entry older than the cooldown window can never suppress anything
  again (`cooled_down?` would return true regardless), so it is dead
  weight — with per-cid keys it grew without bound and was persisted every
  dirty tick. Evicted here, every call.
  """

  def alert_key(alert), do: Map.get(alert, :key, {alert.swarm, alert.type})

  def process(alerts, last_alert, cooldown_ms, budget, swarm, now_ms) do
    last_alert = evict(last_alert, cooldown_ms, now_ms)

    {passed, cooled_suppressed} =
      Enum.reduce(alerts, {[], 0}, fn alert, {passed, supp} ->
        if cooled_down?(last_alert, alert, cooldown_ms, now_ms),
          do: {[alert | passed], supp},
          else: {passed, supp + 1}
      end)

    passed = Enum.reverse(passed)
    deduped = Enum.uniq_by(passed, &alert_key/1)
    deduped_dropped = length(passed) - length(deduped)

    {emit, coalesced_suppressed} =
      apply_budget(deduped, last_alert, cooldown_ms, budget, swarm, now_ms)

    stamped =
      Enum.reduce(emit, last_alert, fn alert, acc ->
        Map.put(acc, alert_key(alert), alert.at_ms)
      end)

    %{
      emit: emit,
      suppressed: cooled_suppressed + deduped_dropped + coalesced_suppressed,
      last_alert: stamped
    }
  end

  defp evict(last_alert, cooldown_ms, now_ms) do
    Map.filter(last_alert, fn {_key, at_ms} -> now_ms - at_ms < cooldown_ms end)
  end

  defp cooled_down?(last_alert, alert, cooldown_ms, now_ms) do
    case Map.get(last_alert, alert_key(alert)) do
      nil -> true
      last_ms -> now_ms - last_ms >= cooldown_ms
    end
  end

  defp apply_budget(alerts, last_alert, cooldown_ms, budget, swarm, now_ms) do
    if length(alerts) <= budget do
      {alerts, 0}
    else
      {kept, dropped} = Enum.split(alerts, budget)

      dropped_counts =
        dropped
        |> Enum.group_by(& &1.type)
        |> Map.new(fn {type, list} -> {to_string(type), length(list)} end)

      coalesced = %{
        type: :alerts_coalesced,
        swarm: swarm,
        at_ms: now_ms,
        summary: "#{length(dropped)} additional alert(s) suppressed by the per-tick budget",
        evidence: %{"dropped" => dropped_counts},
        key: {swarm, :alerts_coalesced},
        cids: [],
        source: __MODULE__
      }

      if cooled_down?(last_alert, coalesced, cooldown_ms, now_ms) do
        {kept ++ [coalesced], length(dropped)}
      else
        {kept, length(dropped) + 1}
      end
    end
  end
end
