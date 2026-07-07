defmodule Genswarms.Observer.Detectors.TopicsStale do
  @moduledoc """
  Alerts when the observed swarm's `conversation_topics` dashboard extension
  hasn't produced a fresh `final` period recently, or has gone missing after
  once being present.

  Reads `get_in(envelope, ["extensions", "conversation_topics", "periods"])`:
  a list of `%{"period_id" => "YYYY-MM-DD", "final" => bool}` maps (UTC
  calendar days). The newest `final` period is compared against the CURRENT
  UTC day (derived from `ctx.now_ms`, never a wall clock) minus
  `"topics_stale.periods"` days.

  A swarm that has NEVER shown a well-formed periods list with at least one
  final entry raises nothing — it may simply not run the topics feature.
  Once that has been observed once, later absence of the extension OR a
  malformed block (periods not a list, or entries that aren't maps) is
  itself a signal -> alert. State: `%{ever_seen: bool}`.
  """

  @behaviour Genswarms.Observer.Detector

  @impl true
  def default_thresholds, do: %{"topics_stale.periods" => 1, "topics_stale.grace_hours" => 1}

  @impl true
  def init, do: %{ever_seen: false}

  @impl true
  def detect(fetched, ctx) do
    periods_threshold = ctx.thresholds["topics_stale.periods"]
    state = normalize_state(ctx.state)

    case newest_final_period_id(fetched) do
      {:ok, period_id} ->
        new_state = %{ever_seen: true}

        if stale?(period_id, ctx.now_ms, ctx.thresholds) do
          {[stale_alert(ctx.swarm, ctx.now_ms, period_id, periods_threshold)], new_state}
        else
          {[], new_state}
        end

      :none ->
        {[], state}

      :absent_or_malformed ->
        if state.ever_seen do
          {[missing_alert(ctx.swarm, ctx.now_ms)], state}
        else
          {[], state}
        end

      # F8: the dashboard did not fetch this tick — we have NO evidence about
      # the extension either way. Mirrors the feed detectors' no-op discipline:
      # a transient endpoint blip must not read as "extension missing".
      :no_data ->
        {[], state}
    end
  end

  # {:ok, period_id} when a newest final, parseable period was found; :none
  # when the block is well-formed but has no final periods yet (not itself
  # a signal); :absent_or_malformed when the extension is missing or broken.
  defp newest_final_period_id(%{dashboard: {:ok, envelope}}) do
    case get_in(envelope, ["extensions", "conversation_topics", "periods"]) do
      periods when is_list(periods) ->
        if Enum.all?(periods, &is_map/1) do
          case final_period_ids(periods) do
            [] -> :none
            ids -> {:ok, Enum.max(ids)}
          end
        else
          :absent_or_malformed
        end

      _ ->
        :absent_or_malformed
    end
  end

  # No fetched dashboard at all ({:error, _}, missing key, malformed client
  # return): no evidence, no verdict.
  defp newest_final_period_id(_), do: :no_data

  # F2 guard (same class as DeliveryFailureBurst.normalize_state/1): a
  # poisoned store entry must restart clean, never crash the tick forever.
  defp normalize_state(%{ever_seen: seen}) when is_boolean(seen), do: %{ever_seen: seen}
  defp normalize_state(_), do: %{ever_seen: false}

  # ISO-8601 "YYYY-MM-DD" strings sort lexicographically in chronological
  # order, so a plain Enum.max/1 over the validated id strings is enough.
  defp final_period_ids(periods) do
    periods
    |> Enum.filter(&(&1["final"] == true))
    |> Enum.flat_map(fn p ->
      case p["period_id"] do
        id when is_binary(id) -> if valid_date?(id), do: [id], else: []
        _ -> []
      end
    end)
  end

  defp valid_date?(id) do
    match?({:ok, _}, Date.from_iso8601(id))
  end

  # F3: the cutoff date is derived from (now - grace_hours), not raw now.
  # The producer's promise is "yesterday's period closes shortly AFTER
  # midnight" (e.g. a nightly close cron minutes past 00:00) — evaluating against the raw UTC
  # date in that gap would false-alarm nightly. Promise-vs-observation:
  # never evaluate a schedule the producer hasn't had time to keep.
  defp stale?(period_id, now_ms, thresholds) do
    periods_threshold = thresholds["topics_stale.periods"]
    grace_ms = round(Map.get(thresholds, "topics_stale.grace_hours", 1) * 3_600_000)
    {:ok, newest_date} = Date.from_iso8601(period_id)

    today =
      (now_ms - grace_ms) |> DateTime.from_unix!(:millisecond) |> DateTime.to_date()

    cutoff = Date.add(today, -periods_threshold)
    Date.compare(newest_date, cutoff) == :lt
  end

  defp stale_alert(swarm, now_ms, period_id, periods_threshold) do
    %{
      type: :topics_stale,
      swarm: swarm,
      at_ms: now_ms,
      summary: "conversation topics stale — newest final period #{period_id}",
      evidence: %{"newest_final_period" => period_id, "periods_threshold" => periods_threshold},
      key: {swarm, :topics_stale},
      cids: []
    }
  end

  defp missing_alert(swarm, now_ms) do
    %{
      type: :topics_stale,
      swarm: swarm,
      at_ms: now_ms,
      summary: "conversation topics extension missing after previously being present",
      evidence: %{"reason" => "extension_absent_or_malformed"},
      key: {swarm, :topics_stale},
      cids: []
    }
  end
end
