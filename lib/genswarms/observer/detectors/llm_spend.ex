defmodule Genswarms.Observer.Detectors.LlmSpend do
  @moduledoc """
  Burn-RATE anomaly on the observed swarm's LLM spend — the complement to
  the llm-proxy package's own 75%/90%-of-ceiling health_rules. Those fire
  when the day's budget is nearly gone; this fires while a runaway loop is
  still cheap, by comparing the last window's spend against the swarm's own
  trailing baseline.

  Reads a cumulative same-day dollar value from the dashboard envelope —
  default `extensions["llm_proxy_budget"]["spent_usd"]` (the llm-proxy
  package's float twin; durable, resets at 00:00 UTC). The path is a
  threshold (`"llm_spend.path"`, a list of keys under `"extensions"`) so an
  operator can point it at any block exposing a cumulative daily spend.
  Values may be numbers or decimal strings (`"2.387737"`).

  Each tick the current value is appended to a sample ring. Spend inside an
  interval is the sum of counter INCREASES between consecutive samples —
  reset-aware: a drop (midnight rollover) contributes the new value, never
  a negative. The detector fires when

      spend(last window) >= factor * avg(spend per baseline window)
      AND spend(last window) >= min_usd

  where the baseline is the `llm_spend.baseline_windows` windows preceding
  the current one, restricted to windows the sample span actually covers.
  Fewer than `llm_spend.min_baseline_windows` covered windows → no verdict
  (warm-up after an observer restart, since detector state is only as
  durable as the configured store). The `min_usd` floor keeps a quiet
  baseline (avg ≈ 0, so factor× ≈ 0) from firing on pocket change.

  No dashboard this tick, or the path absent/malformed → no-op with prior
  state (the feed detectors' discipline: a transient blip is not evidence).
  """

  @behaviour Genswarms.Observer.Detector

  @impl true
  def default_thresholds do
    %{
      "llm_spend.window_s" => 3600,
      "llm_spend.factor" => 3.0,
      "llm_spend.min_usd" => 1.0,
      "llm_spend.baseline_windows" => 6,
      "llm_spend.min_baseline_windows" => 2,
      "llm_spend.path" => ["llm_proxy_budget", "spent_usd"]
    }
  end

  @impl true
  def init, do: %{samples: []}

  @impl true
  def detect(fetched, ctx) do
    state = normalize_state(ctx.state)

    case read_spend(fetched, ctx.thresholds) do
      {:ok, value} ->
        window_ms = round(num(ctx.thresholds, "llm_spend.window_s", 3600) * 1_000)
        baseline_windows = int(ctx.thresholds, "llm_spend.baseline_windows", 6)

        samples =
          (state.samples ++ [{ctx.now_ms, value}])
          |> prune(ctx.now_ms - (baseline_windows + 1) * window_ms)

        {verdict(samples, value, ctx, window_ms, baseline_windows), %{samples: samples}}

      :no_data ->
        {[], state}
    end
  end

  defp verdict(samples, today_usd, ctx, window_ms, baseline_windows) do
    now = ctx.now_ms
    factor = num(ctx.thresholds, "llm_spend.factor", 3.0)
    min_usd = num(ctx.thresholds, "llm_spend.min_usd", 1.0)
    min_baseline = int(ctx.thresholds, "llm_spend.min_baseline_windows", 2)

    covered = covered_baseline_windows(samples, now, window_ms, baseline_windows)

    if covered < min_baseline do
      []
    else
      current = increase(samples, now - window_ms, now)

      baseline_avg =
        increase(samples, now - (covered + 1) * window_ms, now - window_ms) / covered

      if current >= min_usd and current >= factor * baseline_avg do
        [alert(ctx, current, baseline_avg, window_ms, today_usd)]
      else
        []
      end
    end
  end

  # How many FULL baseline windows (each window_ms, ending at now-window_ms)
  # the sample span reaches back over, capped at the configured count. The
  # oldest sample is the span's edge — a window it doesn't reach into has
  # unknown spend and must not count as "quiet baseline".
  defp covered_baseline_windows([], _now, _window_ms, _max), do: 0

  defp covered_baseline_windows([{oldest, _} | _], now, window_ms, max) do
    span = now - oldest
    min(max, div(span - window_ms, window_ms)) |> max(0)
  end

  # Sum of counter increases attributed to samples inside (from, to]. A
  # sample below its predecessor is a reset (midnight rollover): the new
  # cumulative IS the spend since the reset. The predecessor value threads
  # across the whole ring so the first in-interval sample diffs against the
  # last sample before the interval.
  defp increase(samples, from, to) do
    samples
    |> Enum.reduce({nil, 0.0}, fn {ts, value}, {prev, acc} ->
      delta =
        cond do
          is_nil(prev) -> 0.0
          value >= prev -> value - prev
          true -> value
        end

      acc = if ts > from and ts <= to, do: acc + delta, else: acc
      {value, acc}
    end)
    |> elem(1)
  end

  defp alert(ctx, current, baseline_avg, window_ms, today_usd) do
    mins = div(window_ms, 60_000)

    %{
      type: :llm_spend_spike,
      swarm: ctx.swarm,
      at_ms: ctx.now_ms,
      summary:
        "LLM spend spiking — $#{usd(current)} in the last #{mins} min vs a $#{usd(baseline_avg)}/window baseline",
      evidence: %{
        "window_usd" => usd(current),
        "baseline_avg_usd" => usd(baseline_avg),
        "window_minutes" => mins,
        "today_usd" => usd(today_usd)
      },
      key: {ctx.swarm, :llm_spend_spike},
      cids: []
    }
  end

  defp usd(value), do: :erlang.float_to_binary(value / 1, decimals: 2)

  defp read_spend(%{dashboard: {:ok, envelope}}, thresholds) when is_map(envelope) do
    path = path(thresholds)

    case get_in(envelope, ["extensions" | path]) do
      v when is_number(v) and v >= 0 -> {:ok, v / 1}
      v when is_binary(v) -> parse_decimal(v)
      _ -> :no_data
    end
  rescue
    # get_in over a non-map intermediate (malformed envelope) is no evidence
    _ -> :no_data
  end

  defp read_spend(_fetched, _thresholds), do: :no_data

  defp path(thresholds) do
    case Map.get(thresholds, "llm_spend.path") do
      path when is_list(path) and path != [] ->
        if Enum.all?(path, &is_binary/1), do: path, else: ["llm_proxy_budget", "spent_usd"]

      _ ->
        ["llm_proxy_budget", "spent_usd"]
    end
  end

  defp parse_decimal(s) do
    case Float.parse(String.trim(s)) do
      {v, ""} when v >= 0 -> {:ok, v}
      _ -> :no_data
    end
  end

  # F2 guard: a poisoned store entry restarts clean, never crashes the tick.
  defp normalize_state(%{samples: samples}) when is_list(samples) do
    %{samples: Enum.filter(samples, &valid_sample?/1)}
  end

  defp normalize_state(_), do: %{samples: []}

  defp valid_sample?({ts, value}) when is_integer(ts) and is_number(value) and value >= 0,
    do: true

  defp valid_sample?(_), do: false

  defp prune(samples, cutoff), do: Enum.filter(samples, fn {ts, _} -> ts >= cutoff end)

  defp num(thresholds, key, default) do
    case Map.get(thresholds, key) do
      v when is_number(v) and v >= 0 -> v / 1
      _ -> default / 1
    end
  end

  defp int(thresholds, key, default) do
    case Map.get(thresholds, key) do
      v when is_integer(v) and v > 0 -> v
      _ -> default
    end
  end
end
