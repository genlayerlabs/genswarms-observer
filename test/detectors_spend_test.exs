defmodule Genswarms.Observer.DetectorsSpendTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Detectors.LlmSpend

  @t0 1_752_300_000_000
  @minute 60_000
  @hour 60 * @minute

  defp thresholds(overrides), do: Map.merge(LlmSpend.default_thresholds(), overrides)

  defp fetched(value) do
    %{
      dashboard:
        {:ok, %{"extensions" => %{"llm_proxy_budget" => %{"spent_usd" => value, "v" => 1}}}},
      events: {:ok, []},
      feed: :unavailable
    }
  end

  defp ctx(now_ms, state, overrides \\ %{}) do
    %{swarm: "wingston", thresholds: thresholds(overrides), state: state, now_ms: now_ms}
  end

  # Feed a [{minutes_offset, cumulative_usd}] trajectory through detect/2
  # tick by tick; returns {alerts_from_the_LAST_tick, final_state}.
  defp run(trajectory, overrides \\ %{}) do
    Enum.reduce(trajectory, {[], LlmSpend.init()}, fn {min, value}, {_alerts, state} ->
      LlmSpend.detect(fetched(value), ctx(@t0 + min * @minute, state, overrides))
    end)
  end

  # $0.05 every 30 min = a $0.10/hour baseline over `hours`.
  defp steady(hours) do
    for i <- 0..(hours * 2), do: {i * 30, i * 0.05}
  end

  test "fires when the last window's spend towers over the trailing baseline" do
    base = steady(6)
    {_, at} = List.last(base)
    spike = [{6 * 60 + 30, at + 1.0}, {7 * 60, at + 2.0}]

    {alerts, _state} = run(base ++ spike)

    assert [alert] = alerts
    assert alert.type == :llm_spend_spike
    assert alert.key == {"wingston", :llm_spend_spike}
    assert alert.evidence["window_usd"] == "2.00"
    assert alert.evidence["baseline_avg_usd"] == "0.10"
    assert alert.evidence["window_minutes"] == 60
    assert alert.summary =~ "$2.00"
  end

  test "a steady burn rate never fires, even above min_usd" do
    # $2/hour for 8 hours: every window looks like its baseline
    trajectory = for i <- 0..16, do: {i * 30, i * 1.0}
    {alerts, _} = run(trajectory)
    assert alerts == []
  end

  test "warm-up: too little history yields no verdict regardless of spend" do
    {alerts, _} = run([{0, 0.0}, {30, 50.0}, {60, 90.0}])
    assert alerts == []
  end

  test "min_usd floors a quiet baseline — pocket change never pages" do
    base = steady(6)
    {_, at} = List.last(base)
    {alerts, _} = run(base ++ [{7 * 60, at + 0.5}])
    assert alerts == []
  end

  test "midnight reset: a cumulative drop reads as spend-since-reset, never negative" do
    # steady day, rollover to 0, tiny post-midnight spend — quiet
    base = steady(6)
    {_, at} = List.last(base)
    rollover = [{6 * 60 + 30, 0.02}, {7 * 60, 0.05}]
    {alerts, state} = run(base ++ rollover)
    assert alerts == []

    # the reset didn't poison the ring: a real post-midnight spike still fires
    {alerts, _} = LlmSpend.detect(fetched(3.05), ctx(@t0 + 8 * 60 * @minute, state))
    assert [%{type: :llm_spend_spike}] = alerts
    _ = at
  end

  test "decimal-string values (llm_usage-style) parse like floats" do
    base = steady(6)
    {_, at} = List.last(base)
    trajectory = Enum.map(base, fn {min, v} -> {min, Float.to_string(v)} end)
    {alerts, _} = run(trajectory ++ [{7 * 60, Float.to_string(at + 2.0)}])
    assert [%{type: :llm_spend_spike}] = alerts
  end

  test "absent extension or failed dashboard is a no-op with prior state" do
    {_, state} = run(steady(3))

    for fetched <- [
          %{dashboard: {:ok, %{"extensions" => %{}}}, events: {:ok, []}, feed: :unavailable},
          %{dashboard: {:ok, %{}}, events: {:ok, []}, feed: :unavailable},
          %{dashboard: {:error, :timeout}, events: {:error, :timeout}, feed: {:error, :timeout}},
          %{
            dashboard: {:ok, %{"extensions" => %{"llm_proxy_budget" => %{"spent_usd" => "junk"}}}},
            events: {:ok, []},
            feed: :unavailable
          }
        ] do
      assert {[], ^state} = LlmSpend.detect(fetched, ctx(@t0 + 4 * @hour, state))
    end
  end

  test "poisoned store state restarts clean instead of crashing the tick" do
    for bad <- [nil, :junk, %{samples: :junk}, %{samples: [{:bad, "sample"}, {1, -2.0}]}] do
      {alerts, state} = LlmSpend.detect(fetched(1.0), ctx(@t0, bad))
      assert alerts == []
      assert %{samples: [{@t0, 1.0}]} = state
    end
  end

  test "the ring prunes past the baseline horizon" do
    {_, state} = run(steady(24))
    horizon = @t0 + 24 * @hour - 7 * @hour
    assert Enum.all?(state.samples, fn {ts, _} -> ts >= horizon end)
  end

  test "llm_spend.path reads an alternate extension location" do
    fetched = %{
      dashboard: {:ok, %{"extensions" => %{"proxy_router" => %{"spent_usd" => "1.50"}}}},
      events: {:ok, []},
      feed: :unavailable
    }

    overrides = %{"llm_spend.path" => ["proxy_router", "spent_usd"]}
    {[], state} = LlmSpend.detect(fetched, ctx(@t0, LlmSpend.init(), overrides))
    assert state.samples == [{@t0, 1.5}]
  end
  # ── review findings 2026-07-12 (adversarial pass on the merged detector) ──

  test "a jitter dip (stale replica read) is NOT a reset — one cent down must not page $39" do
    # quiet swarm creeping to ~$39, then a single sample ONE CENT lower
    base = for i <- 0..27, do: {i * 15, 39.0 + i * 0.0125}
    {alerts, _} = run(base ++ [{28 * 15, 39.32}])

    assert alerts == [],
           "a one-cent dip contributed the whole cumulative to the window: " <>
             inspect(Enum.map(alerts, & &1.summary))
  end

  test "a real midnight rollover is still treated as spend-since-reset" do
    base = steady(6)
    {_, at} = List.last(base)
    # cumulative drops to near zero (rollover), then a genuine post-midnight spike
    {alerts, _} = run(base ++ [{6 * 60 + 30, 0.02}, {7 * 60, 3.02}])
    assert [%{type: :llm_spend_spike}] = alerts
    _ = at
  end

  test "a sample GAP (observer down) yields no verdict — never a spike on the first tick back" do
    # 3h of dense samples, then a 4h hole (observer restarted), then one sample
    # carrying 4h of perfectly normal spend
    dense = for i <- 0..11, do: {i * 15, i * 0.0125}
    {alerts, state} = run(dense ++ [{3 * 60 + 4 * 60, 0.15 + 2.0}])

    assert alerts == [],
           "the gap's spend was attributed to the last window: " <>
             inspect(Enum.map(alerts, & &1.evidence))

    # and the ring restarted from the post-gap sample, so coverage rebuilds honestly
    assert [{_, 2.15}] = state.samples
  end

  test "llm_spend.window_s = 0 falls back to the default instead of crashing the tick" do
    {alerts, state} =
      LlmSpend.detect(fetched(1.0), ctx(@t0, LlmSpend.init(), %{"llm_spend.window_s" => 0}))

    assert alerts == []
    assert [{@t0, 1.0}] = state.samples
  end
end
