defmodule Genswarms.Observer.DetectorsTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Detectors

  # Fixtures follow the dashboard wire contract (genswarms-dashboard/backend
  # README, pinned by its golden contract test) — string keys, ISO8601 stamps.

  @now_ms 1_751_734_800_000

  defp iso(ms_ago) do
    (@now_ms - ms_ago) |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()
  end

  defp envelope(overrides \\ %{}) do
    Map.merge(
      %{
        "swarm" => "wingston",
        "status" => "running",
        "uptime_s" => 3600,
        "generated_at" => iso(0),
        "data_source" => "genswarms",
        "warnings" => [],
        "summary" => %{
          "agents" => 3,
          "objects" => 2,
          "sessions" => 0,
          "pool" => %{"leased" => 0, "size" => 4}
        },
        "nodes" => [
          %{"name" => "dashboard", "type" => "object", "subtype" => "dashboard"},
          %{"name" => "worker", "type" => "agent", "state" => "idle"}
        ],
        "edges" => [],
        "sessions" => [],
        "extensions" => %{}
      },
      overrides
    )
  end

  defp event(overrides \\ %{}) do
    Map.merge(
      %{
        "id" => 1,
        "timestamp" => iso(1_000),
        "level" => "info",
        "category" => "agent",
        "swarm" => "wingston",
        "agent" => "worker",
        "event_type" => "agent_started",
        "message" => "agent worker agent started",
        "metadata" => %{}
      },
      overrides
    )
  end

  defp detect(data, thresholds \\ %{}, det_state \\ nil) do
    Detectors.detect("wingston", data, thresholds, det_state, @now_ms)
  end

  defp ok(envelope, events), do: %{dashboard: {:ok, envelope}, events: {:ok, events}}

  defp types({alerts, _st}), do: Enum.map(alerts, & &1.type)

  # ── quiet baseline ────────────────────────────────────────────────────────

  test "healthy idle swarm with fresh events raises nothing" do
    assert {[], _} = detect(ok(envelope(), [event()]))
  end

  test "healthy swarm with zero events raises nothing (fresh boot)" do
    assert {[], _} = detect(ok(envelope(), []))
  end

  # ── endpoint_down ─────────────────────────────────────────────────────────

  test "dashboard fetch error fires endpoint_down and nothing else" do
    result = detect(%{dashboard: {:error, :econnrefused}, events: {:error, :econnrefused}})
    assert types(result) == [:endpoint_down]

    {[alert], _} = result
    assert alert.swarm == "wingston"
    assert alert.at_ms == @now_ms
    assert alert.evidence["reason"] =~ "econnrefused"
  end

  test "endpoint_down resets pool saturation memory (no stale streak on recovery)" do
    saturated = envelope(%{"summary" => %{"pool" => %{"leased" => 4, "size" => 4}}})

    {[], st} = detect(ok(saturated, [event()]))
    assert st.saturated_since_ms == @now_ms

    {_alerts, st} = detect(%{dashboard: {:error, :timeout}, events: {:error, :timeout}}, %{}, st)
    assert st.saturated_since_ms == nil
  end

  test "events fetch error alone does not fire endpoint_down (dashboard is the liveness probe)" do
    assert {[], _} = detect(%{dashboard: {:ok, envelope()}, events: {:error, :timeout}})
  end

  # ── stall ─────────────────────────────────────────────────────────────────

  test "leased pool + silent events beyond stall_minutes fires stall" do
    env = envelope(%{"summary" => %{"pool" => %{"leased" => 1, "size" => 4}}})
    old_event = event(%{"timestamp" => iso(11 * 60_000)})

    result = detect(ok(env, [old_event]))
    assert types(result) == [:stall]

    {[alert], _} = result
    assert alert.evidence["silent_minutes"] >= 10
    assert alert.evidence["active"]["leased"] == 1
  end

  test "busy agent node (non-idle state) counts as active for stall" do
    env = envelope(%{"nodes" => [%{"name" => "worker", "type" => "agent", "state" => "working"}]})
    result = detect(ok(env, [event(%{"timestamp" => iso(30 * 60_000)})]))
    assert types(result) == [:stall]
    {[alert], _} = result
    assert alert.evidence["active"]["busy_agents"] == ["worker"]
  end

  test "idle swarm never stalls no matter how old the events are" do
    assert {[], _} = detect(ok(envelope(), [event(%{"timestamp" => iso(120 * 60_000)})]))
  end

  test "active swarm with recent events does not stall" do
    env = envelope(%{"summary" => %{"pool" => %{"leased" => 2, "size" => 4}}})
    assert {[], _} = detect(ok(env, [event(%{"timestamp" => iso(60_000)})]))
  end

  test "active swarm with NO events does not stall (nothing to date the silence from)" do
    env = envelope(%{"summary" => %{"pool" => %{"leased" => 2, "size" => 4}}})
    assert {[], _} = detect(ok(env, []))
  end

  test "stall_minutes threshold is honored" do
    env = envelope(%{"summary" => %{"pool" => %{"leased" => 1, "size" => 4}}})
    events = [event(%{"timestamp" => iso(6 * 60_000)})]

    assert {[], _} = detect(ok(env, events))
    assert [:stall] = types(detect(ok(env, events), %{"stall_minutes" => 5}))
  end

  test "stall uses the NEWEST event (one fresh event silences old ones)" do
    env = envelope(%{"summary" => %{"pool" => %{"leased" => 1, "size" => 4}}})
    events = [event(%{"timestamp" => iso(60 * 60_000)}), event(%{"timestamp" => iso(1_000)})]
    assert {[], _} = detect(ok(env, events))
  end

  test "unparsable timestamps are ignored, not crashed on" do
    env = envelope(%{"summary" => %{"pool" => %{"leased" => 1, "size" => 4}}})
    events = [event(%{"timestamp" => "garbage"}), event(%{"timestamp" => nil})]
    assert {[], _} = detect(ok(env, events))
  end

  # ── error_burst ───────────────────────────────────────────────────────────

  test "K error events inside the window fire error_burst" do
    errors =
      for i <- 1..5 do
        event(%{
          "id" => i,
          "level" => "error",
          "event_type" => "llm_error",
          "timestamp" => iso(i * 5_000),
          "message" => "llm call failed"
        })
      end

    result = detect(ok(envelope(), errors))
    assert types(result) == [:error_burst]

    {[alert], _} = result
    assert alert.evidence["count"] == 5
    assert length(alert.evidence["sample"]) == 3
  end

  test "errors below the count threshold stay quiet" do
    errors =
      for i <- 1..4,
          do: event(%{"id" => i, "level" => "error", "timestamp" => iso(i * 5_000)})

    assert {[], _} = detect(ok(envelope(), errors))
  end

  test "old errors outside the window do not count" do
    errors =
      for i <- 1..5,
          do: event(%{"id" => i, "level" => "error", "timestamp" => iso(120_000 + i * 5_000)})

    assert {[], _} = detect(ok(envelope(), errors))
  end

  test "non-error levels never count toward the burst" do
    warnings =
      for i <- 1..10,
          do: event(%{"id" => i, "level" => "warning", "timestamp" => iso(i * 1_000)})

    assert {[], _} = detect(ok(envelope(), warnings))
  end

  test "error_burst thresholds are tunable" do
    errors =
      for i <- 1..2,
          do: event(%{"id" => i, "level" => "error", "timestamp" => iso(i * 1_000)})

    assert [:error_burst] = types(detect(ok(envelope(), errors), %{"error_burst_count" => 2}))
  end

  # ── budget_block ──────────────────────────────────────────────────────────

  test "llm_proxy_global_block event_type fires budget_block" do
    ev = event(%{"event_type" => "llm_proxy_global_block", "level" => "warning"})
    result = detect(ok(envelope(), [ev]))
    assert types(result) == [:budget_block]
    {[alert], _} = result
    assert alert.evidence["event"]["event_type"] == "llm_proxy_global_block"
  end

  test "llm_proxy_global_block in the message also fires (belt and braces)" do
    ev = event(%{"message" => "proxy: llm_proxy_global_block engaged, budget exhausted"})
    assert [:budget_block] = types(detect(ok(envelope(), [ev])))
  end

  test "one budget_block alert even with many sightings" do
    evs = for i <- 1..3, do: event(%{"id" => i, "event_type" => "llm_proxy_global_block"})
    assert [:budget_block] = types(detect(ok(envelope(), evs)))
  end

  # ── pool_saturated ────────────────────────────────────────────────────────

  test "saturation must be sustained: first sighting arms, alert only after pool_saturated_s" do
    env = envelope(%{"summary" => %{"pool" => %{"leased" => 4, "size" => 4}}})

    {[], st} = detect(ok(env, [event()]))
    assert st.saturated_since_ms == @now_ms

    # 60s later: still below the 120s default -> quiet
    {alerts, st} = Detectors.detect("wingston", ok(env, [event()]), %{}, st, @now_ms + 60_000)
    assert alerts == []

    # 130s after arming -> fires
    {alerts, _st} = Detectors.detect("wingston", ok(env, [event()]), %{}, st, @now_ms + 130_000)
    assert [:pool_saturated] = Enum.map(alerts, & &1.type)
    assert hd(alerts).evidence == %{"leased" => 4, "size" => 4, "saturated_for_s" => 130}
  end

  test "desaturating resets the streak" do
    full = envelope(%{"summary" => %{"pool" => %{"leased" => 4, "size" => 4}}})
    free = envelope(%{"summary" => %{"pool" => %{"leased" => 3, "size" => 4}}})

    {[], st} = detect(ok(full, []))
    {[], st} = Detectors.detect("wingston", ok(free, []), %{}, st, @now_ms + 60_000)
    assert st.saturated_since_ms == nil

    # saturated again much later: streak restarts, no instant alert
    {alerts, st} = Detectors.detect("wingston", ok(full, []), %{}, st, @now_ms + 600_000)
    assert alerts == []
    assert st.saturated_since_ms == @now_ms + 600_000
  end

  test "empty pool (size 0) never saturates" do
    env = envelope(%{"summary" => %{"pool" => %{"leased" => 0, "size" => 0}}})
    {[], st} = detect(ok(env, []))
    assert st.saturated_since_ms == nil
  end

  test "pool_saturated keeps firing while held (Scope's cooldown dedupes)" do
    env = envelope(%{"summary" => %{"pool" => %{"leased" => 2, "size" => 2}}})
    {[], st} = detect(ok(env, []))
    {[a1], st} = Detectors.detect("wingston", ok(env, []), %{}, st, @now_ms + 130_000)
    {[a2], _} = Detectors.detect("wingston", ok(env, []), %{}, st, @now_ms + 260_000)
    assert a1.type == :pool_saturated and a2.type == :pool_saturated
  end

  # ── composition ───────────────────────────────────────────────────────────

  test "independent detectors can fire together in one tick" do
    env = envelope(%{"summary" => %{"pool" => %{"leased" => 1, "size" => 4}}})

    events =
      [event(%{"timestamp" => iso(20 * 60_000)})] ++
        for i <- 1..5 do
          event(%{
            "id" => i,
            "level" => "error",
            "event_type" => "llm_error",
            "timestamp" => iso(i * 1_000)
          })
        end

    {alerts, _} = detect(ok(env, events))
    assert Enum.sort(Enum.map(alerts, & &1.type)) == [:error_burst]

    # note: fresh error events also refresh the stall clock — a bursting swarm
    # is noisy, not silent. Verify stall stays out.
    refute :stall in Enum.map(alerts, & &1.type)
  end

  test "missing summary/pool keys degrade gracefully" do
    env = envelope(%{"summary" => %{}})
    assert {[], _} = detect(ok(env, [event()]))
  end

  test "default thresholds are exposed" do
    assert %{"stall_minutes" => 10} = Detectors.default_thresholds()
  end
end
