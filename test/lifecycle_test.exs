defmodule Genswarms.Observer.LifecycleTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Lifecycle

  @now 1_751_734_800_000
  @cooldown_ms 30 * 60_000

  defp alert(type, key, at \\ @now) do
    %{
      type: type,
      swarm: "w",
      at_ms: at,
      summary: "s",
      evidence: %{},
      key: key,
      cids: [],
      source: __MODULE__
    }
  end

  test "cooldown suppresses a key seen within the window, admits it after" do
    last = %{{"w", :stall} => @now - 10 * 60_000}

    %{emit: emit, suppressed: 1} =
      Lifecycle.process([alert(:stall, {"w", :stall})], last, @cooldown_ms, 6, "w", @now)

    assert emit == []

    %{emit: [_]} =
      Lifecycle.process(
        [alert(:stall, {"w", :stall})],
        %{{"w", :stall} => @now - @cooldown_ms - 1},
        @cooldown_ms,
        6,
        "w",
        @now
      )
  end

  test "same-key alerts within one batch dedupe to the first" do
    %{emit: emit} =
      Lifecycle.process(
        [alert(:stall, {"w", :stall}), alert(:stall, {"w", :stall})],
        %{},
        @cooldown_ms,
        6,
        "w",
        @now
      )

    assert length(emit) == 1
  end

  test "overflow coalesces into one summary and stamps only emitted keys" do
    alerts = for i <- 1..8, do: alert(:unanswered, {"w", :unanswered, "cid#{i}"})

    %{emit: emit, last_alert: last} = Lifecycle.process(alerts, %{}, @cooldown_ms, 6, "w", @now)

    assert length(emit) == 7
    assert List.last(emit).type == :alerts_coalesced
    # Only the 6 emitted individuals + the summary are stamped — the 2
    # dropped keys are NOT, so they remain eligible next tick (feeds F4).
    assert map_size(last) == 7
    refute Map.has_key?(last, {"w", :unanswered, "cid7"})
    refute Map.has_key?(last, {"w", :unanswered, "cid8"})
  end

  @tag regression: "F10"
  test "entries older than the cooldown window are evicted" do
    last = %{
      {"w", :unanswered, "dead-cid"} => @now - @cooldown_ms - 1,
      {"w", :stall} => @now - 1_000
    }

    %{last_alert: pruned} = Lifecycle.process([], last, @cooldown_ms, 6, "w", @now)

    refute Map.has_key?(pruned, {"w", :unanswered, "dead-cid"})
    assert Map.has_key?(pruned, {"w", :stall})
  end
end
