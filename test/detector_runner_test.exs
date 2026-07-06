defmodule DetectorRunnerTest do
  use ExUnit.Case, async: true
  alias Genswarms.Observer.DetectorRunner

  defmodule Good do
    @behaviour Genswarms.Observer.Detector
    def default_thresholds, do: %{"good.n" => 1}

    def detect(_fetched, ctx) do
      n = ctx.thresholds["good.n"]

      {[%{type: :good, swarm: ctx.swarm, at_ms: ctx.now_ms, summary: "n=#{n}", evidence: %{}}],
       (ctx.state || 0) + 1}
    end
  end

  defmodule Crasher do
    @behaviour Genswarms.Observer.Detector
    def detect(_f, _c), do: raise("boom")
  end

  defmodule Malformed do
    @behaviour Genswarms.Observer.Detector
    def detect(_f, ctx), do: {[%{oops: true}], %{ctx.state | poisoned: true}}
  end

  defmodule Forger do
    @behaviour Genswarms.Observer.Detector

    # Tries to forge provenance by pre-setting :source on its own returned
    # alert — the runner must overwrite this, never trust it.
    def detect(_f, ctx) do
      {[
         %{
           type: :sneaky,
           swarm: ctx.swarm,
           at_ms: ctx.now_ms,
           summary: "forged",
           evidence: %{},
           source: Genswarms.Observer.Detectors
         }
       ], nil}
    end
  end

  @fetched %{dashboard: {:ok, %{}}, events: {:ok, []}}

  test "success commits state, defaults key and cids, thresholds overlay" do
    {alerts, states, health} =
      DetectorRunner.run([Good], @fetched, "w", %{"good.n" => 9}, %{}, 1000)

    assert [%{key: {"w", :good}, cids: [], summary: "n=9", source: Good}] = alerts
    assert states[Good] == 1
    assert [%{module: Good, ok: true}] = health
  end

  test "normalize tags every alert with its source module, not client-controlled" do
    {alerts, _states, _health} =
      DetectorRunner.run([Good], @fetched, "w", %{"good.n" => 1}, %{}, 1000)

    assert [%{source: Good}] = alerts
  end

  test "a detector cannot forge its own :source — the runner's tag always wins" do
    {alerts, _states, _health} = DetectorRunner.run([Forger], @fetched, "w", %{}, %{}, 1000)

    assert [%{source: Forger}] = alerts
  end

  test "crash keeps prior state, emits detector_crashed, others still run" do
    {alerts, states, _} =
      DetectorRunner.run([Crasher, Good], @fetched, "w", %{}, %{Crasher => :prior}, 1000)

    assert states[Crasher] == :prior
    assert Enum.any?(alerts, &(&1.type == :detector_crashed))
    assert Enum.any?(alerts, &(&1.type == :good))
  end

  defmodule UnencodableEvidence do
    @behaviour Genswarms.Observer.Detector

    # evidence is a map (passes valid_alert?) but its VALUES can't survive
    # Jason — a pid and an engine backend-spec tuple, exactly the kind of
    # host value a custom detector leaks by accident.
    def detect(_f, ctx) do
      {[
         %{
           type: :weird,
           swarm: ctx.swarm,
           at_ms: ctx.now_ms,
           summary: "weird evidence",
           evidence: %{"pid" => self(), "spec" => {:bwrap, %{}}}
         }
       ], ctx.state}
    end
  end

  test "unencodable evidence is replaced with a bounded inspect — downstream Jason.encode! stays safe" do
    {alerts, _states, health} =
      DetectorRunner.run([UnencodableEvidence], @fetched, "w", %{}, %{}, 1000)

    assert [%{type: :weird, evidence: evidence}] = alerts
    # the invariant scope.ex relies on (alert_card / escalate Jason.encode!)
    assert {:ok, _} = Jason.encode(evidence)
    assert evidence["unencodable"] =~ "bwrap"
    # sanitized, not dropped — the detector itself ran fine
    assert [%{module: UnencodableEvidence, ok: true}] = health
  end

  test "malformed alerts dropped, state NOT committed" do
    {alerts, states, _} =
      DetectorRunner.run([Malformed], @fetched, "w", %{}, %{Malformed => %{poisoned: false}}, 1000)

    refute Enum.any?(alerts, &Map.has_key?(&1, :oops))
    assert Enum.any?(alerts, &(&1.type == :detector_invalid))
    assert states[Malformed] == %{poisoned: false}
  end

  defmodule TypedThresholdDetector do
    @behaviour Genswarms.Observer.Detector
    def default_thresholds, do: %{"typed.count" => 3}

    def detect(_fetched, ctx) do
      {[
         %{
           type: :typed_probe,
           swarm: ctx.swarm,
           at_ms: ctx.now_ms,
           summary: "count=#{inspect(ctx.thresholds["typed.count"])}",
           evidence: %{"count" => ctx.thresholds["typed.count"]}
         }
       ], ctx.state}
    end
  end

  describe "F6: threshold overrides are type-validated at merge" do
    @tag regression: "F6"
    test "a numeric-string override is coerced to the default's type" do
      {[alert], _, _} =
        Genswarms.Observer.DetectorRunner.run(
          [TypedThresholdDetector],
          %{},
          "w",
          %{"typed.count" => "5"},
          %{},
          1_000
        )

      assert alert.evidence["count"] == 5
    end

    @tag regression: "F6"
    test "a non-numeric override falls back to the module default" do
      {[alert], _, _} =
        Genswarms.Observer.DetectorRunner.run(
          [TypedThresholdDetector],
          %{},
          "w",
          %{"typed.count" => "lots"},
          %{},
          1_000
        )

      assert alert.evidence["count"] == 3
    end

    @tag regression: "F6"
    test "an override for a key with no default passes through untouched" do
      {[alert], _, _} =
        Genswarms.Observer.DetectorRunner.run(
          [TypedThresholdDetector],
          %{},
          "w",
          %{"typed.count" => 4, "someone.elses" => "free"},
          %{},
          1_000
        )

      assert alert.evidence["count"] == 4
    end
  end
end
