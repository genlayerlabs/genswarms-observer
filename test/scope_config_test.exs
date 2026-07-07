defmodule Genswarms.Observer.ScopeConfigTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Client
  alias Genswarms.Observer.Objects.Scope

  # Same rationale as scope_test.exs: the real Store.InMemory default is a
  # process-wide singleton and would leak last_alert/det across async tests.
  defmodule NullStore do
    @behaviour Genswarms.Observer.Store
    def load, do: :empty
    def save(_saved), do: :ok
  end

  # Always alerts once per swarm per tick, tagging the alert's swarm from
  # ctx — enough to observe which swarm(s) a custom detector actually ran
  # against without any extra plumbing.
  defmodule ProbeDetector do
    @behaviour Genswarms.Observer.Detector

    def detect(_fetched, ctx) do
      alert = %{
        type: :probe_alert,
        swarm: ctx.swarm,
        at_ms: ctx.now_ms,
        summary: "probe fired for #{ctx.swarm}",
        evidence: %{}
      }

      {[alert], ctx.state}
    end
  end

  defmodule NotADetector do
    def hello, do: :world
  end

  defmodule CollidingThresholdsA do
    @behaviour Genswarms.Observer.Detector
    def default_thresholds, do: %{"shared_key" => 1}
    def detect(_fetched, ctx), do: {[], ctx.state}
  end

  defmodule CollidingThresholdsB do
    @behaviour Genswarms.Observer.Detector
    def default_thresholds, do: %{"shared_key" => 2}
    def detect(_fetched, ctx), do: {[], ctx.state}
  end

  defp healthy_dashboard do
    %{
      "status" => "running",
      "summary" => %{"pool" => %{"leased" => 0, "size" => 4}},
      "nodes" => [%{"name" => "worker", "type" => "agent", "state" => "idle"}],
      "sessions" => [],
      "warnings" => []
    }
  end

  defp healthy_fixture, do: %{dashboard: {:ok, healthy_dashboard()}, events: {:ok, []}}

  defp base_config(overrides) do
    Map.merge(
      %{
        swarm_name: "observer",
        registry: %{
          "wingston" => %{dashboard_url: "http://a.example", token_env: nil, repo: nil}
        },
        tick_sources: ["cron"],
        read_sources: [],
        store_mod: NullStore
      },
      overrides
    )
  end

  # ── fail-closed module resolution ────────────────────────────────────────

  test "an unresolvable custom detector module raises at init, naming the module" do
    config =
      base_config(%{
        custom_detectors: ["Elixir.Nonexistent.Detector.Module.ThatIsNotLoaded"]
      })

    assert_raise ArgumentError, ~r/Nonexistent\.Detector\.Module\.ThatIsNotLoaded/, fn ->
      Scope.init(config)
    end
  end

  test "a custom detector module missing detect/2 raises at init, naming the module" do
    config = base_config(%{custom_detectors: [NotADetector]})

    assert_raise ArgumentError, ~r/does not export detect\/2/, fn ->
      Scope.init(config)
    end
  end

  test "a malformed custom_detectors entry raises at init" do
    config = base_config(%{custom_detectors: [123]})

    assert_raise ArgumentError, ~r/custom_detectors/, fn ->
      Scope.init(config)
    end
  end

  test "a map entry missing :module raises at init" do
    config = base_config(%{custom_detectors: [%{swarms: ["wingston"]}]})

    assert_raise ArgumentError, ~r/missing its required :module key/, fn ->
      Scope.init(config)
    end
  end

  test "a resolvable custom detector (atom, scoped) boots cleanly" do
    config =
      base_config(%{custom_detectors: [%{module: ProbeDetector, swarms: ["wingston"]}]})

    assert {:ok, state} = Scope.init(config)
    assert [%{module: ProbeDetector, swarms: ["wingston"]}] = state.custom_detectors
  end

  test "a bare module entry (no swarms map) is global" do
    config = base_config(%{custom_detectors: [ProbeDetector]})

    assert {:ok, state} = Scope.init(config)
    assert [%{module: ProbeDetector, swarms: nil}] = state.custom_detectors
  end

  # ── threshold boot check ─────────────────────────────────────────────────

  test "a default_thresholds/0 key collision across detector modules raises at init" do
    config =
      base_config(%{
        custom_detectors: [CollidingThresholdsA, CollidingThresholdsB]
      })

    error =
      assert_raise ArgumentError, fn ->
        Scope.init(config)
      end

    assert error.message =~ "shared_key"
    assert error.message =~ "CollidingThresholdsA"
    assert error.message =~ "CollidingThresholdsB"
  end

  test "distinct threshold keys across custom + built-in detectors boot fine" do
    config = base_config(%{custom_detectors: [ProbeDetector]})
    assert {:ok, _state} = Scope.init(config)
  end

  # ── per-swarm scoping (observable via a tick) ────────────────────────────

  test "a swarm-scoped custom detector only fires for its scoped swarm, not others" do
    {:ok, fake} =
      Client.Fake.start_link(%{
        "mm" => healthy_fixture(),
        "wingston" => healthy_fixture()
      })

    {:ok, clock} = Agent.start_link(fn -> 1_751_734_800_000 end)
    {:ok, outbox} = Agent.start_link(fn -> [] end)

    config =
      base_config(%{
        registry: %{
          "mm" => %{dashboard_url: "http://mm.example", token_env: nil, repo: nil},
          "wingston" => %{dashboard_url: "http://wingston.example", token_env: nil, repo: nil}
        },
        client: Client.Fake,
        client_opts: [fake: fake],
        alert_conversation_id: "tg:1:0",
        sender: :sender,
        now_fn: fn -> Agent.get(clock, & &1) end,
        deliver_fn: fn target, from, content ->
          Agent.update(outbox, &[%{target: target, from: from, content: content} | &1])
          :ok
        end,
        custom_detectors: [%{module: ProbeDetector, swarms: ["mm"]}]
      })

    {:ok, state} = Scope.init(config)
    {:reply, _json, _state} = Scope.handle_message(:cron, ~s({"action":"tick"}), state)

    cards =
      outbox
      |> Agent.get(& &1)
      |> Enum.reverse()
      |> Enum.map(&Jason.decode!(&1.content))

    probe_cards = Enum.filter(cards, &(&1["card"]["title"] =~ "probe_alert"))
    assert length(probe_cards) == 1
    assert hd(probe_cards)["card"]["title"] =~ "mm"
    refute hd(probe_cards)["card"]["title"] =~ "wingston"
  end

  test "a global custom detector (no swarms scoping) fires for every observed swarm" do
    {:ok, fake} =
      Client.Fake.start_link(%{
        "mm" => healthy_fixture(),
        "wingston" => healthy_fixture()
      })

    {:ok, clock} = Agent.start_link(fn -> 1_751_734_800_000 end)
    {:ok, outbox} = Agent.start_link(fn -> [] end)

    config =
      base_config(%{
        registry: %{
          "mm" => %{dashboard_url: "http://mm.example", token_env: nil, repo: nil},
          "wingston" => %{dashboard_url: "http://wingston.example", token_env: nil, repo: nil}
        },
        client: Client.Fake,
        client_opts: [fake: fake],
        alert_conversation_id: "tg:1:0",
        sender: :sender,
        now_fn: fn -> Agent.get(clock, & &1) end,
        deliver_fn: fn target, from, content ->
          Agent.update(outbox, &[%{target: target, from: from, content: content} | &1])
          :ok
        end,
        custom_detectors: [ProbeDetector]
      })

    {:ok, state} = Scope.init(config)
    {:reply, _json, _state} = Scope.handle_message(:cron, ~s({"action":"tick"}), state)

    cards =
      outbox
      |> Agent.get(& &1)
      |> Enum.reverse()
      |> Enum.map(&Jason.decode!(&1.content))

    probe_cards = Enum.filter(cards, &(&1["card"]["title"] =~ "probe_alert"))
    assert length(probe_cards) == 2
    swarms_hit = Enum.map(probe_cards, & &1["card"]["title"]) |> Enum.sort()
    assert Enum.any?(swarms_hit, &(&1 =~ "mm"))
    assert Enum.any?(swarms_hit, &(&1 =~ "wingston"))
  end

  # ── Task 6: signal_rules — operator config, fail-CLOSED ──────────────────

  test "an invalid signal_rules operator rule raises at init, naming the rule id" do
    config =
      base_config(%{
        signal_rules: [
          %{
            "block" => "cron",
            "id" => "Not Valid!",
            "card" => "x",
            "when" => %{"op" => "gt", "lhs" => "now", "rhs" => 0}
          }
        ]
      })

    assert_raise ArgumentError, ~r/Not Valid!/, fn -> Scope.init(config) end
  end

  test "a signal_rules entry missing its \"block\" key raises at init" do
    config =
      base_config(%{
        signal_rules: [%{"id" => "x", "card" => "x", "when" => %{"op" => "gt", "lhs" => 1, "rhs" => 0}}]
      })

    assert_raise ArgumentError, ~r/"block"/, fn -> Scope.init(config) end
  end

  test "a non-list signal_rules raises at init" do
    config = base_config(%{signal_rules: %{"block" => "cron"}})
    assert_raise ArgumentError, ~r/signal_rules/, fn -> Scope.init(config) end
  end

  test "valid signal_rules entries boot cleanly, grouped by block" do
    config =
      base_config(%{
        signal_rules: [
          %{
            "block" => "cron",
            "id" => "always",
            "card" => "x",
            "when" => %{"op" => "gt", "lhs" => "now", "rhs" => 0}
          },
          %{
            "block" => "metrics_today",
            "id" => "other",
            "card" => "y",
            "when" => %{"op" => "gt", "lhs" => "now", "rhs" => 0}
          }
        ]
      })

    assert {:ok, state} = Scope.init(config)
    assert %{"cron" => [%{"id" => "always"}], "metrics_today" => [%{"id" => "other"}]} =
             state.signal_rules_by_block
  end

  test "default (empty) signal_rules boots cleanly" do
    assert {:ok, state} = Scope.init(base_config(%{}))
    assert state.signal_rules_by_block == %{}
  end
end
