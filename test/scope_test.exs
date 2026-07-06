defmodule Genswarms.Observer.ScopeTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Client
  alias Genswarms.Observer.Objects.Scope

  @t0 1_751_734_800_000

  # These tests exercise alerting/detector behavior, not durability — the
  # real `Store.InMemory` default is a process-wide singleton (by design,
  # see store.ex) and would leak `last_alert`/`det` across async tests.
  defmodule NullStore do
    @behaviour Genswarms.Observer.Store
    def load, do: :empty
    def save(_saved), do: :ok
  end

  defp iso(ms) when is_integer(ms),
    do: ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()

  defp healthy_dashboard do
    %{
      "swarm" => "wingston",
      "status" => "running",
      "summary" => %{"pool" => %{"leased" => 0, "size" => 4}},
      "nodes" => [%{"name" => "worker", "type" => "agent", "state" => "idle"}],
      "sessions" => [],
      "warnings" => []
    }
  end

  defp healthy_fixture do
    %{
      dashboard: {:ok, healthy_dashboard()},
      events: {:ok, [%{"id" => 1, "timestamp" => iso(@t0 - 1_000), "level" => "info"}]}
    }
  end

  # A healthy dashboard (no detector alerts) carrying one final, unseen
  # `conversation_topics` period — isolates the digest path from the
  # alert-cooldown machinery in the tests below.
  defp topics_envelope(swarm, period_id) do
    healthy_dashboard()
    |> Map.put("swarm", swarm)
    |> Map.put("extensions", %{
      "conversation_topics" => %{
        "v" => 1,
        "coverage" => "dm",
        "periods" => [
          %{
            "period_id" => period_id,
            "final" => true,
            "status" => "ok",
            "generated_at" => "#{period_id}T00:00:00Z",
            "source_watermark" => 1,
            "topics" => [%{"label" => "billing", "count" => 3}],
            "counts" => %{"conversations" => 5, "turns" => 9},
            "signals" => []
          }
        ]
      }
    })
  end

  defp start_scope(opts \\ []) do
    {:ok, fake} = Client.Fake.start_link(Keyword.get(opts, :fixture, %{"wingston" => healthy_fixture()}))
    {:ok, clock} = Agent.start_link(fn -> @t0 end)
    {:ok, outbox} = Agent.start_link(fn -> [] end)

    config =
      Map.merge(
        %{
          swarm_name: "observer",
          registry: %{
            "wingston" => %{
              dashboard_url: "http://dash.example:4994",
              token_env: Keyword.get(opts, :token_env),
              repo: "genlayerlabs/wingston-rally-bot"
            }
          },
          tick_sources: ["cron"],
          read_sources: ["diagnostico"],
          alert_conversation_id: "tg:42:0",
          client: Client.Fake,
          client_opts: [fake: fake],
          store_mod: NullStore,
          now_fn: fn -> Agent.get(clock, & &1) end,
          deliver_fn: fn target, from, content ->
            Agent.update(outbox, &[%{target: target, from: from, content: content} | &1])
            :ok
          end
        },
        Keyword.get(opts, :config, %{})
      )

    {:ok, state} = Scope.init(config)
    %{state: state, fake: fake, clock: clock, outbox: outbox}
  end

  defp sent(outbox), do: outbox |> Agent.get(& &1) |> Enum.reverse()

  defp advance(clock, ms), do: Agent.update(clock, &(&1 + ms))

  defp tick(state), do: Scope.handle_message(:cron, ~s({"action":"tick"}), state)

  defp decode_reply({:reply, json, state}), do: {Jason.decode!(json), state}

  # ── init / config normalization ───────────────────────────────────────────

  test "init normalizes atom- and string-keyed registries identically" do
    {:ok, s1} =
      Scope.init(%{registry: %{"w" => %{dashboard_url: "http://x", token_env: "T", repo: "o/r"}}})

    {:ok, s2} =
      Scope.init(%{
        "registry" => %{"w" => %{"dashboard_url" => "http://x", "token_env" => "T", "repo" => "o/r"}}
      })

    assert s1.registry == s2.registry
    assert s1.registry["w"]["dashboard_url"] == "http://x"
  end

  test "allowlists are fail-closed: default config trusts nobody" do
    {:ok, state} = Scope.init(%{})
    assert {:noreply, _} = Scope.handle_message(:cron, ~s({"action":"tick"}), state)
    assert {:noreply, _} = Scope.handle_message(:agent, ~s({"action":"status"}), state)
  end

  test "garbage input is ignored" do
    %{state: state} = start_scope()
    assert {:noreply, _} = Scope.handle_message(:cron, "not json", state)
    assert {:noreply, _} = Scope.handle_message(:cron, ~s({"action":"selfdestruct"}), state)
  end

  # ── tick: happy path ──────────────────────────────────────────────────────

  test "healthy tick replies counters and sends nothing" do
    %{state: state, outbox: outbox} = start_scope()

    {reply, _state} = decode_reply(tick(state))
    assert reply == %{"ok" => true, "checked" => 1, "alerts" => 0, "suppressed" => 0}
    assert sent(outbox) == []
  end

  test "tick from a source not in tick_sources is dropped" do
    %{state: state, outbox: outbox} = start_scope()
    assert {:noreply, _} = Scope.handle_message(:impostor, ~s({"action":"tick"}), state)
    assert sent(outbox) == []
  end

  # ── tick: alerting ────────────────────────────────────────────────────────

  test "endpoint_down sends a card to :sender with deep-link and investigation prompt" do
    %{state: state, outbox: outbox} =
      start_scope(fixture: %{"wingston" => %{dashboard: {:error, :econnrefused}}})

    {reply, _state} = decode_reply(tick(state))
    assert reply["alerts"] == 1

    [delivery] = sent(outbox)
    assert delivery.target == :sender
    assert delivery.from == :scope

    msg = Jason.decode!(delivery.content)
    assert msg["action"] == "send_card"
    assert msg["conversation_id"] == "tg:42:0"
    assert msg["card"]["title"] =~ "wingston"
    assert msg["card"]["title"] =~ "endpoint_down"

    texts = Enum.map(msg["card"]["blocks"], & &1["text"])
    assert Enum.any?(texts, &(&1 =~ "http://dash.example:4994/api/swarms/wingston/dashboard"))
    assert Enum.any?(texts, &(&1 =~ "genswarms-fleet"))
    assert Enum.any?(texts, &(&1 =~ "github.com/genlayerlabs/wingston-rally-bot"))
  end

  test "cooldown: same (swarm,type) is suppressed within the window and refires after" do
    %{state: state, outbox: outbox, clock: clock} =
      start_scope(fixture: %{"wingston" => %{dashboard: {:error, :econnrefused}}})

    {reply, state} = decode_reply(tick(state))
    assert reply["alerts"] == 1

    advance(clock, 5 * 60_000)
    {reply, state} = decode_reply(tick(state))
    assert reply == %{"ok" => true, "checked" => 1, "alerts" => 0, "suppressed" => 1}
    assert length(sent(outbox)) == 1

    advance(clock, 30 * 60_000)
    {reply, _state} = decode_reply(tick(state))
    assert reply["alerts"] == 1
    assert length(sent(outbox)) == 2
  end

  test "cooldown_minutes is configurable" do
    %{state: state, clock: clock, outbox: outbox} =
      start_scope(
        fixture: %{"wingston" => %{dashboard: {:error, :down}}},
        config: %{cooldown_minutes: 1}
      )

    {_, state} = decode_reply(tick(state))
    advance(clock, 61_000)
    {reply, _} = decode_reply(tick(state))
    assert reply["alerts"] == 1
    assert length(sent(outbox)) == 2
  end

  test "detector state threads across ticks (pool saturation arms then fires)" do
    saturated = %{
      dashboard:
        {:ok,
         %{
           "summary" => %{"pool" => %{"leased" => 4, "size" => 4}},
           "nodes" => [],
           "status" => "running"
         }},
      events: {:ok, [%{"id" => 1, "timestamp" => iso(@t0), "level" => "info"}]}
    }

    %{state: state, clock: clock, outbox: outbox} =
      start_scope(fixture: %{"wingston" => saturated})

    {reply, state} = decode_reply(tick(state))
    assert reply["alerts"] == 0

    advance(clock, 130_000)
    {reply, _state} = decode_reply(tick(state))
    assert reply["alerts"] == 1

    [delivery] = sent(outbox)
    assert Jason.decode!(delivery.content)["card"]["title"] =~ "pool_saturated"
  end

  test "a crashing client reads as endpoint_down, not an object crash" do
    defmodule BoomClient do
      def get_dashboard(_, _, _, _), do: raise("boom")
      def get_events(_, _, _, _), do: raise("boom")
    end

    %{state: state, outbox: outbox} = start_scope(config: %{client: BoomClient})

    {reply, _} = decode_reply(tick(state))
    assert reply["alerts"] == 1
    [delivery] = sent(outbox)
    assert Jason.decode!(delivery.content)["card"]["title"] =~ "endpoint_down"
  end

  # ── per-swarm alert budget ────────────────────────────────────────────────

  test "a swarm firing more than the per-tick budget gets the overflow coalesced into one alert" do
    defmodule ManyAlertsDetector do
      @behaviour Genswarms.Observer.Detector

      def detect(_fetched, ctx) do
        alerts =
          for i <- 1..8 do
            %{
              type: :"synthetic_#{i}",
              swarm: ctx.swarm,
              at_ms: ctx.now_ms,
              summary: "alert #{i}",
              evidence: %{}
            }
          end

        {alerts, ctx.state}
      end
    end

    %{state: state, outbox: outbox} = start_scope()
    state = %{state | detectors: [ManyAlertsDetector]}

    {reply, _state} = decode_reply(tick(state))
    # 6 kept + 1 synthetic :alerts_coalesced summary for the other 2
    assert reply["alerts"] == 7

    cards = sent(outbox) |> Enum.map(&Jason.decode!(&1.content))
    assert length(cards) == 7
    assert Enum.count(cards, &(&1["card"]["title"] =~ "alerts_coalesced")) == 1

    [coalesced] = Enum.filter(cards, &(&1["card"]["title"] =~ "alerts_coalesced"))
    evidence_text = Enum.find(coalesced["card"]["blocks"], &(&1["text"] =~ "evidence"))["text"]
    assert evidence_text =~ "synthetic_7"
    assert evidence_text =~ "synthetic_8"
  end

  # ── escalation (fase 3) ───────────────────────────────────────────────────

  test "with escalate_to set, an emitted alert also becomes a diagnosis task" do
    %{state: state, outbox: outbox} =
      start_scope(
        fixture: %{"wingston" => %{dashboard: {:error, :econnrefused}}},
        config: %{escalate_to: :diagnostico}
      )

    {reply, _} = decode_reply(tick(state))
    assert reply["alerts"] == 1

    assert [card, task] = sent(outbox)
    assert card.target == :sender
    assert task.target == :diagnostico
    assert task.content =~ "endpoint_down"
    assert task.content =~ ~s({"action":"get_events","swarm":"wingston"})
    assert task.content =~ "NO network"
  end

  test "escalation respects the cooldown (a suppressed alert does not escalate)" do
    %{state: state, outbox: outbox} =
      start_scope(
        fixture: %{"wingston" => %{dashboard: {:error, :econnrefused}}},
        config: %{escalate_to: :diagnostico}
      )

    {_, state} = decode_reply(tick(state))
    {reply, _} = decode_reply(tick(state))

    assert reply["suppressed"] == 1
    # one card + one escalation from the FIRST tick only
    assert length(sent(outbox)) == 2
  end

  test "without escalate_to nothing is escalated (default off)" do
    %{state: state, outbox: outbox} =
      start_scope(fixture: %{"wingston" => %{dashboard: {:error, :econnrefused}}})

    {_, _} = decode_reply(tick(state))
    assert [%{target: :sender}] = sent(outbox)
  end

  # ── tokens ────────────────────────────────────────────────────────────────

  test "token_env resolves through the environment at fetch time" do
    System.put_env("OBSERVER_TEST_DASH_TOKEN", "sekrit")
    on_exit(fn -> System.delete_env("OBSERVER_TEST_DASH_TOKEN") end)

    %{state: state, fake: fake} = start_scope(token_env: "OBSERVER_TEST_DASH_TOKEN")
    {_, _} = decode_reply(tick(state))

    assert [%{token: "sekrit"} | _] = Client.Fake.calls(fake)
  end

  test "unset token_env fetches with nil token (loopback dashboards)" do
    %{state: state, fake: fake} = start_scope(token_env: "OBSERVER_TEST_UNSET_VAR")
    {_, _} = decode_reply(tick(state))
    assert [%{token: nil} | _] = Client.Fake.calls(fake)
  end

  # ── agent-facing reads ────────────────────────────────────────────────────

  test "get_dashboard replies the envelope to read_sources only" do
    %{state: state} = start_scope()

    {:reply, json, _} =
      Scope.handle_message(:diagnostico, ~s({"action":"get_dashboard","swarm":"wingston"}), state)

    assert %{"ok" => true, "dashboard" => %{"status" => "running"}} = Jason.decode!(json)

    assert {:noreply, _} =
             Scope.handle_message(:impostor, ~s({"action":"get_dashboard","swarm":"wingston"}), state)
  end

  test "get_events replies the raw event list" do
    %{state: state} = start_scope()

    {:reply, json, _} =
      Scope.handle_message(:diagnostico, ~s({"action":"get_events","swarm":"wingston"}), state)

    assert %{"ok" => true, "events" => [%{"id" => 1}]} = Jason.decode!(json)
  end

  test "reads on an unobserved swarm answer an error, never fetch" do
    %{state: state, fake: fake} = start_scope()

    {:reply, json, _} =
      Scope.handle_message(:diagnostico, ~s({"action":"get_dashboard","swarm":"otro"}), state)

    assert %{"ok" => false, "error" => error} = Jason.decode!(json)
    assert error =~ "not observed"
    assert Client.Fake.calls(fake) == []
  end

  # ── status / dashboard extension ──────────────────────────────────────────

  test "status reports watching, thresholds, last tick and recent alerts" do
    %{state: state} = start_scope(fixture: %{"wingston" => %{dashboard: {:error, :down}}})

    {_, state} = decode_reply(tick(state))

    {:reply, json, _} = Scope.handle_message(:cron, ~s({"action":"status"}), state)
    status = Jason.decode!(json)

    assert status["watching"] == ["wingston"]
    assert status["last_tick_ms"] == @t0
    assert [%{"type" => "endpoint_down", "swarm" => "wingston"}] = status["recent_alerts"]
    assert status["thresholds"] == %{}
  end

  test "dashboard/1 exposes the observer extension" do
    %{state: state} = start_scope(fixture: %{"wingston" => %{dashboard: {:error, :down}}})
    {_, state} = decode_reply(tick(state))

    assert [%{kind: :extension, name: "observer", data: %{count: 1, items: [item]}}] =
             Scope.dashboard(state)

    assert item.type == :endpoint_down
  end

  # ── digest (O4) ───────────────────────────────────────────────────────────

  test "a fresh unseen digest period sends one card to :sender and marks it seen" do
    envelope = topics_envelope("wingston", "2026-07-01")

    %{state: state, outbox: outbox} =
      start_scope(fixture: %{"wingston" => %{dashboard: {:ok, envelope}, events: {:ok, []}}})

    {reply, state} = decode_reply(tick(state))
    assert reply["alerts"] == 0

    [delivery] = sent(outbox)
    assert delivery.target == :sender

    msg = Jason.decode!(delivery.content)
    assert msg["action"] == "send_card"
    assert msg["conversation_id"] == "tg:42:0"
    assert msg["card"]["title"] =~ "digest: wingston · 2026-07-01"

    assert state.seen_periods["wingston"] == MapSet.new(["2026-07-01"])

    # ticking again with the same (now-seen) period sends nothing more.
    {_reply, _state} = decode_reply(tick(state))
    assert length(sent(outbox)) == 1
  end

  test "a malformed conversation_topics extension never crashes a tick, and sends no digest card" do
    # Map-shaped (so the unrelated TopicsStale detector's own get_in reads
    # stay well-formed and silent — a swarm that's never shown a periods
    # list before raises nothing there) but wrong/garbage at the fields
    # Digest.plan/2 actually cares about.
    bad_envelope =
      Map.put(healthy_dashboard(), "extensions", %{
        "conversation_topics" => %{"v" => 1, "periods" => "not-a-list"}
      })

    %{state: state, outbox: outbox} =
      start_scope(fixture: %{"wingston" => %{dashboard: {:ok, bad_envelope}, events: {:ok, []}}})

    {reply, state} = decode_reply(tick(state))
    assert reply["ok"] == true
    assert reply["alerts"] == 0
    assert sent(outbox) == []
    assert Map.get(state.seen_periods, "wingston", MapSet.new()) == MapSet.new()
  end

  test "seen-after-send: a failed digest delivery marks nothing, and the whole batch retries next tick" do
    envelope = topics_envelope("wingston", "2026-07-01")

    {:ok, gate} = Agent.start_link(fn -> :fail end)
    {:ok, mine} = Agent.start_link(fn -> [] end)

    %{state: state, clock: clock} =
      start_scope(
        fixture: %{"wingston" => %{dashboard: {:ok, envelope}, events: {:ok, []}}},
        config: %{
          deliver_fn: fn target, from, content ->
            Agent.update(mine, &[%{target: target, from: from, content: content} | &1])
            Agent.get(gate, & &1)
          end
        }
      )

    {reply, state} = decode_reply(tick(state))
    assert reply["alerts"] == 0
    assert length(Agent.get(mine, & &1)) == 1
    assert Map.get(state.seen_periods, "wingston", MapSet.new()) == MapSet.new()

    Agent.update(gate, fn _ -> :ok end)
    advance(clock, 60_000)
    {_reply, state} = decode_reply(tick(state))

    # the whole batch (one card, here) was retried and now delivered.
    assert length(Agent.get(mine, & &1)) == 2
    assert state.seen_periods["wingston"] == MapSet.new(["2026-07-01"])

    # a third tick has nothing new to say — no further send.
    advance(clock, 60_000)
    {_reply, _state} = decode_reply(tick(state))
    assert length(Agent.get(mine, & &1)) == 2
  end

  test "two swarms ticked together: a failing sender for one leaves only the other's periods marked seen" do
    envelope_a = topics_envelope("alpha", "2026-07-01")
    envelope_b = topics_envelope("beta", "2026-07-01")

    {:ok, fake} =
      Client.Fake.start_link(%{
        "alpha" => %{dashboard: {:ok, envelope_a}, events: {:ok, []}},
        "beta" => %{dashboard: {:ok, envelope_b}, events: {:ok, []}}
      })

    {:ok, clock} = Agent.start_link(fn -> @t0 end)
    {:ok, mine} = Agent.start_link(fn -> [] end)

    config = %{
      swarm_name: "observer",
      registry: %{
        "alpha" => %{dashboard_url: "http://a.example", token_env: nil, repo: nil},
        "beta" => %{dashboard_url: "http://b.example", token_env: nil, repo: nil}
      },
      tick_sources: ["cron"],
      read_sources: [],
      alert_conversation_id: "tg:42:0",
      client: Client.Fake,
      client_opts: [fake: fake],
      store_mod: NullStore,
      now_fn: fn -> Agent.get(clock, & &1) end,
      deliver_fn: fn target, from, content ->
        Agent.update(mine, &[%{target: target, from: from, content: content} | &1])
        if String.contains?(content, "alpha"), do: {:error, :boom}, else: :ok
      end
    }

    {:ok, state} = Scope.init(config)
    {reply, state} = decode_reply(tick(state))
    assert reply["alerts"] == 0

    assert length(Agent.get(mine, & &1)) == 2
    assert Map.get(state.seen_periods, "alpha", MapSet.new()) == MapSet.new()
    assert state.seen_periods["beta"] == MapSet.new(["2026-07-01"])
  end

  # ── pipeline health (O7) ──────────────────────────────────────────────────

  defp status_health(state) do
    {:reply, json, _} = Scope.handle_message(:cron, ~s({"action":"status"}), state)
    Jason.decode!(json)["health"]
  end

  test "healthy tick: every touched stage records last_success_ms and no last_error" do
    # topics envelope so the digest actually plans+sends a card — that
    # touches all five stages (fetch, decode, detectors, digest, sender).
    envelope = topics_envelope("wingston", "2026-07-01")

    %{state: state} =
      start_scope(fixture: %{"wingston" => %{dashboard: {:ok, envelope}, events: {:ok, []}}})

    {reply, state} = decode_reply(tick(state))
    assert reply["ok"] == true

    health = status_health(state)["wingston"]

    for stage <- ~w(fetch decode detectors digest sender) do
      assert health[stage]["last_success_ms"] == @t0, "stage #{stage} missing success stamp"
      assert health[stage]["last_error"] == nil, "stage #{stage} unexpectedly errored"
    end
  end

  test "failing client: fetch errors with last_success_ms nil, and the other swarm stays healthy" do
    {:ok, fake} =
      Client.Fake.start_link(%{
        # alpha's endpoint is down (no fixture entries -> {:error, :not_configured})
        "alpha" => %{},
        "beta" => healthy_fixture()
      })

    {:ok, clock} = Agent.start_link(fn -> @t0 end)
    {:ok, outbox} = Agent.start_link(fn -> [] end)

    config = %{
      swarm_name: "observer",
      registry: %{
        "alpha" => %{dashboard_url: "http://a.example", token_env: nil, repo: nil},
        "beta" => %{dashboard_url: "http://b.example", token_env: nil, repo: nil}
      },
      tick_sources: ["cron"],
      read_sources: [],
      alert_conversation_id: "tg:42:0",
      client: Client.Fake,
      client_opts: [fake: fake],
      store_mod: NullStore,
      now_fn: fn -> Agent.get(clock, & &1) end,
      deliver_fn: fn target, from, content ->
        Agent.update(outbox, &[%{target: target, from: from, content: content} | &1])
        :ok
      end
    }

    {:ok, state} = Scope.init(config)
    {_reply, state} = decode_reply(tick(state))
    health = status_health(state)

    # alpha: fetch errored, never succeeded; decode never ran (nothing to
    # decode); detectors still ran fine (endpoint_down is THEIR verdict).
    assert health["alpha"]["fetch"]["last_success_ms"] == nil
    assert health["alpha"]["fetch"]["last_error"] =~ "not_configured"
    refute Map.has_key?(health["alpha"], "decode")
    assert health["alpha"]["detectors"]["last_error"] == nil
    assert health["alpha"]["detectors"]["last_success_ms"] == @t0

    # beta: per-swarm isolation — alpha's broken endpoint leaks nothing here.
    assert health["beta"]["fetch"]["last_success_ms"] == @t0
    assert health["beta"]["fetch"]["last_error"] == nil
    assert health["beta"]["decode"]["last_success_ms"] == @t0
    assert health["beta"]["detectors"]["last_error"] == nil
  end

  test "fetch recovery clears last_error and stamps last_success_ms" do
    %{state: state, fake: fake, clock: clock} =
      start_scope(fixture: %{"wingston" => %{dashboard: {:error, :econnrefused}}})

    {_reply, state} = decode_reply(tick(state))
    health = status_health(state)["wingston"]
    assert health["fetch"]["last_error"] =~ "econnrefused"
    assert health["fetch"]["last_success_ms"] == nil

    Client.Fake.put(fake, "wingston", healthy_fixture())
    advance(clock, 60_000)
    {_reply, state} = decode_reply(tick(state))

    health = status_health(state)["wingston"]
    assert health["fetch"]["last_error"] == nil
    assert health["fetch"]["last_success_ms"] == @t0 + 60_000
  end

  test "a crashing detector marks the detectors stage errored, naming the module" do
    defmodule CrashingDetector do
      @behaviour Genswarms.Observer.Detector
      def detect(_fetched, _ctx), do: raise("kaboom")
    end

    %{state: state} = start_scope()
    state = %{state | detectors: [CrashingDetector]}

    {reply, state} = decode_reply(tick(state))
    # the crash still surfaces as a detector_crashed alert, as before
    assert reply["alerts"] == 1

    health = status_health(state)["wingston"]
    assert health["detectors"]["last_error"] =~ "CrashingDetector"
    assert health["detectors"]["last_success_ms"] == nil
    # the crash never poisons the other stages
    assert health["fetch"]["last_error"] == nil
  end

  test "a malformed conversation_topics extension marks the decode stage errored" do
    bad_envelope =
      Map.put(healthy_dashboard(), "extensions", %{
        "conversation_topics" => %{"v" => 1, "periods" => "not-a-list"}
      })

    %{state: state} =
      start_scope(fixture: %{"wingston" => %{dashboard: {:ok, bad_envelope}, events: {:ok, []}}})

    {reply, state} = decode_reply(tick(state))
    assert reply["ok"] == true

    health = status_health(state)["wingston"]
    assert health["decode"]["last_error"] =~ "malformed"
    assert health["decode"]["last_success_ms"] == nil
    assert health["fetch"]["last_error"] == nil
  end

  test "a failing deliver_fn marks the sender (and digest) stages errored" do
    envelope = topics_envelope("wingston", "2026-07-01")

    %{state: state} =
      start_scope(
        fixture: %{"wingston" => %{dashboard: {:ok, envelope}, events: {:ok, []}}},
        config: %{deliver_fn: fn _target, _from, _content -> {:error, :boom} end}
      )

    {_reply, state} = decode_reply(tick(state))
    health = status_health(state)["wingston"]

    assert health["sender"]["last_error"] =~ "boom"
    assert health["sender"]["last_success_ms"] == nil
    assert health["digest"]["last_error"] =~ "planned 1 card(s), delivered 0"
    # the pipeline up to the sender was fine
    assert health["fetch"]["last_error"] == nil
    assert health["decode"]["last_error"] == nil
  end

  test "dashboard/1 exposes the compact per-swarm health summary" do
    %{state: state} = start_scope(fixture: %{"wingston" => %{dashboard: {:error, :down}}})
    {_reply, state} = decode_reply(tick(state))

    assert [%{kind: :extension, name: "observer", data: data}] = Scope.dashboard(state)
    assert data.health == %{"wingston" => %{ok: false, failing: [:fetch]}}

    # and status carries the full map alongside
    health = status_health(state)["wingston"]
    assert health["fetch"]["last_error"] != nil
  end

  test "dashboard summary reads ok:true for a healthy swarm and before any tick" do
    %{state: state} = start_scope()

    # before any tick: no health entries at all — summary is empty, not failing
    assert [%{data: %{health: pre}}] = Scope.dashboard(state)
    assert pre == %{}

    {_reply, state} = decode_reply(tick(state))
    assert [%{data: %{health: post}}] = Scope.dashboard(state)
    assert post == %{"wingston" => %{ok: true, failing: []}}
  end
end
