defmodule Genswarms.Observer.ScopeTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Client
  alias Genswarms.Observer.Objects.Scope

  @t0 1_751_734_800_000

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

  # ── escalada (fase 3) ─────────────────────────────────────────────────────

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
    assert task.content =~ "NO tienes red"
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
end
