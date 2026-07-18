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

  test "a real endpoint_down (refused) reads plainly and keeps the investigate tail" do
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
    assert msg["card"]["title"] =~ "unreachable"

    # 2026-07-09 redesign: no internal deep-links or MCP boilerplate — a human
    # sentence plus the machine tail (a REAL fetch failure stays investigable;
    # only the restart-shaped swarm_not_found blip is quiet).
    texts = Enum.map(msg["card"]["blocks"], & &1["text"])
    refute Enum.any?(texts, &(&1 =~ "http://dash.example"))
    refute Enum.any?(texts, &(&1 =~ "genswarms-fleet"))
    assert Enum.any?(texts, &(&1 =~ "paste to Claude"))
  end

  test "dashboard down but events alive triages as dashboard_slow, not endpoint_down" do
    %{state: state, outbox: outbox} =
      start_scope(
        fixture: %{
          "wingston" => %{
            dashboard: {:error, :timeout},
            events: {:ok, [%{"id" => 1, "timestamp" => iso(@t0 - 1_000), "level" => "info"}]}
          }
        }
      )

    {reply, _state} = decode_reply(tick(state))
    assert reply["alerts"] == 1

    [delivery] = sent(outbox)
    msg = Jason.decode!(delivery.content)
    assert msg["card"]["title"] =~ "slow"
    texts = Enum.map(msg["card"]["blocks"], & &1["text"])
    assert Enum.any?(texts, &(&1 =~ "alive"))
  end

  test "tick gap over the threshold mints ONE observer_gap card on the wake tick" do
    %{state: state, outbox: outbox, clock: clock} = start_scope()

    {_reply, state} = decode_reply(tick(state))
    assert sent(outbox) == []

    # the Mac slept for 7 hours
    advance(clock, 7 * 60 * 60_000)
    {_reply, state} = decode_reply(tick(state))

    gap_cards =
      outbox
      |> sent()
      |> Enum.map(&Jason.decode!(&1.content))
      |> Enum.filter(&(&1["card"]["title"] =~ "blind"))

    assert [card] = gap_cards
    texts = Enum.map(card["card"]["blocks"], & &1["text"])
    assert Enum.any?(texts, &(&1 =~ "7 h"))

    # a normal 5-min cadence never fires it
    advance(clock, 5 * 60_000)
    {_reply, _state} = decode_reply(tick(state))

    gap_cards2 =
      outbox
      |> sent()
      |> Enum.map(&Jason.decode!(&1.content))
      |> Enum.filter(&(&1["card"]["title"] =~ "blind"))

    assert length(gap_cards2) == 1
  end

  test "http_timeout_ms config lands in client_opts; detector_timeout_ms is normalized" do
    %{state: state} = start_scope(config: %{http_timeout_ms: 9_000, detector_timeout_ms: 4_000})
    assert Keyword.get(state.client_opts, :timeout_ms) == 9_000
    assert state.detector_timeout_ms == 4_000

    %{state: default_state} = start_scope()
    assert default_state.detector_timeout_ms == 2_000
    assert Keyword.get(default_state.client_opts, :timeout_ms) == nil
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
    assert Jason.decode!(delivery.content)["card"]["title"] =~ "pool saturated"
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
    assert Jason.decode!(delivery.content)["card"]["title"] =~ "unreachable"
  end

  # ── events feed (F1): cursor fetch + UX-detector wiring ──────────────────

  # Wingston-shaped feed event as the observer client sees it after
  # Jason.decode — objects/event_feed.ex:164-177 stores
  # `meta |> json_safe_map() |> Map.put(:seq, seq) |> Map.put(:ts, ms/1000)`,
  # string keys on the wire, "ts" in float unix SECONDS.
  defp feed_event(kind, cid, ms, extra \\ %{}) do
    Map.merge(%{"kind" => kind, "cid" => cid, "seq" => 1, "ts" => ms / 1000}, extra)
  end

  test "feed events drive the Unanswered detector through a real tick" do
    open = feed_event("request_open", "tg:9:0", @t0 - 20 * 60_000)

    %{state: state, outbox: outbox} =
      start_scope(
        fixture: %{
          "wingston" =>
            Map.put(healthy_fixture(), :events_feed, {:ok, %{events: [open], seq: 1}})
        }
      )

    {reply, _state} = decode_reply(tick(state))
    assert reply["alerts"] == 1

    [delivery] = sent(outbox)
    assert Jason.decode!(delivery.content)["card"]["title"] =~ "waiting"
  end

  test "tick fetches the feed with the session cursor, seeded 0, advancing to the returned seq" do
    %{state: state, fake: fake, clock: clock} =
      start_scope(
        fixture: %{
          "wingston" =>
            Map.put(healthy_fixture(), :events_feed, fn since ->
              {:ok, %{events: [], seq: since + 7}}
            end)
        }
      )

    {_, state} = decode_reply(tick(state))
    advance(clock, 60_000)
    {_, state} = decode_reply(tick(state))

    assert state.feed_cursors == %{"wingston" => 14}

    sinces = for %{kind: :events_feed, since: s} <- Client.Fake.calls(fake), do: s
    assert sinces == [0, 7]
  end

  test "the cursor survives feed :unavailable and {:error, _} answers unchanged" do
    %{state: state, fake: fake, clock: clock} =
      start_scope(
        fixture: %{
          "wingston" => Map.put(healthy_fixture(), :events_feed, {:ok, %{events: [], seq: 5}})
        }
      )

    {_, state} = decode_reply(tick(state))
    assert state.feed_cursors == %{"wingston" => 5}

    Client.Fake.put(fake, "wingston", Map.put(healthy_fixture(), :events_feed, {:error, :boom}))
    advance(clock, 60_000)
    {reply, state} = decode_reply(tick(state))
    assert reply["ok"] == true
    assert state.feed_cursors == %{"wingston" => 5}

    Client.Fake.put(fake, "wingston", healthy_fixture())
    advance(clock, 60_000)
    {_, state} = decode_reply(tick(state))
    assert state.feed_cursors == %{"wingston" => 5}

    sinces = for %{kind: :events_feed, since: s} <- Client.Fake.calls(fake), do: s
    assert sinces == [0, 5, 5]
  end

  test "a malformed feed seq from a buggy client never poisons the cursor" do
    %{state: state} =
      start_scope(
        fixture: %{
          "wingston" =>
            Map.put(healthy_fixture(), :events_feed, {:ok, %{events: [], seq: "nine"}})
        }
      )

    {reply, state} = decode_reply(tick(state))
    assert reply["ok"] == true
    assert state.feed_cursors == %{}
  end

  # ── first-read drain (restart batch-boundary) ─────────────────────────────

  test "first read drains the ring to head — an answered pair split across page boundaries never false-alerts" do
    # Observer restart: the cursor reseeds 0 while the host ring holds an OLD
    # answered pair. The server pages the replay; the open lands in page 1
    # and its ok reply in page 2. A single-page first read would track the
    # open on tick 1 and false-alert (> 15 min old) before tick 2 ever saw
    # the reply — the drain unions the pages so the pair cancels first.
    open = feed_event("request_open", "tg:7:0", @t0 - 30 * 60_000)
    reply = feed_event("reply_sent", "tg:7:0", @t0 - 29 * 60_000, %{"ok" => true})

    %{state: state, fake: fake} =
      start_scope(
        fixture: %{
          "wingston" =>
            Map.put(healthy_fixture(), :events_feed, fn
              0 -> {:ok, %{events: [open], seq: 1}}
              1 -> {:ok, %{events: [reply], seq: 2}}
              2 -> {:ok, %{events: [], seq: 2}}
            end)
        }
      )

    {reply_json, state} = decode_reply(tick(state))
    assert reply_json["alerts"] == 0
    assert state.feed_cursors == %{"wingston" => 2}

    sinces = for %{kind: :events_feed, since: s} <- Client.Fake.calls(fake), do: s
    assert sinces == [0, 1, 2]
  end

  test "a genuinely-unanswered old request still alerts through the drained union" do
    # Draining must FEED the backlog to the detectors, not skip to head — an
    # old open with no reply anywhere in the ring is a real alert.
    open = feed_event("request_open", "tg:8:0", @t0 - 30 * 60_000)

    %{state: state, outbox: outbox} =
      start_scope(
        fixture: %{
          "wingston" =>
            Map.put(healthy_fixture(), :events_feed, fn
              0 -> {:ok, %{events: [open], seq: 1}}
              _ -> {:ok, %{events: [], seq: 1}}
            end)
        }
      )

    {reply_json, _state} = decode_reply(tick(state))
    assert reply_json["alerts"] == 1
    [delivery] = sent(outbox)
    assert Jason.decode!(delivery.content)["card"]["title"] =~ "waiting"
  end

  test "the first-read drain is bounded — a pathological always-growing feed cannot loop a tick forever" do
    %{state: state, fake: fake} =
      start_scope(
        fixture: %{
          "wingston" =>
            Map.put(healthy_fixture(), :events_feed, fn since ->
              {:ok, %{events: [feed_event("chatter", "x", @t0)], seq: since + 1}}
            end)
        }
      )

    {reply_json, state} = decode_reply(tick(state))
    assert reply_json["ok"] == true

    feed_calls = for %{kind: :events_feed} <- Client.Fake.calls(fake), do: :call
    assert length(feed_calls) == 10
    assert state.feed_cursors == %{"wingston" => 10}
  end

  test "a mid-drain error discards the partial batch and leaves the cursor unset for a clean retry" do
    # Feeding a partial drain to the detectors would recreate the exact
    # batch-boundary false-alert the drain exists to prevent — on any
    # mid-drain failure the whole read reports as an error and the cursor
    # stays unset, so the next tick re-drains from 0 (nothing is lost: the
    # cursor never advanced).
    open = feed_event("request_open", "tg:7:0", @t0 - 30 * 60_000)
    reply_ev = feed_event("reply_sent", "tg:7:0", @t0 - 29 * 60_000, %{"ok" => true})

    %{state: state, fake: fake, clock: clock} =
      start_scope(
        fixture: %{
          "wingston" =>
            Map.put(healthy_fixture(), :events_feed, fn
              0 -> {:ok, %{events: [open], seq: 1}}
              _ -> {:error, :boom}
            end)
        }
      )

    {reply_json, state} = decode_reply(tick(state))
    assert reply_json["alerts"] == 0
    assert state.feed_cursors == %{}

    health = status_health(state)["wingston"]
    assert health["fetch"]["last_error"] =~ "boom"

    # feed recovers: the next tick re-drains from 0 and sees the whole pair
    Client.Fake.put(
      fake,
      "wingston",
      Map.put(healthy_fixture(), :events_feed, fn
        0 -> {:ok, %{events: [open, reply_ev], seq: 2}}
        _ -> {:ok, %{events: [], seq: 2}}
      end)
    )

    advance(clock, 60_000)
    {reply_json2, state} = decode_reply(tick(state))
    assert reply_json2["alerts"] == 0
    assert state.feed_cursors == %{"wingston" => 2}
  end

  test "a non-advancing nonempty page (echo/mid-drain feed restart) stops the drain without duplicating events" do
    # A static page that keeps answering the same events with the same seq
    # must not be appended twice into the union.
    open = feed_event("request_open", "tg:9:0", @t0 - 20 * 60_000)

    %{state: state, outbox: outbox} =
      start_scope(
        fixture: %{
          "wingston" =>
            Map.put(healthy_fixture(), :events_feed, {:ok, %{events: [open], seq: 1}})
        }
      )

    {reply_json, state} = decode_reply(tick(state))
    assert reply_json["alerts"] == 1
    assert state.feed_cursors == %{"wingston" => 1}
    assert length(sent(outbox)) == 1
  end

  test "feed :unavailable keeps the fetch stage healthy (a host without an EventsSource is not an error)" do
    # healthy_fixture/0 has no :events_feed — the Fake answers :unavailable,
    # exactly like a dashboard whose host never wired an EventsSource.
    %{state: state} = start_scope()

    {reply, state} = decode_reply(tick(state))
    assert reply["ok"] == true

    health = status_health(state)["wingston"]
    assert health["fetch"]["last_error"] == nil
    assert health["fetch"]["last_success_ms"] == @t0
  end

  test "feed {:error, _} marks the fetch stage errored but the tick completes" do
    %{state: state} =
      start_scope(
        fixture: %{
          "wingston" => Map.put(healthy_fixture(), :events_feed, {:error, :feed_exploded})
        }
      )

    {reply, state} = decode_reply(tick(state))
    assert reply["ok"] == true

    health = status_health(state)["wingston"]
    assert health["fetch"]["last_error"] =~ "feed"
    assert health["fetch"]["last_error"] =~ "feed_exploded"
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
    evidence_text = Enum.find(coalesced["card"]["blocks"], &(&1["text"] =~ "synthetic_"))["text"]
    assert evidence_text =~ "synthetic_7"
    assert evidence_text =~ "synthetic_8"
  end

  @tag regression: "F10"
  test "detector alerts survive the budget ahead of noisy signal-rule alerts" do
    defmodule CoreDetector do
      @behaviour Genswarms.Observer.Detector

      def detect(_fetched, ctx) do
        alert = %{
          type: :unanswered,
          key: {ctx.swarm, :unanswered, "cid-core"},
          swarm: ctx.swarm,
          at_ms: ctx.now_ms,
          summary: "core detector alert",
          evidence: %{},
          cids: ["cid-core"]
        }

        {[alert], ctx.state}
      end
    end

    dashboard =
      healthy_dashboard()
      |> Map.put("extensions", %{
        "rules" => %{
          "items" => for(i <- 1..10, do: %{"name" => "item#{i}"}),
          "health_rules" => [
            %{
              "id" => "noisy",
              "card" => "noisy {name}",
              "each" => "items",
              "when" => %{"op" => "eq", "lhs" => 1, "rhs" => 1}
            }
          ]
        }
      })

    %{state: state, outbox: outbox} =
      start_scope(fixture: %{"wingston" => %{healthy_fixture() | dashboard: {:ok, dashboard}}})

    state = %{state | detectors: [CoreDetector]}
    {reply, _state} = decode_reply(tick(state))

    assert reply["alerts"] == 7

    titles =
      outbox
      |> sent()
      |> Enum.map(&Jason.decode!(&1.content)["card"]["title"])

    assert Enum.any?(titles, &(&1 =~ "waiting"))
  end

  test "sustained overflow: the coalesced summary respects its own cooldown across ticks" do
    defmodule FloodDetector do
      @behaviour Genswarms.Observer.Detector

      # 8 fresh-keyed alerts EVERY tick (keys carry a tick counter, so
      # cooldown never filters the individual alerts) — sustained overflow.
      def detect(_fetched, ctx) do
        tick_n = ctx.state || 0

        alerts =
          for i <- 1..8 do
            %{
              type: :flood,
              key: {ctx.swarm, :flood, tick_n, i},
              swarm: ctx.swarm,
              at_ms: ctx.now_ms,
              summary: "flood #{tick_n}/#{i}",
              evidence: %{}
            }
          end

        {alerts, tick_n + 1}
      end
    end

    %{state: state, outbox: outbox, clock: clock} = start_scope()
    state = %{state | detectors: [FloodDetector]}

    {reply1, state} = decode_reply(tick(state))
    # 6 kept + the coalesced summary
    assert reply1["alerts"] == 7

    advance(clock, 60_000)
    {reply2, _state} = decode_reply(tick(state))
    # 6 fresh kept alerts, but the summary is inside its cooldown window:
    # suppressed like any other same-key alert, not re-emitted every tick.
    # Lifecycle: suppressed now counts every dropped alert — the 2 alerts
    # dropped by the budget PLUS the coalesced summary itself (3), not the
    # old 0/1-for-the-whole-overflow count.
    assert reply2["alerts"] == 6
    assert reply2["suppressed"] == 3

    cards = sent(outbox) |> Enum.map(&Jason.decode!(&1.content))
    assert Enum.count(cards, &(&1["card"]["title"] =~ "alerts_coalesced")) == 1
  end

  test "two same-key alerts in one tick collapse into a single card" do
    defmodule DupKeyDetector do
      @behaviour Genswarms.Observer.Detector

      def detect(_fetched, ctx) do
        alert = %{
          type: :dup_thing,
          swarm: ctx.swarm,
          at_ms: ctx.now_ms,
          summary: "same key twice in one detect",
          evidence: %{}
        }

        # identical key (default {swarm, type}) — cooldown can't catch this
        # within a tick because last_alert only updates on emit.
        {[alert, alert], ctx.state}
      end
    end

    %{state: state, outbox: outbox} = start_scope()
    state = %{state | detectors: [DupKeyDetector]}

    {reply, _state} = decode_reply(tick(state))
    assert reply["alerts"] == 1

    assert [delivery] = sent(outbox)
    assert Jason.decode!(delivery.content)["card"]["title"] =~ "dup_thing"
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

  @tag regression: "F11"
  test "escalation is dampened per swarm and type while cards keep per-key cooldown" do
    defmodule FreshCidBurstDetector do
      @behaviour Genswarms.Observer.Detector

      def detect(_fetched, ctx) do
        tick_n = ctx.state || 0

        unanswered =
          for i <- 1..3 do
            %{
              type: :unanswered,
              key: {ctx.swarm, :unanswered, tick_n, i},
              swarm: ctx.swarm,
              at_ms: ctx.now_ms,
              summary: "unanswered #{tick_n}/#{i}",
              evidence: %{},
              cids: ["cid-#{tick_n}-#{i}"]
            }
          end

        other = %{
          type: :error_burst,
          key: {ctx.swarm, :error_burst, tick_n},
          swarm: ctx.swarm,
          at_ms: ctx.now_ms,
          summary: "different type #{tick_n}",
          evidence: %{},
          cids: []
        }

        {unanswered ++ [other], tick_n + 1}
      end
    end

    %{state: state, outbox: outbox, clock: clock} =
      start_scope(config: %{escalate_to: :diagnostico})

    state = %{state | detectors: [FreshCidBurstDetector]}

    {_, state} = decode_reply(tick(state))
    advance(clock, 5 * 60_000)
    {_, state} = decode_reply(tick(state))

    deliveries = sent(outbox)
    cards = Enum.count(deliveries, &(&1.target == :sender))
    escalations = Enum.filter(deliveries, &(&1.target == :diagnostico))

    assert cards == 8
    assert length(escalations) == 2
    assert Enum.count(escalations, &(&1.content =~ "type: unanswered")) == 1
    assert Enum.count(escalations, &(&1.content =~ "type: error_burst")) == 1

    advance(clock, 31 * 60_000)
    {_, _state} = decode_reply(tick(state))

    escalations = sent(outbox) |> Enum.filter(&(&1.target == :diagnostico))
    assert Enum.count(escalations, &(&1.content =~ "type: unanswered")) == 2
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

  test "get_session_history with a non-binary cid is dropped, never crashes" do
    %{state: state, fake: fake} = start_scope()

    for bad_cid <- [%{}, [1, 2], 7, nil, true] do
      msg = Jason.encode!(%{action: "get_session_history", swarm: "wingston", cid: bad_cid})
      assert {:noreply, _} = Scope.handle_message(:diagnostico, msg, state)
    end

    assert Client.Fake.calls(fake) == []
  end

  test "get_session_history with a non-binary swarm is dropped, never crashes" do
    %{state: state, fake: fake} = start_scope()

    # the diagnosis agent is an LLM assembling JSON — a map/list/number
    # where the swarm string belongs must fall to the catch-all, not
    # Protocol.UndefinedError inside to_string/1
    for bad_swarm <- [%{}, [1, 2], 7, nil, true] do
      msg = Jason.encode!(%{action: "get_session_history", swarm: bad_swarm, cid: "tg:1:0"})
      assert {:noreply, _} = Scope.handle_message(:diagnostico, msg, state)
    end

    assert Client.Fake.calls(fake) == []
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

  # ── F2: detector quarantine ───────────────────────────────────────────────

  defmodule AlwaysCrashDetector do
    @behaviour Genswarms.Observer.Detector
    def detect(_fetched, _ctx), do: raise("boom")
  end

  describe "F2: detector quarantine" do
    @tag regression: "F2"
    test "3 consecutive crashes quarantine the module, clear its state, and alert once" do
      %{state: state, clock: clock, outbox: outbox} =
        start_scope(config: %{custom_detectors: [AlwaysCrashDetector], cooldown_minutes: 0})

      # Ticks 1-3: detector_crashed each tick; tick 3 transitions to quarantined.
      state =
        Enum.reduce(1..3, state, fn _, st ->
          advance(clock, 60_000)
          {_, st} = decode_reply(tick(st))
          st
        end)

      assert state.quarantine[{"wingston", AlwaysCrashDetector}] == 3
      refute Map.has_key?(Map.get(state.det, "wingston", %{}), AlwaysCrashDetector)

      quarantine_cards =
        sent(outbox)
        |> Enum.filter(&String.contains?(&1.content, "detector_quarantined"))

      assert length(quarantine_cards) == 1

      # Tick 4: the module no longer runs — no NEW detector_crashed card.
      crashed_before =
        sent(outbox) |> Enum.count(&String.contains?(&1.content, "detector_crashed"))

      advance(clock, 60_000)
      {_, state} = decode_reply(tick(state))

      crashed_after =
        sent(outbox) |> Enum.count(&String.contains?(&1.content, "detector_crashed"))

      assert crashed_after == crashed_before
      assert state.quarantine[{"wingston", AlwaysCrashDetector}] == 3
    end

    @tag regression: "F2"
    test "status carries the quarantine trace after a module is disabled" do
      %{state: state, clock: clock} =
        start_scope(config: %{custom_detectors: [AlwaysCrashDetector], cooldown_minutes: 0})

      state =
        Enum.reduce(1..3, state, fn _, st ->
          advance(clock, 60_000)
          {_, st} = decode_reply(tick(st))
          st
        end)

      {:reply, json, _} = Scope.handle_message(:cron, ~s({"action":"status"}), state)
      status = Jason.decode!(json)

      # health goes quiet on a quarantined module (it's dropped from
      # det_health entirely) — status must carry the trace instead, since
      # a restart is the only reset lever.
      assert status["quarantine"] == %{
               "wingston/Genswarms.Observer.ScopeTest.AlwaysCrashDetector" => 3
             }
    end
  end

  # ── F1: cursor commit is gated on every active detector succeeding ────────

  defmodule CrashOnceDetector do
    @behaviour Genswarms.Observer.Detector
    # Crashes only on its first run per BEAM (an Agent scoped to the test
    # process would not survive the runner's Task); a named Agent keyed by
    # the test pid keeps async tests isolated.
    def start_flag(name), do: Agent.start_link(fn -> :crash end, name: name)

    def detect(_fetched, ctx) do
      flag = ctx.thresholds["crash_once.flag_name"] |> String.to_existing_atom()

      case Agent.get_and_update(flag, fn v -> {v, :ok} end) do
        :crash -> raise "first-run crash"
        :ok -> {[], ctx.state}
      end
    end

    def default_thresholds, do: %{"crash_once.flag_name" => "unset"}
  end

  describe "F1: cursor only commits when every active detector succeeded" do
    @tag regression: "F1"
    test "a tick with a crashed detector re-reads the same feed window next tick" do
      flag = :"crash_once_#{System.unique_integer([:positive])}"
      {:ok, _} = CrashOnceDetector.start_flag(flag)

      # Feed: one request_open, opened long enough ago to be overdue.
      open = feed_event("request_open", "tg:9:0", @t0 - 20 * 60_000)

      %{state: state, fake: _fake, clock: clock, outbox: outbox} =
        start_scope(
          fixture: %{
            "wingston" =>
              Map.put(healthy_fixture(), :events_feed, {:ok, %{events: [open], seq: 1}})
          },
          config: %{
            custom_detectors: [CrashOnceDetector],
            thresholds: %{"crash_once.flag_name" => to_string(flag)}
          }
        )

      # Tick 1: CrashOnceDetector crashes → cursor must NOT commit.
      {_, state} = decode_reply(tick(state))
      assert state.feed_cursors == %{}

      # Tick 2: detector healthy now; the SAME window replays. Tick 1
      # already fired the Unanswered alert — what this tick proves is that
      # the cursor finally commits once the detector runs clean, and the
      # replayed window is processed idempotently (no duplicate alert;
      # count stays 1 below).
      advance(clock, 60_000)
      {_, state} = decode_reply(tick(state))

      assert state.feed_cursors["wingston"] == 1

      unanswered =
        sent(outbox) |> Enum.filter(&String.contains?(&1.content, "unanswered"))

      assert length(unanswered) == 1
    end
  end

  describe "F4: budget-dropped alerts re-emit on later ticks" do
    @tag regression: "F4"
    test "9 overdue conversations all eventually alert individually" do
      open_ts = @t0 / 1000 - 20 * 60

      feed_events =
        for i <- 1..9 do
          %{"kind" => "request_open", "cid" => "tg:#{i}:0", "seq" => i, "ts" => open_ts + i / 10}
        end

      # `events_feed` as a 1-arity fun of `since` (real Fake vocabulary —
      # there is no `Client.Fake.set_feed`): since == 0 drains the full
      # backlog in tick 1 (Ingest pages to head), and since >= 9 (the
      # committed cursor after tick 1) answers with an empty page at the
      # same seq — exactly tick-1-full / tick-2-empty.
      feed_fun = fn
        0 -> {:ok, %{events: feed_events, seq: 9}}
        _ -> {:ok, %{events: [], seq: 9}}
      end

      %{state: state, clock: clock, outbox: outbox} =
        start_scope(
          fixture: %{"wingston" => Map.put(healthy_fixture(), :events_feed, feed_fun)}
        )

      # Tick 1: 6 individual cards + 1 coalesced summary.
      {_, state} = decode_reply(tick(state))

      # Tick 2 (feed empty now — the opens were consumed): the 3 dropped
      # cids are still unmarked and re-fire within the budget.
      advance(clock, 60_000)
      {_, _state} = decode_reply(tick(state))

      # Anchored on the alert's own "request <cid> unanswered for" summary
      # phrase, not a bare `tg:\d+:0` scan over the whole card JSON — the
      # latter also matches the fixture's routing `alert_conversation_id`
      # ("tg:42:0", embedded in every card's `conversation_id` field),
      # which would inflate the unique count by one phantom "cid".
      unanswered_cids =
        sent(outbox)
        |> Enum.filter(&String.contains?(&1.content, "unanswered"))
        |> Enum.flat_map(fn %{content: c} ->
          # anchored on the card's machine tail ("· <cid> — paste to Claude"),
          # which names the ALERT's cid — never the routing conversation_id.
          Regex.scan(~r/· (tg:\d+:0) — paste to Claude/, c, capture: :all_but_first)
        end)
        |> List.flatten()
        |> Enum.uniq()

      assert length(unanswered_cids) == 9
    end
  end

  # ── review fix: synthetic alerts never reach a detector's on_emitted/2 ────

  defmodule SucceedsThenCrashesDetector do
    @behaviour Genswarms.Observer.Detector
    # Same named-Agent-flag pattern as CrashOnceDetector above, inverted:
    # succeeds (and commits state) on tick 1, crashes on every tick after.
    def start_flag(name), do: Agent.start_link(fn -> :first end, name: name)

    def detect(_fetched, ctx) do
      flag = ctx.thresholds["succeeds_then_crashes.flag_name"] |> String.to_existing_atom()

      case Agent.get_and_update(flag, fn v -> {v, :done} end) do
        :first -> {[], %{seen: true}}
        :done -> raise "boom on second tick"
      end
    end

    def default_thresholds, do: %{"succeeds_then_crashes.flag_name" => "unset"}

    # If apply_on_emitted/3 ever calls this for the module's OWN synthetic
    # :detector_crashed alert (source is runner-stamped provenance, not an
    # alert this module produced), the process dictionary flag flips.
    def on_emitted(state, _alert) do
      Process.put(:succeeds_then_crashes_on_emitted_called, true)
      state
    end
  end

  describe "review fix: synthetic detector_crashed/detector_invalid alerts skip on_emitted/2" do
    test "a detector's own on_emitted/2 is never invoked for its synthetic crash alert" do
      flag = :"succeeds_then_crashes_#{System.unique_integer([:positive])}"
      {:ok, _} = SucceedsThenCrashesDetector.start_flag(flag)
      Process.delete(:succeeds_then_crashes_on_emitted_called)

      %{state: state, clock: clock} =
        start_scope(
          config: %{
            custom_detectors: [SucceedsThenCrashesDetector],
            cooldown_minutes: 0,
            thresholds: %{"succeeds_then_crashes.flag_name" => to_string(flag)}
          }
        )

      # Tick 1: detect/2 succeeds — state commits under the module's key.
      {_, state} = decode_reply(tick(state))

      # Tick 2: detect/2 crashes. The runner's synthetic :detector_crashed
      # alert is source-stamped with this module, which DOES export
      # on_emitted/2 and DOES already have committed det state from tick 1
      # — exactly the shape that would slip past a naive source-only check.
      advance(clock, 60_000)
      {_, _state} = decode_reply(tick(state))

      refute Process.get(:succeeds_then_crashes_on_emitted_called)
    end
  end

  # ── Task 6: Signals stage ────────────────────────────────────────────────
  #
  # Fixtures copied verbatim from the plan / signals_test.exs (provenance:
  # docs/superpowers/plans/2026-07-07-observability-stages-2-4.md, Task 1 +
  # Task 3) so the wiring is proven against the actual shipped shapes.
  describe "Task 6: Signals stage" do
    @cron_missed_tick_rule %{
      "id" => "missed_tick",
      "severity" => "warn",
      "card" => "cron job {name} did not run (overdue past grace)",
      "each" => "jobs",
      "where" => %{"op" => "eq", "lhs" => %{"path" => "state"}, "rhs" => %{"lit" => "active"}},
      "when" => %{
        "op" => "gt",
        "lhs" => "now",
        "rhs" => %{"add" => [%{"path" => "next_run_at_ms"}, 1_800_000]}
      }
    }

    @poll_conflict_rule %{
      "id" => "poll_conflict",
      "severity" => "warn",
      "card" => "getUpdates 409 conflict — two pollers are fighting over this bot token",
      "when" => %{"op" => "gt", "lhs" => %{"delta" => "conflict_count"}, "rhs" => 0}
    }

    defp cron_envelope(jobs) do
      healthy_dashboard()
      |> Map.put("extensions", %{
        "cron" => %{"v" => 1, "jobs" => jobs, "health_rules" => [@cron_missed_tick_rule]}
      })
    end

    defp cron_envelope(jobs, extra_rules) do
      healthy_dashboard()
      |> Map.put("extensions", %{
        "cron" => %{"v" => 1, "jobs" => jobs, "health_rules" => extra_rules}
      })
    end

    defp poller_envelope(conflict_count) do
      healthy_dashboard()
      |> Map.put("extensions", %{
        "telegram_poller" => %{
          "v" => 1,
          "conflict_count" => conflict_count,
          "health_rules" => [@poll_conflict_rule]
        }
      })
    end

    defp fixture(envelope), do: %{"wingston" => %{dashboard: {:ok, envelope}, events: {:ok, []}}}

    test "an overdue active job fires one health_rule alert through the normal pipeline; a paused overdue job doesn't" do
      envelope =
        cron_envelope([
          %{"name" => "sync", "state" => "active", "next_run_at_ms" => 0},
          %{"name" => "paused_job", "state" => "paused", "next_run_at_ms" => 0}
        ])

      %{state: state, outbox: outbox} = start_scope(fixture: fixture(envelope))

      {reply, _state} = decode_reply(tick(state))
      assert reply["alerts"] == 1

      [card] = sent(outbox) |> Enum.map(&Jason.decode!(&1.content))
      assert card["card"]["title"] =~ "health_rule"
      assert Enum.any?(card["card"]["blocks"], &(&1["text"] =~ "sync"))
    end

    test "a paused-only overdue job raises nothing" do
      envelope = cron_envelope([%{"name" => "paused_job", "state" => "paused", "next_run_at_ms" => 0}])
      %{state: state, outbox: outbox} = start_scope(fixture: fixture(envelope))

      {reply, _state} = decode_reply(tick(state))
      assert reply["alerts"] == 0
      assert sent(outbox) == []
    end

    test "delta rule: first tick no alert, second tick (increase) alerts, flat third tick doesn't" do
      %{state: state, fake: fake, clock: clock, outbox: outbox} =
        start_scope(fixture: fixture(poller_envelope(0)))

      {reply1, state} = decode_reply(tick(state))
      assert reply1["alerts"] == 0

      Client.Fake.put(fake, "wingston", %{dashboard: {:ok, poller_envelope(1)}, events: {:ok, []}})
      advance(clock, 1_000)
      {reply2, state} = decode_reply(tick(state))
      assert reply2["alerts"] == 1

      Client.Fake.put(fake, "wingston", %{dashboard: {:ok, poller_envelope(1)}, events: {:ok, []}})
      advance(clock, 1_000)
      {reply3, _state} = decode_reply(tick(state))
      assert reply3["alerts"] == 0

      cards = sent(outbox) |> Enum.map(&Jason.decode!(&1.content))
      assert Enum.count(cards, &(&1["card"]["title"] =~ "health_rule")) == 1
    end

    test "malformed package rules (17 rules) drop that block's alerts, name it in :signals health, other blocks still evaluate" do
      broken_rules = for i <- 1..17, do: %{@cron_missed_tick_rule | "id" => "r#{i}"}

      envelope =
        cron_envelope(
          [%{"name" => "sync", "state" => "active", "next_run_at_ms" => 0}],
          broken_rules
        )
        |> Map.update!("extensions", fn ext ->
          Map.put(ext, "telegram_poller", %{
            "v" => 1,
            "health_rules" => [
              %{"id" => "always_fires", "card" => "c", "when" => %{"op" => "gt", "lhs" => "now", "rhs" => 0}}
            ]
          })
        end)

      %{state: state, outbox: outbox} = start_scope(fixture: fixture(envelope))
      {reply, state} = decode_reply(tick(state))

      # cron's 17-rule block never fires; telegram_poller's always_fires rule does.
      assert reply["alerts"] == 1
      [card] = sent(outbox) |> Enum.map(&Jason.decode!(&1.content))
      assert card["card"]["title"] =~ "health_rule"
      refute card["card"]["title"] =~ "cron"

      assert %{"wingston" => %{signals: %{last_error: err}}} = state.health
      assert err =~ "cron"
      assert err =~ "17"
    end

    test "item_key cap: 15 matching items feed no more than 10 signal alerts into the per-swarm budget" do
      jobs = for i <- 1..15, do: %{"name" => "job#{i}", "state" => "active", "next_run_at_ms" => 0}
      envelope = cron_envelope(jobs)

      %{state: state, outbox: outbox} = start_scope(fixture: fixture(envelope))
      {reply, _state} = decode_reply(tick(state))

      # 6 kept by the per-swarm alert budget + 1 coalesced summary.
      assert reply["alerts"] == 7

      cards = sent(outbox) |> Enum.map(&Jason.decode!(&1.content))
      coalesced = Enum.find(cards, &(&1["card"]["title"] =~ "alerts_coalesced"))
      assert coalesced

      evidence_block = Enum.find(coalesced["card"]["blocks"], &(&1["text"] =~ "health_rule"))
      # 10 (the per-rule cap) - 6 (the budget) = 4 dropped, not 15 - 6 = 9.
      assert evidence_block["text"] =~ "health_rule 4"
    end

    test "operator signal_rules run against a block even without package health_rules" do
      envelope = healthy_dashboard() |> Map.put("extensions", %{"metrics_today" => %{"widgets" => 3}})

      %{state: state, outbox: outbox} =
        start_scope(
          fixture: fixture(envelope),
          config: %{
            signal_rules: [
              %{
                "block" => "metrics_today",
                "id" => "widget_check",
                "card" => "widgets={widgets}",
                "when" => %{"op" => "gt", "lhs" => %{"path" => "widgets"}, "rhs" => 0}
              }
            ]
          }
        )

      {reply, _state} = decode_reply(tick(state))
      assert reply["alerts"] == 1

      [card] = sent(outbox) |> Enum.map(&Jason.decode!(&1.content))
      assert Enum.any?(card["card"]["blocks"], &(&1["text"] =~ "widgets=3"))
    end
  end

  describe "Task 6: sovereign rules_gone (with the 2-tick debounce)" do
    @gone_test_rule %{
      "id" => "poll_conflict",
      "severity" => "warn",
      "card" => "getUpdates 409 conflict",
      "when" => %{"op" => "gt", "lhs" => %{"delta" => "conflict_count"}, "rhs" => 0}
    }

    defp present_poller_envelope do
      healthy_dashboard()
      |> Map.put("extensions", %{
        "telegram_poller" => %{"v" => 1, "conflict_count" => 0, "health_rules" => [@gone_test_rule]}
      })
    end

    defp absent_poller_envelope, do: healthy_dashboard()

    test "1 absent tick doesn't fire; the 2nd consecutive dashboard-ok absent tick does; a fetch-error tick doesn't count" do
      %{state: state, fake: fake, clock: clock, outbox: outbox} =
        start_scope(fixture: %{"wingston" => %{dashboard: {:ok, present_poller_envelope()}, events: {:ok, []}}})

      # tick1: block present — establishes rules_seen.
      {_reply1, state} = decode_reply(tick(state))
      refute Enum.any?(sent(outbox), &(Jason.decode!(&1.content)["card"]["title"] =~ "health_rules_gone"))

      # tick2: block absent (1st miss) — no gone alert yet.
      Client.Fake.put(fake, "wingston", %{dashboard: {:ok, absent_poller_envelope()}, events: {:ok, []}})
      advance(clock, 1_000)
      {_reply2, state} = decode_reply(tick(state))
      refute Enum.any?(sent(outbox), &(Jason.decode!(&1.content)["card"]["title"] =~ "health_rules_gone"))

      # tick3: a fetch error — resets nothing, counts nothing (no-verdict).
      Client.Fake.put(fake, "wingston", %{dashboard: {:error, :econnrefused}, events: {:ok, []}})
      advance(clock, 1_000)
      {_reply3, state} = decode_reply(tick(state))
      refute Enum.any?(sent(outbox), &(Jason.decode!(&1.content)["card"]["title"] =~ "health_rules_gone"))

      # tick4: block absent again with dashboard OK — this is the 2nd
      # CONSECUTIVE dashboard-ok miss (tick3's error didn't count) — fires.
      Client.Fake.put(fake, "wingston", %{dashboard: {:ok, absent_poller_envelope()}, events: {:ok, []}})
      advance(clock, 1_000)
      {_reply4, _state} = decode_reply(tick(state))
      assert Enum.any?(sent(outbox), &(Jason.decode!(&1.content)["card"]["title"] =~ "health_rules_gone"))
    end

    test "a block that never goes away never fires rules_gone" do
      %{state: state, clock: clock, outbox: outbox} =
        start_scope(fixture: %{"wingston" => %{dashboard: {:ok, present_poller_envelope()}, events: {:ok, []}}})

      {_reply1, state} = decode_reply(tick(state))
      advance(clock, 1_000)
      {_reply2, _state} = decode_reply(tick(state))

      refute Enum.any?(sent(outbox), &(Jason.decode!(&1.content)["card"]["title"] =~ "health_rules_gone"))
    end
  end

  # ── per-tick alert log line ────────────────────────────────────────────────

  describe "alert emission log" do
    import ExUnit.CaptureLog

    test "an alerting tick logs one line naming the emitted types" do
      %{state: state} =
        start_scope(fixture: %{"wingston" => %{dashboard: {:error, :econnrefused}, events: {:error, :x}}})

      log = capture_log(fn -> tick(state) end)
      assert log =~ "[observer] alerts swarm=wingston sent=endpoint_down:1 suppressed=0"
    end

    test "a quiet tick logs nothing" do
      %{state: state} = start_scope()

      log = capture_log(fn -> tick(state) end)
      refute log =~ "[observer] alerts swarm="
    end
  end

  # ── daily ops digest wiring ───────────────────────────────────────────────

  # @t0 is 2025-07-05 17:00 UTC — past the configured hour_utc, so the
  # digest is due on the very first tick.
  defp ops_envelope do
    healthy_dashboard()
    |> Map.put("extensions", %{"audience" => %{"blocked" => 3, "reachable_dm" => 42}})
  end

  defp ops_config do
    %{
      ops_digest: %{
        "hour_utc" => 7,
        "sections" => [%{"kind" => "block", "block" => "audience", "title" => "audience now"}]
      }
    }
  end

  defp ops_cards(outbox) do
    outbox
    |> sent()
    |> Enum.map(&Jason.decode!(&1.content)["card"])
    |> Enum.filter(&(&1["title"] =~ "🌅"))
  end

  describe "ops digest" do
    test "sends one card per day with the configured block section" do
      %{state: state, clock: clock, outbox: outbox} =
        start_scope(
          fixture: %{"wingston" => %{dashboard: {:ok, ops_envelope()}, events: {:ok, []}}},
          config: ops_config()
        )

      {_reply, state} = decode_reply(tick(state))

      assert [card] = ops_cards(outbox)
      assert card["title"] =~ "wingston"
      assert card["title"] =~ "2025-07-05"
      assert [%{"text" => text}] = card["blocks"]
      assert text =~ "audience now"
      assert text =~ "blocked 3"
      assert text =~ "reachable_dm 42"

      # same day: no second card
      advance(clock, 60_000)
      {_reply, _state} = decode_reply(tick(state))
      assert length(ops_cards(outbox)) == 1
    end

    test "a failed delivery is not marked and retries; the next day sends again" do
      {:ok, failures} = Agent.start_link(fn -> 1 end)

      %{state: state, clock: clock, outbox: outbox} =
        start_scope(
          fixture: %{"wingston" => %{dashboard: {:ok, ops_envelope()}, events: {:ok, []}}},
          config: ops_config()
        )

      # the FIRST delivery fails, everything after succeeds
      state = %{
        state
        | deliver_fn: fn target, from, content ->
            if Agent.get_and_update(failures, &{&1, &1 - 1}) > 0 do
              {:error, :boom}
            else
              Agent.update(outbox, &[%{target: target, from: from, content: content} | &1])
              :ok
            end
          end
      }

      {_reply, state} = decode_reply(tick(state))
      assert ops_cards(outbox) == []

      # not marked → the next tick retries and succeeds
      advance(clock, 60_000)
      {_reply, state} = decode_reply(tick(state))
      assert length(ops_cards(outbox)) == 1

      # a day later it sends again
      advance(clock, 24 * 60 * 60_000)
      {_reply, _state} = decode_reply(tick(state))
      assert length(ops_cards(outbox)) == 2
    end

    test "malformed ops_digest config raises at boot (fail-closed operator config)" do
      assert_raise ArgumentError, fn ->
        start_scope(config: %{ops_digest: %{"sections" => [%{"kind" => "nope"}]}})
      end
    end

    test "a corrupt FUTURE ops_sent day is dropped, not honored (it would skip a whole day silently)" do
      # a store carrying tomorrow's date for this swarm: if it survived load,
      # tomorrow's `last_sent_day == day` check would suppress that day's digest
      # entirely — no card, no log, no trace.
      tomorrow =
        @t0 |> DateTime.from_unix!(:millisecond) |> DateTime.to_date() |> Date.add(1) |> Date.to_iso8601()

      defmodule FutureStore do
        @behaviour Genswarms.Observer.Store
        def load, do: {:ok, %{ops_sent: %{"wingston" => Process.get(:future_day)}, save_seq: 1}}
        def save(_), do: :ok
      end

      Process.put(:future_day, tomorrow)

      %{state: state, outbox: outbox} =
        start_scope(
          fixture: %{"wingston" => %{dashboard: {:ok, ops_envelope()}, events: {:ok, []}}},
          config: Map.put(ops_config(), :store_mod, FutureStore)
        )

      refute Map.has_key?(state.ops_sent, "wingston"),
             "tomorrow's date survived validation — the digest would be silently skipped"

      {_reply, _state} = decode_reply(tick(state))
      assert length(ops_cards(outbox)) == 1
    end

    test "without ops_digest config nothing changes" do
      %{state: state, outbox: outbox} =
        start_scope(fixture: %{"wingston" => %{dashboard: {:ok, ops_envelope()}, events: {:ok, []}}})

      {_reply, _state} = decode_reply(tick(state))
      assert ops_cards(outbox) == []
    end
  end

  # ── wire-name override end to end ─────────────────────────────────────────

  describe "registry wire name" do
    test "fetches under the entry name; alerts keep the registry-key identity" do
      # fixture answers ONLY to the wire name "wingston"
      {:ok, fake} = Client.Fake.start_link(%{"wingston" => healthy_fixture()})
      {:ok, clock} = Agent.start_link(fn -> @t0 end)
      {:ok, outbox} = Agent.start_link(fn -> [] end)

      config = %{
        swarm_name: "observer",
        registry: %{
          "wingston-prod" => %{
            dashboard_url: "http://elb.example",
            token_env: nil,
            repo: "o/r",
            name: "wingston"
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
      }

      {:ok, state} = Scope.init(config)

      # healthy tick: the wire fetch resolved (no endpoint_down card), and
      # every client call went out under the wire name
      {reply, state} = decode_reply(tick(state))
      assert reply["alerts"] == 0
      assert Enum.all?(Client.Fake.calls(fake), &(&1.swarm == "wingston"))

      # break the endpoint: the alert carries the REGISTRY KEY identity
      Client.Fake.put(fake, "wingston", %{dashboard: {:error, :econnrefused}, events: {:error, :x}})
      advance(clock, 1_000)
      {_reply, _state} = decode_reply(tick(state))

      [card] = sent(outbox) |> Enum.map(&Jason.decode!(&1.content)["card"])
      assert card["title"] =~ "wingston-prod"
    end
  end
end
