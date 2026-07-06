defmodule Genswarms.Observer.DetectorsUxTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Detectors.{DeliveryFailureBurst, TopicsStale, Unanswered}

  # ── REAL feed wire shapes (ground truth for every fixture below) ──────────
  #
  # The UX detectors consume `fetched.feed` — the display-event feed served by
  # `GET /api/swarms/:name/events/feed` (cursor read), NOT the engine LogStore
  # `/events` surface. Two known hosts serve it:
  #
  # WINGSTON — wingstonrallybot/objects/event_feed.ex:164-177 (handle_cast):
  #   event = meta |> json_safe_map() |> Map.put(:seq, seq)
  #                |> Map.put(:ts, System.system_time(:millisecond) / 1000)
  # Per-kind fields from the registry (objects/event_feed.ex:28-52):
  #   request_open: cid · reply_sent: cid, ok, threaded · reply_failed: from
  # The collector stores atom keys / stringified kind values; the dashboard
  # backend Jason-encodes the maps verbatim and the observer client
  # Jason.decodes — so on OUR side of the wire the keys are STRINGS and
  # `"ts"` is a float of unix SECONDS.
  #
  # MICROMARKETS — micromarkets/dashboard/feed/event_feed.ex:317-329 (base/2),
  # already string-keyed at the source:
  #   %{"kind", "ts", "source" => "log_store", "log_id", "event_type",
  #     "category", "message", "metadata"}
  #   + per-kind extras: "cid"/"user" (request_open, :133-148), "ok"/"from"
  #     (reply_sent, :253-276), "seq" stamped by ingest_log (:99-102).
  #   "ts" via unix_ts/1 (:479-480) = DateTime.to_unix(:millisecond) / 1000 —
  #   float unix SECONDS.
  #
  # Both swarms use the SAME key names for everything these detectors read:
  # "kind", "cid", "ok", "ts". There is no per-swarm divergence to bridge —
  # micromarkets just carries extra log-store bookkeeping keys, which the
  # detectors must tolerate (app-opaque events, EventsSource contract).

  # float unix seconds, as both feeds put on the wire
  defp ts(ms), do: ms / 1000

  # Minimal wingston-shaped feed event (provenance above).
  defp ev(kind, cid, ms, extra \\ %{}) do
    # Generate unique seq based on ms so distinct events don't collide on seq.
    # Real wires stamp monotonic per-session seqs; this deterministic mapping
    # ensures test fixtures behave like real ones (distinct events → distinct seqs).
    seq = 1 + abs(Kernel.div(ms, 1000))
    Map.merge(%{"kind" => kind, "cid" => cid, "seq" => seq, "ts" => ts(ms)}, extra)
  end

  # Full micromarkets-shaped feed event (provenance above).
  defp mm_ev(kind, cid, ms, extra) do
    # Generate unique seq based on ms like ev() does
    seq = 1 + abs(Kernel.div(ms, 1000))
    Map.merge(
      %{
        "kind" => kind,
        "cid" => cid,
        "ts" => ts(ms),
        "seq" => seq,
        "source" => "log_store",
        "log_id" => 42,
        "event_type" => "telegram_reply",
        "category" => "communication",
        "message" => "delivered",
        "metadata" => %{"conversation_id" => cid}
      },
      extra
    )
  end

  defp fetched(feed_events, dashboard \\ %{}),
    do: %{dashboard: {:ok, dashboard}, events: {:ok, []}, feed: {:ok, feed_events}}

  # ── Unanswered ─────────────────────────────────────────────────────────────

  describe "Unanswered" do
    test "request_open without ok reply fires after threshold" do
      f = fetched([ev("request_open", "tg:1:0", 0)])

      ctx0 = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: nil,
        now_ms: 16 * 60_000
      }

      {alerts, _state} = Unanswered.detect(f, ctx0)

      assert [%{type: :unanswered, cids: ["tg:1:0"], key: {"w", :unanswered, "tg:1:0"}}] = alerts
    end

    test "request_open still within threshold raises nothing" do
      f = fetched([ev("request_open", "tg:1:0", 0)])

      ctx0 = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: nil,
        now_ms: 10 * 60_000
      }

      assert {[], _state} = Unanswered.detect(f, ctx0)
    end

    test "ok reply_sent before threshold clears the cid, no alert" do
      f =
        fetched([
          ev("request_open", "tg:1:0", 0),
          ev("reply_sent", "tg:1:0", 60_000, %{"ok" => true})
        ])

      ctx0 = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: nil,
        now_ms: 16 * 60_000
      }

      assert {[], state} = Unanswered.detect(f, ctx0)
      assert state == %{}
    end

    test "reply_sent with missing ok key counts as delivered" do
      f =
        fetched([
          ev("request_open", "tg:1:0", 0),
          ev("reply_sent", "tg:1:0", 60_000)
        ])

      ctx0 = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: nil,
        now_ms: 16 * 60_000
      }

      assert {[], %{}} = Unanswered.detect(f, ctx0)
    end

    test "reply_sent with ok:false does not clear the cid" do
      f =
        fetched([
          ev("request_open", "tg:1:0", 0),
          ev("reply_sent", "tg:1:0", 60_000, %{"ok" => false})
        ])

      ctx0 = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: nil,
        now_ms: 16 * 60_000
      }

      assert {[%{type: :unanswered, cids: ["tg:1:0"]}], _state} = Unanswered.detect(f, ctx0)
    end

    test "overlapping event windows dedupe: same open event across two ticks alerts once" do
      f = fetched([ev("request_open", "tg:1:0", 0)])
      thresholds = %{"unanswered.minutes" => 15}

      ctx0 = %{swarm: "w", thresholds: thresholds, state: nil, now_ms: 16 * 60_000}
      {alerts1, state1} = Unanswered.detect(f, ctx0)
      assert [%{type: :unanswered, cids: ["tg:1:0"]}] = alerts1
      assert state1["tg:1:0"] == %{opened_ms: 0, alerted: true}

      # Next tick's fetch still returns the same request_open (feed window
      # overlap) and the cid is still open — must NOT re-alert.
      ctx1 = %{swarm: "w", thresholds: thresholds, state: state1, now_ms: 17 * 60_000}
      {alerts2, state2} = Unanswered.detect(f, ctx1)
      assert alerts2 == []
      assert state2 == state1
    end

    test "cid cleared by a reply reopens cleanly on a later request_open" do
      f1 =
        fetched([
          ev("request_open", "tg:1:0", 0),
          ev("reply_sent", "tg:1:0", 60_000, %{"ok" => true})
        ])

      ctx0 = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: nil,
        now_ms: 2 * 60_000
      }

      {[], state0} = Unanswered.detect(f1, ctx0)
      assert state0 == %{}

      f2 = fetched([ev("request_open", "tg:1:0", 20 * 60_000)])

      ctx1 = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: state0,
        now_ms: 40 * 60_000
      }

      {alerts, _state1} = Unanswered.detect(f2, ctx1)
      assert [%{type: :unanswered, cids: ["tg:1:0"]}] = alerts
    end

    test "default_thresholds exposes only its namespaced key" do
      assert Unanswered.default_thresholds() == %{"unanswered.minutes" => 15}
    end

    test "pruning evicts an alerted cid whose opened_ms is more than 24h stale" do
      now_ms = 100_000_000
      opened_ms = now_ms - 25 * 60 * 60 * 1000
      state = %{"tg:1:0" => %{opened_ms: opened_ms, alerted: true}}

      ctx = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: state,
        now_ms: now_ms
      }

      {alerts, new_state} = Unanswered.detect(fetched([]), ctx)

      assert alerts == []
      assert new_state == %{}
    end

    test "an alerted cid only 1h stale is kept" do
      now_ms = 100_000_000
      opened_ms = now_ms - 60 * 60 * 1000
      state = %{"tg:1:0" => %{opened_ms: opened_ms, alerted: true}}

      ctx = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: state,
        now_ms: now_ms
      }

      {alerts, new_state} = Unanswered.detect(fetched([]), ctx)

      assert alerts == []
      assert new_state == state
    end

    test "an unalerted old cid is NOT evicted (it may still legitimately alert)" do
      now_ms = 100_000_000
      opened_ms = now_ms - 25 * 60 * 60 * 1000
      state = %{"tg:1:0" => %{opened_ms: opened_ms, alerted: false}}

      # Large threshold so this tick doesn't itself fire an alert — isolating
      # the prune behavior from the alert-firing behavior.
      ctx = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 100_000},
        state: state,
        now_ms: now_ms
      }

      {alerts, new_state} = Unanswered.detect(fetched([]), ctx)

      assert alerts == []
      assert new_state == state
    end

    test "malformed ts on request_open is skipped; a later valid open for the same cid tracks normally" do
      f =
        fetched([
          %{"kind" => "request_open", "cid" => "tg:1:0", "seq" => 1, "ts" => "not-a-number"},
          ev("request_open", "tg:1:0", 0)
        ])

      ctx0 = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: nil,
        now_ms: 16 * 60_000
      }

      {alerts, state} = Unanswered.detect(f, ctx0)

      assert [%{type: :unanswered, cids: ["tg:1:0"]}] = alerts
      assert state["tg:1:0"].opened_ms == 0
    end

    test "malformed-ts events are fully inert: dropped before the sort, never coerced to ts 0" do
      # The old `|| 0` coercion sorted a malformed-ts event BEFORE every
      # valid one, defeating the defensive order the sort insures. Untimed
      # events cannot participate in time-based tracking at all — including
      # an ok reply_sent with a junk ts, which previously still cleared its
      # cid whenever the stable sort happened to keep it after the open.
      # Only well-formed events mutate tracking (both real wires always
      # stamp a numeric "ts"; only a malformed host ever hits this).
      f =
        fetched([
          ev("request_open", "tg:1:0", 0),
          %{"kind" => "reply_sent", "cid" => "tg:1:0", "ok" => true, "seq" => 2}
        ])

      ctx0 = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: nil,
        now_ms: 16 * 60_000
      }

      {alerts, _state} = Unanswered.detect(f, ctx0)

      assert [%{type: :unanswered, cids: ["tg:1:0"]}] = alerts
    end

    test "a junk-ts event among a shuffled valid pair does not defeat the defensive order" do
      # reply listed before its open (non-compliant host order) plus a
      # malformed-ts stray: the sort must still fold open-then-reply, and
      # the stray must not displace either.
      f =
        fetched([
          ev("reply_sent", "tg:1:0", 60_000, %{"ok" => true}),
          %{"kind" => "reply_sent", "cid" => "tg:9:9", "ok" => true, "seq" => 3, "ts" => "junk"},
          ev("request_open", "tg:1:0", 0)
        ])

      ctx0 = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: nil,
        now_ms: 16 * 60_000
      }

      assert {[], %{}} = Unanswered.detect(f, ctx0)
    end

    test "micromarkets-shaped events (full log-store keys) track and clear identically" do
      # Same behavioral case as "ok reply clears the cid", on the FULL
      # micromarkets base/2 projection shape — the extra log-store keys
      # ("source"/"log_id"/"event_type"/"category"/"message"/"metadata")
      # must be tolerated, per the app-opaque EventsSource contract.
      f =
        fetched([
          mm_ev("request_open", "tg:1:0", 0, %{"event_type" => "message_received", "user" => "al"}),
          mm_ev("reply_sent", "tg:1:0", 60_000, %{"ok" => true, "from" => "agent_1"})
        ])

      ctx0 = %{
        swarm: "mm",
        thresholds: %{"unanswered.minutes" => 15},
        state: nil,
        now_ms: 16 * 60_000
      }

      assert {[], %{}} = Unanswered.detect(f, ctx0)
    end

    # F1: the feed can legitimately be absent — the host has no EventsSource
    # (`source: "unavailable"`) or the read failed. Without the window we
    # cannot know whether replies happened, so the only safe answer is a
    # NO-OP with prior state: no scan, no alert, no state mutation.
    test "feed :unavailable is a no-op with prior state (no false alert on a tracked cid)" do
      state = %{"tg:1:0" => %{opened_ms: 0, alerted: false}}
      f = %{dashboard: {:ok, %{}}, events: {:ok, []}, feed: :unavailable}

      ctx = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: state,
        now_ms: 16 * 60_000
      }

      assert {[], ^state} = Unanswered.detect(f, ctx)
    end

    test "feed {:error, _} is a no-op with prior state" do
      state = %{"tg:1:0" => %{opened_ms: 0, alerted: false}}
      f = %{dashboard: {:ok, %{}}, events: {:ok, []}, feed: {:error, :econnrefused}}

      ctx = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: state,
        now_ms: 16 * 60_000
      }

      assert {[], ^state} = Unanswered.detect(f, ctx)
    end

    test "a fetched map without a :feed key at all is a no-op with prior state" do
      state = %{"tg:1:0" => %{opened_ms: 0, alerted: false}}
      f = %{dashboard: {:ok, %{}}, events: {:ok, []}}

      ctx = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: state,
        now_ms: 16 * 60_000
      }

      assert {[], ^state} = Unanswered.detect(f, ctx)
    end

    # F2: the EventsSource contract guarantees oldest-first ascending
    # (wingston vendor/genswarms-dashboard/backend README §EventsSource:
    # "Events with seq > since, oldest first"), so fold order is correct by
    # contract — but the detector sorts by "ts" as cheap insurance against a
    # non-compliant host. A shuffled window (reply delivered BEFORE its open
    # in list order) must not false-alert an answered pair.
    test "a shuffled window does not false-alert an answered pair (defensive ts sort)" do
      f =
        fetched([
          ev("reply_sent", "tg:1:0", 60_000, %{"ok" => true, "seq" => 2}),
          ev("request_open", "tg:1:0", 0, %{"seq" => 1})
        ])

      ctx0 = %{
        swarm: "w",
        thresholds: %{"unanswered.minutes" => 15},
        state: nil,
        now_ms: 16 * 60_000
      }

      assert {[], %{}} = Unanswered.detect(f, ctx0)
    end
  end

  describe "Unanswered — F2 guard: poisoned persisted state restarts clean" do
    @tag regression: "F2"
    test "a non-map ctx.state does not crash detect/2" do
      events = [%{"kind" => "request_open", "cid" => "tg:1:0", "seq" => 1, "ts" => 1.0}]

      ctx = %{
        swarm: "wingston",
        thresholds: %{"unanswered.minutes" => 15},
        state: :poisoned_garbage,
        now_ms: 120_000
      }

      # 2 minutes elapsed: no alert yet, but the open must be tracked in a MAP.
      assert {[], state} =
               Genswarms.Observer.Detectors.Unanswered.detect(%{feed: {:ok, events}}, ctx)

      assert is_map(state)
      assert Map.has_key?(state, "tg:1:0")
    end
  end

  # ── DeliveryFailureBurst ─────────────────────────────────────────────────────

  describe "DeliveryFailureBurst" do
    @thresholds %{"delivery_failure.count" => 3, "delivery_failure.window_s" => 600}

    test "3x reply_sent ok:false same cid within window fires one alert with cids [cid]" do
      events =
        for ms <- [0, 100_000, 200_000] do
          ev("reply_sent", "tg:2:0", ms, %{"ok" => false})
        end

      f = fetched(events)
      ctx = %{swarm: "w", thresholds: @thresholds, state: nil, now_ms: 300_000}

      {alerts, _state} = DeliveryFailureBurst.detect(f, ctx)

      assert [
               %{
                 type: :delivery_failure_burst,
                 cids: ["tg:2:0"],
                 key: {"w", :delivery_failure_burst, "tg:2:0"}
               }
             ] = alerts
    end

    test "reply_sent ok:true does not count towards the burst" do
      events =
        for ms <- [0, 100_000, 200_000] do
          ev("reply_sent", "tg:2:0", ms, %{"ok" => true})
        end

      f = fetched(events)
      ctx = %{swarm: "w", thresholds: @thresholds, state: nil, now_ms: 300_000}

      assert {[], _state} = DeliveryFailureBurst.detect(f, ctx)
    end

    test "reply_failed events (no cid) feed :reply_failed_burst only, never delivery_failure_burst" do
      # wingston reply_failed carries `from`, never a cid — the target could
      # not be resolved (objects/event_feed.ex:39).
      events =
        for {ms, seq} <- [{0, 1}, {100_000, 101}, {200_000, 201}] do
          %{"kind" => "reply_failed", "from" => "agent_3", "seq" => seq, "ts" => ts(ms)}
        end

      f = fetched(events)
      ctx = %{swarm: "w", thresholds: @thresholds, state: nil, now_ms: 300_000}

      {alerts, _state} = DeliveryFailureBurst.detect(f, ctx)

      assert [%{type: :reply_failed_burst}] = alerts
      # cids/key are the runner's normalization job, not the module's — it
      # simply doesn't set them for a swarm-level alert.
      refute Map.has_key?(hd(alerts), :cids)
    end

    test "below-threshold counts raise nothing" do
      events = [ev("reply_sent", "tg:2:0", 0, %{"ok" => false})]
      f = fetched(events)
      ctx = %{swarm: "w", thresholds: @thresholds, state: nil, now_ms: 10_000}

      # below threshold raises nothing, but the failure is now REMEMBERED
      # (accumulate-and-prune state) for the cross-tick window
      assert {[], %{fail_ts: %{"tg:2:0" => [{{:seq, 1}, 0}]}}} = DeliveryFailureBurst.detect(f, ctx)
    end

    test "events outside the window are excluded from the count" do
      events =
        for ms <- [0, 100_000] do
          ev("reply_sent", "tg:2:0", ms, %{"ok" => false})
        end ++ [ev("reply_sent", "tg:2:0", -1_000_000, %{"ok" => false})]

      f = fetched(events)
      ctx = %{swarm: "w", thresholds: @thresholds, state: nil, now_ms: 300_000}

      # the out-of-window failure is pruned at ingest, not merely not-counted
      assert {[], %{fail_ts: %{"tg:2:0" => kept}}} = DeliveryFailureBurst.detect(f, ctx)
      assert length(kept) == 2
    end

    test "default_thresholds exposes only its namespaced keys" do
      assert DeliveryFailureBurst.default_thresholds() == @thresholds
    end

    test "micromarkets-shaped failed replies (full log-store keys) burst identically" do
      events =
        for ms <- [0, 100_000, 200_000] do
          mm_ev("reply_sent", "tg:2:0", ms, %{"ok" => false, "from" => "agent_1"})
        end

      f = fetched(events)
      ctx = %{swarm: "mm", thresholds: @thresholds, state: nil, now_ms: 300_000}

      {alerts, _state} = DeliveryFailureBurst.detect(f, ctx)

      assert [%{type: :delivery_failure_burst, cids: ["tg:2:0"]}] = alerts
    end

    test "feed :unavailable / {:error, _} / missing key are no-ops with prior state" do
      ctx = %{swarm: "w", thresholds: @thresholds, state: :prior, now_ms: 300_000}

      for feedless <- [
            %{dashboard: {:ok, %{}}, events: {:ok, []}, feed: :unavailable},
            %{dashboard: {:ok, %{}}, events: {:ok, []}, feed: {:error, :timeout}},
            %{dashboard: {:ok, %{}}, events: {:ok, []}}
          ] do
        assert {[], :prior} = DeliveryFailureBurst.detect(feedless, ctx)
      end
    end

    # ── cross-tick accumulation (the feed is INCREMENTAL: Scope's cursor
    # advances every tick, so each detect/2 sees only the events since the
    # last tick — a stateless recompute is blind to any burst spread across
    # ticks; the detector must accumulate + prune in ctx.state) ──────────────

    defp burst_tick(state, events, now_ms) do
      ctx = %{swarm: "w", thresholds: @thresholds, state: state, now_ms: now_ms}
      DeliveryFailureBurst.detect(fetched(events), ctx)
    end

    test "a burst spread across three ticks (one failure per tick) fires on the third" do
      fail = fn ms -> ev("reply_sent", "tg:2:0", ms, %{"ok" => false}) end

      {[], s1} = burst_tick(nil, [fail.(0)], 10_000)
      {[], s2} = burst_tick(s1, [fail.(100_000)], 110_000)
      {alerts, _s3} = burst_tick(s2, [fail.(200_000)], 210_000)

      assert [
               %{
                 type: :delivery_failure_burst,
                 cids: ["tg:2:0"],
                 key: {"w", :delivery_failure_burst, "tg:2:0"},
                 evidence: %{"cid" => "tg:2:0", "count" => 3, "window_s" => 600}
               }
             ] = alerts
    end

    test "accumulated failures age out: a later tick past the window sees them pruned, no alert" do
      fail = fn ms -> ev("reply_sent", "tg:2:0", ms, %{"ok" => false}) end

      {[], s1} = burst_tick(nil, [fail.(0)], 10_000)
      {[], s2} = burst_tick(s1, [fail.(100_000)], 110_000)
      {[_alert], s3} = burst_tick(s2, [fail.(200_000)], 210_000)

      # a window+ later the three old failures are outside the window — one
      # NEW failure alone must not ride the stale accumulation into an alert
      later = 200_000 + 600_000 + 60_000
      assert {[], s4} = burst_tick(s3, [fail.(later)], later + 1_000)

      # and the pruned cids are gone from state (no unbounded accumulation)
      assert [_] = s4.fail_ts |> Map.fetch!("tg:2:0")
    end

    test "reply_failed events accumulate across ticks into :reply_failed_burst" do
      rf = fn ms, seq -> %{"kind" => "reply_failed", "from" => "agent_3", "seq" => seq, "ts" => ts(ms)} end

      {[], s1} = burst_tick(nil, [rf.(0, 1)], 10_000)
      {[], s2} = burst_tick(s1, [rf.(100_000, 101)], 110_000)
      {alerts, _s3} = burst_tick(s2, [rf.(200_000, 201)], 210_000)

      assert [%{type: :reply_failed_burst}] = alerts
    end

    test "a feed outage mid-burst preserves the accumulated failures" do
      fail = fn ms -> ev("reply_sent", "tg:2:0", ms, %{"ok" => false}) end

      {[], s1} = burst_tick(nil, [fail.(0), fail.(50_000)], 60_000)

      outage = %{dashboard: {:ok, %{}}, events: {:ok, []}, feed: :unavailable}
      ctx = %{swarm: "w", thresholds: @thresholds, state: s1, now_ms: 120_000}
      {[], s2} = DeliveryFailureBurst.detect(outage, ctx)

      {alerts, _s3} = burst_tick(s2, [fail.(200_000)], 210_000)
      assert [%{type: :delivery_failure_burst}] = alerts
    end

    test "a replayed event (same cid + ts, observer-restart ring replay) never double-counts" do
      # det state is PERSISTED across observer restarts (Scope's store) while
      # the feed cursor is session-local — a restart replays the ring into
      # state that already counted those failures. 2 real failures must stay
      # 2, not become 4 and cross the threshold falsely.
      fail = fn ms -> ev("reply_sent", "tg:2:0", ms, %{"ok" => false}) end

      {[], s1} = burst_tick(nil, [fail.(0), fail.(50_000)], 60_000)
      # restart: the same two events replay in one batch
      assert {[], s2} = burst_tick(s1, [fail.(0), fail.(50_000)], 70_000)
      assert s2.fail_ts |> Map.fetch!("tg:2:0") |> length() == 2
    end

    test "dropped cids whose window emptied do not linger in state" do
      fail = fn ms -> ev("reply_sent", "tg:9:9", ms, %{"ok" => false}) end

      {[], s1} = burst_tick(nil, [fail.(0)], 10_000)
      assert Map.has_key?(s1.fail_ts, "tg:9:9")

      {[], s2} = burst_tick(s1, [], 0 + 600_000 + 60_000)
      refute Map.has_key?(s2.fail_ts, "tg:9:9")
    end
  end

  describe "DeliveryFailureBurst — F9: distinct same-ms events both count" do
    @tag regression: "F9"
    test "two reply_failed with the same ts but different seq both count toward the burst" do
      ts = 1_751_734_800.0

      events = [
        %{"kind" => "reply_failed", "seq" => 10, "ts" => ts},
        %{"kind" => "reply_failed", "seq" => 11, "ts" => ts},
        %{"kind" => "reply_failed", "seq" => 12, "ts" => ts + 1.0}
      ]

      ctx = %{
        swarm: "wingston",
        thresholds: %{"delivery_failure.count" => 3, "delivery_failure.window_s" => 600},
        state: nil,
        now_ms: round((ts + 2) * 1000)
      }

      assert {[alert], _} =
               Genswarms.Observer.Detectors.DeliveryFailureBurst.detect(
                 %{feed: {:ok, events}},
                 ctx
               )

      assert alert.type == :reply_failed_burst
      assert alert.evidence["count"] == 3
    end

    @tag regression: "F9"
    test "ring-replay of the same seq is still deduped (restart safety)" do
      ts = 1_751_734_800.0
      events = [%{"kind" => "reply_failed", "seq" => 10, "ts" => ts}]

      ctx = %{
        swarm: "wingston",
        thresholds: %{"delivery_failure.count" => 2, "delivery_failure.window_s" => 600},
        state: nil,
        now_ms: round((ts + 2) * 1000)
      }

      {[], state} =
        Genswarms.Observer.Detectors.DeliveryFailureBurst.detect(%{feed: {:ok, events}}, ctx)

      # Same event replayed (observer restart, cursor reset): must not double-count.
      assert {[], _} =
               Genswarms.Observer.Detectors.DeliveryFailureBurst.detect(
                 %{feed: {:ok, events}},
                 %{ctx | state: state}
               )
    end

    @tag regression: "F9"
    test "old bare-ms persisted state + seq-carrying replay of the same event does not double-count" do
      ts = 1_751_734_800.0
      ms = round(ts * 1000)

      # Old-format persisted state (pre-upgrade): bare ms integers.
      old_state = %{fail_ts: %{}, reply_failed_ts: [ms]}

      # Restart replay: the SAME failure re-delivered, now with its seq.
      events = [%{"kind" => "reply_failed", "seq" => 10, "ts" => ts}]

      ctx = %{
        swarm: "wingston",
        thresholds: %{"delivery_failure.count" => 2, "delivery_failure.window_s" => 600},
        state: old_state,
        now_ms: ms + 2_000
      }

      # threshold 2: a double-count would fire a spurious burst here.
      assert {[], state} =
               Genswarms.Observer.Detectors.DeliveryFailureBurst.detect(%{feed: {:ok, events}}, ctx)

      assert length(state.reply_failed_ts) == 1
      assert [{{:seq, 10}, ^ms}] = state.reply_failed_ts
    end
  end

  # ── TopicsStale ──────────────────────────────────────────────────────────────

  describe "TopicsStale" do
    defp envelope_with_periods(periods) do
      %{"extensions" => %{"conversation_topics" => %{"periods" => periods}}}
    end

    # Midday UTC on `date_str`, expressed as ms-since-epoch — the only clock
    # `ctx.now_ms` is ever allowed to come from in these tests.
    defp now_for_date(date_str) do
      {:ok, date} = Date.from_iso8601(date_str)
      {y, m, d} = Date.to_erl(date)

      NaiveDateTime.new!(y, m, d, 12, 0, 0)
      |> DateTime.from_naive!("Etc/UTC")
      |> DateTime.to_unix(:millisecond)
    end

    test "newest final period older than threshold fires topics_stale" do
      envelope =
        envelope_with_periods([
          %{"period_id" => "2026-07-01", "final" => true},
          %{"period_id" => "2026-07-03", "final" => true},
          %{"period_id" => "2026-07-06", "final" => false}
        ])

      f = %{dashboard: {:ok, envelope}, events: {:ok, []}}
      now_ms = now_for_date("2026-07-06")
      ctx = %{swarm: "w", thresholds: %{"topics_stale.periods" => 1}, state: nil, now_ms: now_ms}

      {alerts, state} = TopicsStale.detect(f, ctx)

      assert [%{type: :topics_stale, key: {"w", :topics_stale}, cids: []}] = alerts
      assert state == %{ever_seen: true}
    end

    test "newest final period within threshold raises nothing" do
      envelope = envelope_with_periods([%{"period_id" => "2026-07-05", "final" => true}])
      f = %{dashboard: {:ok, envelope}, events: {:ok, []}}
      now_ms = now_for_date("2026-07-06")
      ctx = %{swarm: "w", thresholds: %{"topics_stale.periods" => 1}, state: nil, now_ms: now_ms}

      assert {[], %{ever_seen: true}} = TopicsStale.detect(f, ctx)
    end

    test "never had the extension: no alert, state stays not-seen" do
      f = %{dashboard: {:ok, %{"extensions" => %{}}}, events: {:ok, []}}
      now_ms = now_for_date("2026-07-06")

      ctx = %{
        swarm: "w",
        thresholds: %{"topics_stale.periods" => 1},
        state: %{ever_seen: false},
        now_ms: now_ms
      }

      assert {[], %{ever_seen: false}} = TopicsStale.detect(f, ctx)
    end

    test "seen once then extension goes absent: absence itself alerts" do
      f = %{dashboard: {:ok, %{"extensions" => %{}}}, events: {:ok, []}}
      now_ms = now_for_date("2026-07-06")

      ctx = %{
        swarm: "w",
        thresholds: %{"topics_stale.periods" => 1},
        state: %{ever_seen: true},
        now_ms: now_ms
      }

      {alerts, state} = TopicsStale.detect(f, ctx)

      assert [%{type: :topics_stale, evidence: %{"reason" => "extension_absent_or_malformed"}}] =
               alerts

      assert state == %{ever_seen: true}
    end

    test "seen once then malformed (periods not a list) alerts" do
      envelope = %{"extensions" => %{"conversation_topics" => %{"periods" => "not-a-list"}}}
      f = %{dashboard: {:ok, envelope}, events: {:ok, []}}
      now_ms = now_for_date("2026-07-06")

      ctx = %{
        swarm: "w",
        thresholds: %{"topics_stale.periods" => 1},
        state: %{ever_seen: true},
        now_ms: now_ms
      }

      {alerts, _state} = TopicsStale.detect(f, ctx)

      assert [%{type: :topics_stale}] = alerts
    end

    test "malformed entries (non-map) on first sight do not alert and do not mark seen" do
      envelope = %{"extensions" => %{"conversation_topics" => %{"periods" => ["oops", 1]}}}
      f = %{dashboard: {:ok, envelope}, events: {:ok, []}}
      now_ms = now_for_date("2026-07-06")

      ctx = %{
        swarm: "w",
        thresholds: %{"topics_stale.periods" => 1},
        state: %{ever_seen: false},
        now_ms: now_ms
      }

      assert {[], %{ever_seen: false}} = TopicsStale.detect(f, ctx)
    end

    test "well-formed but no final periods yet: no alert, does not mark seen" do
      envelope = envelope_with_periods([%{"period_id" => "2026-07-06", "final" => false}])
      f = %{dashboard: {:ok, envelope}, events: {:ok, []}}
      now_ms = now_for_date("2026-07-06")

      ctx = %{
        swarm: "w",
        thresholds: %{"topics_stale.periods" => 1},
        state: %{ever_seen: false},
        now_ms: now_ms
      }

      assert {[], %{ever_seen: false}} = TopicsStale.detect(f, ctx)
    end

    test "default_thresholds exposes only its namespaced keys" do
      assert TopicsStale.default_thresholds() == %{"topics_stale.periods" => 1, "topics_stale.grace_hours" => 1}
    end

    # F8: dashboard fetch errors are not 'extension missing'
    @tag regression: "F8"
    test "dashboard {:error, _} after ever_seen is a no-op with prior state" do
      ctx = %{
        swarm: "wingston",
        thresholds: %{"topics_stale.periods" => 1},
        state: %{ever_seen: true},
        now_ms: 1_751_734_800_000
      }

      fetched = %{dashboard: {:error, {:client_crash, "timeout"}}, events: {:ok, []}}
      assert {[], %{ever_seen: true}} =
               Genswarms.Observer.Detectors.TopicsStale.detect(fetched, ctx)
    end

    @tag regression: "F8"
    test "missing :dashboard key is a no-op, not a missing-extension alert" do
      ctx = %{
        swarm: "wingston",
        thresholds: %{"topics_stale.periods" => 1},
        state: %{ever_seen: true},
        now_ms: 1_751_734_800_000
      }

      assert {[], %{ever_seen: true}} =
               Genswarms.Observer.Detectors.TopicsStale.detect(%{events: {:ok, []}}, ctx)
    end

    @tag regression: "F8"
    test "a fetched envelope WITHOUT the extension still alerts after ever_seen" do
      ctx = %{
        swarm: "wingston",
        thresholds: %{"topics_stale.periods" => 1},
        state: %{ever_seen: true},
        now_ms: 1_751_734_800_000
      }

      fetched = %{dashboard: {:ok, %{"swarm" => "wingston"}}, events: {:ok, []}}
      assert {[alert], _} = Genswarms.Observer.Detectors.TopicsStale.detect(fetched, ctx)
      assert alert.type == :topics_stale
      assert alert.evidence["reason"] == "extension_absent_or_malformed"
    end
  end

  # ── TopicsStale — F3: midnight grace window ──────────────────────────────────

  describe "TopicsStale — F3: midnight grace window" do
    # 2026-07-06 00:05:00 UTC in ms (corrected from brief: brief had 1_782_950_700_000 which is 2026-07-02)
    @just_after_midnight 1_783_296_300_000

    defp topics_fetched(period_id) do
      %{
        dashboard:
          {:ok,
           %{
             "extensions" => %{
               "conversation_topics" => %{
                 "periods" => [%{"period_id" => period_id, "final" => true}]
               }
             }
           }},
        events: {:ok, []}
      }
    end

    @tag regression: "F3"
    test "at 00:05 UTC, newest final = D-2 does NOT alert (producer closes D-1 at 00:15)" do
      ctx = %{
        swarm: "wingston",
        thresholds: %{"topics_stale.periods" => 1, "topics_stale.grace_hours" => 1},
        state: %{ever_seen: true},
        now_ms: @just_after_midnight
      }

      # now = 2026-07-06 00:05Z; newest final period = 2026-07-04 (D-2).
      # Without grace: today=07-06, cutoff=07-05, 07-04 < 07-05 → false alarm.
      # With 1h grace: effective today=07-05, cutoff=07-04 → healthy.
      assert {[], _} =
               Genswarms.Observer.Detectors.TopicsStale.detect(topics_fetched("2026-07-04"), ctx)
    end

    @tag regression: "F3"
    test "at 02:00 UTC, newest final = D-2 DOES alert (grace expired, close missed)" do
      two_am = @just_after_midnight + 115 * 60_000

      ctx = %{
        swarm: "wingston",
        thresholds: %{"topics_stale.periods" => 1, "topics_stale.grace_hours" => 1},
        state: %{ever_seen: true},
        now_ms: two_am
      }

      assert {[alert], _} =
               Genswarms.Observer.Detectors.TopicsStale.detect(topics_fetched("2026-07-04"), ctx)

      assert alert.type == :topics_stale
    end
  end

  # ── TopicsStale — F2 guard: poisoned persisted state restarts clean ─────────
  #
  # Task 1 added `normalize_state/1` to topics_stale.ex but nothing exercised
  # its fallback branch: a map missing `:ever_seen` (would KeyError on
  # `state.ever_seen` pre-guard) and a non-map entirely. Both must restart
  # clean (`%{ever_seen: false}`) rather than crash the tick.
  describe "TopicsStale — F2 guard: poisoned persisted state restarts clean" do
    @tag regression: "F2"
    test "ctx.state missing :ever_seen does not crash detect/2" do
      ctx = %{
        swarm: "wingston",
        thresholds: %{"topics_stale.periods" => 1, "topics_stale.grace_hours" => 1},
        state: %{},
        now_ms: 1_751_734_800_000
      }

      fetched = %{dashboard: {:ok, %{"swarm" => "wingston"}}, events: {:ok, []}}

      assert {[], %{ever_seen: false}} =
               Genswarms.Observer.Detectors.TopicsStale.detect(fetched, ctx)
    end

    @tag regression: "F2"
    test "a non-map ctx.state does not crash detect/2" do
      ctx = %{
        swarm: "wingston",
        thresholds: %{"topics_stale.periods" => 1, "topics_stale.grace_hours" => 1},
        state: :garbage,
        now_ms: 1_751_734_800_000
      }

      fetched = %{dashboard: {:ok, %{"swarm" => "wingston"}}, events: {:ok, []}}

      assert {[], %{ever_seen: false}} =
               Genswarms.Observer.Detectors.TopicsStale.detect(fetched, ctx)
    end
  end
end
