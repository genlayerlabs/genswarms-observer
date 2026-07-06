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
    Map.merge(%{"kind" => kind, "cid" => cid, "seq" => 1, "ts" => ts(ms)}, extra)
  end

  # Full micromarkets-shaped feed event (provenance above).
  defp mm_ev(kind, cid, ms, extra) do
    Map.merge(
      %{
        "kind" => kind,
        "cid" => cid,
        "ts" => ts(ms),
        "seq" => 1,
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
        for ms <- [0, 100_000, 200_000] do
          %{"kind" => "reply_failed", "from" => "agent_3", "seq" => 1, "ts" => ts(ms)}
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

      assert {[], nil} = DeliveryFailureBurst.detect(f, ctx)
    end

    test "events outside the window are excluded from the count" do
      events =
        for ms <- [0, 100_000] do
          ev("reply_sent", "tg:2:0", ms, %{"ok" => false})
        end ++ [ev("reply_sent", "tg:2:0", -1_000_000, %{"ok" => false})]

      f = fetched(events)
      ctx = %{swarm: "w", thresholds: @thresholds, state: nil, now_ms: 300_000}

      assert {[], nil} = DeliveryFailureBurst.detect(f, ctx)
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

    test "default_thresholds exposes only its namespaced key" do
      assert TopicsStale.default_thresholds() == %{"topics_stale.periods" => 1}
    end
  end
end
