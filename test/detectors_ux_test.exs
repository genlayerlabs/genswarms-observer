defmodule Genswarms.Observer.DetectorsUxTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Detectors.{DeliveryFailureBurst, TopicsStale, Unanswered}

  # `iso/1` here takes an ABSOLUTE ms-since-epoch (matching the brief's
  # `ev.(kind, cid, ms, extra)` helper), unlike detectors_test.exs's
  # ms_ago-based helper — the brief's fixtures are written against a fixed
  # epoch-relative timeline.
  defp iso(ms), do: ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()

  defp ev(kind, cid, ms, extra \\ %{}) do
    Map.merge(%{"kind" => kind, "cid" => cid, "timestamp" => iso(ms)}, extra)
  end

  defp fetched(events, dashboard \\ %{}),
    do: %{dashboard: {:ok, dashboard}, events: {:ok, events}}

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
      events =
        for ms <- [0, 100_000, 200_000] do
          %{"kind" => "reply_failed", "timestamp" => iso(ms)}
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
