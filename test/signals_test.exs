defmodule Genswarms.Observer.SignalsTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Signals

  # ── shipped-rule fixtures (verbatim from the plan) ─────────────────────
  #
  # These are the REAL consumers of this evaluator — copied verbatim from
  # docs/superpowers/plans/2026-07-07-observability-stages-2-4.md so the
  # grammar this module implements is proven against its actual shipped
  # payloads, not just synthetic cases.

  # Task 1 (genswarms-objects packages/cron/cron.ex) — cron "missed_tick".
  @cron_missed_tick %{
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

  # Task 2 (genswarms-llm-proxy lib/genswarms/llm_proxy.ex) — budget_guard_75,
  # the "where WITHOUT each" (ceiling > 0 guard) shape.
  @budget_guard_75 %{
    "id" => "budget_guard_75",
    "severity" => "info",
    "card" => "LLM spend at 75% of the daily ceiling",
    "where" => %{"op" => "gt", "lhs" => %{"path" => "ceiling_usd"}, "rhs" => 0},
    "when" => %{
      "op" => "gte",
      "lhs" => %{"div" => [%{"path" => "spent_usd"}, %{"path" => "ceiling_usd"}]},
      "rhs" => 0.75
    }
  }

  # Task 3 (genswarms-telegram lib/genswarms/telegram/dashboard.ex) — the
  # "poller_deaf" nil-lit guard and "poll_conflict" delta rule.
  @poller_deaf %{
    "id" => "poller_deaf",
    "severity" => "warn",
    "card" => "telegram poller has not completed a successful getUpdates in over 2 minutes",
    "where" => %{"op" => "neq", "lhs" => %{"path" => "last_poll_ok_ms"}, "rhs" => %{"lit" => nil}},
    "when" => %{
      "op" => "gt",
      "lhs" => %{"sub" => ["now", %{"path" => "last_poll_ok_ms"}]},
      "rhs" => 120_000
    }
  }

  @poll_conflict %{
    "id" => "poll_conflict",
    "severity" => "warn",
    "card" => "getUpdates 409 conflict — two pollers are fighting over this bot token",
    "when" => %{"op" => "gt", "lhs" => %{"delta" => "conflict_count"}, "rhs" => 0}
  }

  # Task 4 (wingstonrallybot objects/dashboard_source.ex) — "../" host-only
  # escape into the whole extensions map.
  @metrics_rejected_rule %{
    "id" => "metrics_rejected",
    "severity" => "warn",
    "card" => "an object is bumping a non-allowlisted metrics key",
    "when" => %{
      "op" => "gt",
      "lhs" => %{"delta" => "../metrics_today.metrics_rejected"},
      "rhs" => 0
    }
  }

  # ── validate_rules/1 ─────────────────────────────────────────────────────

  describe "validate_rules/1 — shipped fixtures" do
    test "all four shipped rules validate as-is" do
      for rule <- [
            @cron_missed_tick,
            @budget_guard_75,
            @poller_deaf,
            @poll_conflict,
            @metrics_rejected_rule
          ] do
        assert {:ok, [validated]} = Signals.validate_rules([rule])
        assert validated["id"] == rule["id"]
      end
    end
  end

  describe "validate_rules/1 — bounds and structure" do
    test "more than 16 rules is rejected with a readable reason" do
      rules = for i <- 1..17, do: %{@poll_conflict | "id" => "r#{i}"}
      assert {:error, reason} = Signals.validate_rules(rules)
      assert reason =~ "17"
      assert reason =~ "16"
    end

    test "more than 32 cond/operand nodes in one rule is rejected" do
      # Nest add/sub arithmetic deep enough to blow the 32-node cap.
      deep =
        Enum.reduce(1..20, 1, fn _n, acc -> %{"add" => [acc, 1]} end)

      rule = %{
        "id" => "too_deep",
        "card" => "x",
        "when" => %{"op" => "gt", "lhs" => deep, "rhs" => 0}
      }

      assert {:error, reason} = Signals.validate_rules([rule])
      assert reason =~ "node count"
    end

    test "bad id format is rejected, naming the offending index" do
      rule = %{@poll_conflict | "id" => "Not Valid!"}
      assert {:error, reason} = Signals.validate_rules([rule])
      assert reason =~ "id"
      assert reason =~ "index 0"
    end

    test "unknown op is rejected" do
      rule = put_in(@poll_conflict, ["when", "op"], "flarp")
      assert {:error, reason} = Signals.validate_rules([rule])
      assert reason =~ "unknown op"
    end

    test "non-map rule is rejected" do
      assert {:error, reason} = Signals.validate_rules(["not-a-map"])
      assert reason =~ "not a map"
    end

    test "non-list input is rejected" do
      assert {:error, _} = Signals.validate_rules(%{"id" => "x"})
    end

    test "missing required when is rejected" do
      rule = Map.delete(@poll_conflict, "when")
      assert {:error, reason} = Signals.validate_rules([rule])
      assert reason =~ "when"
    end

    test "severity defaults to warn when absent" do
      rule = Map.delete(@poll_conflict, "severity")
      assert {:ok, [validated]} = Signals.validate_rules([rule])
      assert validated["severity"] == "warn"
    end

    test "card over 200 chars is rejected" do
      rule = %{@poll_conflict | "card" => String.duplicate("x", 201)}
      assert {:error, reason} = Signals.validate_rules([rule])
      assert reason =~ "card"
    end
  end

  # ── evaluate/7 — operators ───────────────────────────────────────────────

  describe "evaluate/7 — comparison operators" do
    ops_table = [
      {"gt", 5, 3, true},
      {"gt", 3, 5, false},
      {"gte", 5, 5, true},
      {"gte", 4, 5, false},
      {"lt", 3, 5, true},
      {"lt", 5, 3, false},
      {"lte", 5, 5, true},
      {"lte", 6, 5, false}
    ]

    for {op, lhs, rhs, expected} <- ops_table do
      test "#{op}(#{lhs}, #{rhs}) => #{expected}" do
        rule = %{
          "id" => "cmp",
          "card" => "c",
          "when" => %{"op" => unquote(op), "lhs" => unquote(lhs), "rhs" => unquote(rhs)}
        }

        {alerts, _} = Signals.evaluate("b", %{}, [rule], %{}, %{}, "swarm", 0)
        assert alerts != [] == unquote(expected)
      end
    end

    test "eq with strings" do
      rule = %{
        "id" => "e",
        "card" => "c",
        "when" => %{"op" => "eq", "lhs" => %{"lit" => "active"}, "rhs" => %{"lit" => "active"}}
      }

      {alerts, _} = Signals.evaluate("b", %{}, [rule], %{}, %{}, "s", 0)
      assert [_] = alerts
    end

    test "neq with strings" do
      rule = %{
        "id" => "e",
        "card" => "c",
        "when" => %{"op" => "neq", "lhs" => %{"lit" => "active"}, "rhs" => %{"lit" => "paused"}}
      }

      {alerts, _} = Signals.evaluate("b", %{}, [rule], %{}, %{}, "s", 0)
      assert [_] = alerts
    end

    test "now operand resolves to now_ms" do
      rule = %{"id" => "n", "card" => "c", "when" => %{"op" => "eq", "lhs" => "now", "rhs" => 42}}
      {alerts, _} = Signals.evaluate("b", %{}, [rule], %{}, %{}, "s", 42)
      assert [_] = alerts
    end

    test "add/sub/mul/div arithmetic" do
      for {op, a, b, expected} <- [
            {"add", 2, 3, 5},
            {"sub", 5, 2, 3},
            {"mul", 3, 4, 12},
            {"div", 10, 4, 2.5}
          ] do
        rule = %{
          "id" => "a",
          "card" => "c",
          "when" => %{"op" => "eq", "lhs" => %{op => [a, b]}, "rhs" => expected}
        }

        {alerts, _} = Signals.evaluate("b", %{}, [rule], %{}, %{}, "s", 0)
        assert [_] = alerts, "#{op}(#{a},#{b}) expected #{expected}"
      end
    end

    test "div by zero no-ops (no alert)" do
      rule = %{
        "id" => "d",
        "card" => "c",
        "when" => %{"op" => "eq", "lhs" => %{"div" => [10, 0]}, "rhs" => 999}
      }

      assert {[], _} = Signals.evaluate("b", %{}, [rule], %{}, %{}, "s", 0)
    end

    test "missing path no-ops" do
      rule = %{
        "id" => "m",
        "card" => "c",
        "when" => %{"op" => "gt", "lhs" => %{"path" => "nope"}, "rhs" => 0}
      }

      assert {[], _} = Signals.evaluate("b", %{"other" => 1}, [rule], %{}, %{}, "s", 0)
    end
  end

  # ── each / where ─────────────────────────────────────────────────────────

  describe "evaluate/7 — each + where (cron missed_tick fixture)" do
    test "overdue active job fires one alert per matching item, item_key from name" do
      block = %{
        "jobs" => [
          %{"name" => "sync", "state" => "active", "next_run_at_ms" => 0},
          %{"name" => "paused_job", "state" => "paused", "next_run_at_ms" => 0}
        ]
      }

      now_ms = 2_000_000

      {alerts, _} = Signals.evaluate("cron", block, [@cron_missed_tick], %{}, %{}, "w", now_ms)

      assert [alert] = alerts
      assert alert.type == :health_rule
      assert alert.key == {"w", :health_rule, "cron", "missed_tick", "sync"}
      assert alert.summary == "cron job sync did not run (overdue past grace)"

      assert alert.evidence == %{
               "block" => "cron",
               "rule_id" => "missed_tick",
               "severity" => "warn"
             }

      assert alert.cids == []
    end

    test "paused job (where filters it out) raises nothing" do
      block = %{"jobs" => [%{"name" => "paused_job", "state" => "paused", "next_run_at_ms" => 0}]}

      assert {[], _} =
               Signals.evaluate("cron", block, [@cron_missed_tick], %{}, %{}, "w", 2_000_000)
    end

    test "not-yet-overdue active job raises nothing" do
      block = %{
        "jobs" => [%{"name" => "sync", "state" => "active", "next_run_at_ms" => 10_000_000}]
      }

      assert {[], _} =
               Signals.evaluate("cron", block, [@cron_missed_tick], %{}, %{}, "w", 2_000_000)
    end

    test "each path resolving to a non-list no-ops the whole rule" do
      block = %{"jobs" => %{"not" => "a list"}}

      assert {[], _} =
               Signals.evaluate("cron", block, [@cron_missed_tick], %{}, %{}, "w", 2_000_000)
    end

    test "each path missing entirely no-ops the whole rule" do
      assert {[], _} =
               Signals.evaluate("cron", %{}, [@cron_missed_tick], %{}, %{}, "w", 2_000_000)
    end
  end

  describe "evaluate/7 — where WITHOUT each (budget_guard_75 fixture)" do
    test "where evaluated against the block itself; ceiling>0 and spend over 75% fires" do
      block = %{"ceiling_usd" => 10.0, "spent_usd" => 8.0}

      {alerts, _} =
        Signals.evaluate("llm_proxy_budget", block, [@budget_guard_75], %{}, %{}, "w", 0)

      assert [alert] = alerts
      assert alert.key == {"w", :health_rule, "llm_proxy_budget", "budget_guard_75", nil}
    end

    test "ceiling disabled (0) makes the guard inert" do
      block = %{"ceiling_usd" => 0.0, "spent_usd" => 100.0}

      assert {[], _} =
               Signals.evaluate("llm_proxy_budget", block, [@budget_guard_75], %{}, %{}, "w", 0)
    end

    test "spend under 75% raises nothing" do
      block = %{"ceiling_usd" => 10.0, "spent_usd" => 1.0}

      assert {[], _} =
               Signals.evaluate("llm_proxy_budget", block, [@budget_guard_75], %{}, %{}, "w", 0)
    end
  end

  describe "evaluate/7 — nil-lit neq guard (poller_deaf fixture)" do
    test "last_poll_ok_ms present and stale fires" do
      block = %{"last_poll_ok_ms" => 0}

      {alerts, _} =
        Signals.evaluate("telegram_poller", block, [@poller_deaf], %{}, %{}, "w", 200_000)

      assert [_] = alerts
    end

    test "last_poll_ok_ms explicitly nil (never polled) is guarded out — no alert" do
      block = %{"last_poll_ok_ms" => nil}

      assert {[], _} =
               Signals.evaluate("telegram_poller", block, [@poller_deaf], %{}, %{}, "w", 200_000)
    end

    test "last_poll_ok_ms recent (within 2 min) raises nothing" do
      block = %{"last_poll_ok_ms" => 190_000}

      assert {[], _} =
               Signals.evaluate("telegram_poller", block, [@poller_deaf], %{}, %{}, "w", 200_000)
    end
  end

  # ── delta ────────────────────────────────────────────────────────────────

  describe "evaluate/7 — delta (poll_conflict fixture)" do
    test "first sight: no alert, but the current value is recorded into samples" do
      block = %{"conflict_count" => 1}

      {alerts, samples} =
        Signals.evaluate("telegram_poller", block, [@poll_conflict], %{}, %{}, "w", 0)

      assert alerts == []
      assert samples == %{{"telegram_poller", "poll_conflict", "conflict_count"} => 1}
    end

    test "second tick with an increase fires; a flat third tick does not (verbatim tick1/tick2/tick3 story)" do
      block1 = %{"conflict_count" => 0}

      {alerts1, s1} =
        Signals.evaluate("telegram_poller", block1, [@poll_conflict], %{}, %{}, "w", 0)

      assert alerts1 == []

      block2 = %{"conflict_count" => 1}

      {alerts2, s2} =
        Signals.evaluate("telegram_poller", block2, [@poll_conflict], %{}, s1, "w", 1_000)

      assert [_] = alerts2

      block3 = %{"conflict_count" => 1}

      {alerts3, _s3} =
        Signals.evaluate("telegram_poller", block3, [@poll_conflict], %{}, s2, "w", 2_000)

      assert alerts3 == []
    end

    @tag regression: "F9"
    test "the same delta in where and when reads the pre-tick sample both times" do
      rule = %{
        "id" => "same_delta_twice",
        "card" => "delta jumped",
        "where" => %{"op" => "gt", "lhs" => %{"delta" => "conflict_count"}, "rhs" => 0},
        "when" => %{"op" => "gt", "lhs" => %{"delta" => "conflict_count"}, "rhs" => 0}
      }

      samples = %{{"telegram_poller", "same_delta_twice", "conflict_count"} => 1}

      {alerts, new_samples} =
        Signals.evaluate("telegram_poller", %{"conflict_count" => 3}, [rule], %{}, samples, "w", 0)

      assert [_] = alerts
      assert new_samples == %{{"telegram_poller", "same_delta_twice", "conflict_count"} => 3}
    end
  end

  describe "evaluate/7 — \"../\" host-trust escape (metrics_health fixture)" do
    test "delta path with ../ prefix resolves against the whole extensions map" do
      extensions = %{"metrics_today" => %{"metrics_rejected" => 1}}

      {alerts1, s1} =
        Signals.evaluate("metrics_health", %{}, [@metrics_rejected_rule], extensions, %{}, "w", 0)

      assert alerts1 == []

      assert s1 == %{
               {"metrics_health", "metrics_rejected", "../metrics_today.metrics_rejected"} => 1
             }

      extensions2 = %{"metrics_today" => %{"metrics_rejected" => 2}}

      {alerts2, _s2} =
        Signals.evaluate(
          "metrics_health",
          %{},
          [@metrics_rejected_rule],
          extensions2,
          s1,
          "w",
          1_000
        )

      assert [_] = alerts2
    end

    @tag regression: "F9"
    test "each items all see the same pre-tick block-level delta via ../" do
      rule = %{
        "id" => "fanout",
        "card" => "item {name} saw rejected metrics",
        "each" => "items",
        "when" => %{
          "op" => "gt",
          "lhs" => %{"delta" => "../metrics_today.metrics_rejected"},
          "rhs" => 0
        }
      }

      block = %{"items" => [%{"name" => "a"}, %{"name" => "b"}, %{"name" => "c"}]}
      extensions = %{"metrics_today" => %{"metrics_rejected" => 5}}
      samples = %{{"metrics_health", "fanout", "../metrics_today.metrics_rejected"} => 1}

      {alerts, new_samples} =
        Signals.evaluate("metrics_health", block, [rule], extensions, samples, "w", 0)

      assert Enum.map(alerts, & &1.summary) == [
               "item a saw rejected metrics",
               "item b saw rejected metrics",
               "item c saw rejected metrics"
             ]

      assert new_samples == %{{"metrics_health", "fanout", "../metrics_today.metrics_rejected"} => 5}
    end
  end

  # ── interpolation ────────────────────────────────────────────────────────

  describe "card interpolation" do
    test "missing field interpolates to ?" do
      rule = %{
        "id" => "i",
        "card" => "value is {missing_field}",
        "when" => %{"op" => "eq", "lhs" => 1, "rhs" => 1}
      }

      {[alert], _} = Signals.evaluate("b", %{}, [rule], %{}, %{}, "s", 0)
      assert alert.summary == "value is ?"
    end

    test "item field takes priority over block field" do
      rule = %{
        "id" => "i",
        "card" => "{name}",
        "each" => "items",
        "when" => %{"op" => "eq", "lhs" => 1, "rhs" => 1}
      }

      block = %{"items" => [%{"name" => "item-name"}], "name" => "block-name"}
      {[alert], _} = Signals.evaluate("b", block, [rule], %{}, %{}, "s", 0)
      assert alert.summary == "item-name"
    end

    test "falls back to block field when item lacks it" do
      rule = %{
        "id" => "i",
        "card" => "{name}",
        "each" => "items",
        "when" => %{"op" => "eq", "lhs" => 1, "rhs" => 1}
      }

      block = %{"items" => [%{}], "name" => "block-name"}
      {[alert], _} = Signals.evaluate("b", block, [rule], %{}, %{}, "s", 0)
      assert alert.summary == "block-name"
    end

    test "summary is sliced to 200 chars" do
      rule = %{
        "id" => "i",
        "card" => String.duplicate("x", 250),
        "when" => %{"op" => "eq", "lhs" => 1, "rhs" => 1}
      }

      {[alert], _} = Signals.evaluate("b", %{}, [rule], %{}, %{}, "s", 0)
      assert String.length(alert.summary) == 200
    end
  end

  # ── evaluate/7 totality against garbage that slipped validation ─────────

  describe "evaluate/7 — totality against malformed rules" do
    test "a completely garbage rule shape is skipped, not raised" do
      garbage = ["not a map", %{"no" => "when key"}, 42, nil]
      assert {[], %{}} = Signals.evaluate("b", %{}, garbage, %{}, %{}, "s", 0)
    end

    test "one garbage rule among valid rules does not stop the others" do
      rules = [%{"bad" => true}, @poll_conflict]
      block = %{"conflict_count" => 5}
      {alerts, samples} = Signals.evaluate("telegram_poller", block, rules, %{}, %{}, "w", 0)
      assert alerts == []
      assert samples == %{{"telegram_poller", "poll_conflict", "conflict_count"} => 5}
    end
  end
end
