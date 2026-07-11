defmodule Genswarms.Observer.OutboxTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Outbox

  test "send_card wraps the card in a send_card action to the sender" do
    {:ok, box} = Agent.start_link(fn -> [] end)

    deliver = fn target, from, content ->
      Agent.update(box, &[{target, from, content} | &1])
      :ok
    end

    assert :ok = Outbox.send_card(deliver, :sender, :scope, "tg:42:0", %{"title" => "t"})

    [{:sender, :scope, content}] = Agent.get(box, & &1)
    assert %{"action" => "send_card", "conversation_id" => "tg:42:0"} = Jason.decode!(content)
  end

  test "a non-:ok delivery result is returned to the caller" do
    deliver = fn _, _, _ -> {:error, :down} end
    assert {:error, :down} = Outbox.send_card(deliver, :sender, :scope, "tg:42:0", %{})
  end

  # ── human-readable cards (2026-07-09 redesign: "his notifications are total crap") ──

  defp card_text(card), do: card["blocks"] |> Enum.map(& &1["text"]) |> Enum.join("\n")

  test "unanswered renders a human sentence, no raw JSON, with an investigate tail" do
    alert = %{
      type: :unanswered,
      swarm: "wingston-prod",
      at_ms: 1_783_611_350_377,
      summary: "request tg:7790150175:0 unanswered for 16 min",
      evidence: %{"opened_at_ms" => 1_783_611_350_377, "waited_minutes" => 16},
      cids: ["tg:7790150175:0"]
    }

    card = Outbox.alert_card(alert, %{"dashboard_url" => "http://internal-elb", "repo" => "x/y"})
    text = card_text(card)

    assert card["title"] =~ "waiting"
    assert text =~ "16 min"
    # decoded, not dumped: no JSON braces/escapes, no internal URLs
    refute text =~ "{\""
    refute text =~ "internal-elb"
    refute text =~ "evidence:"
    # the machine tail keeps what Claude needs to investigate
    assert text =~ "tg:7790150175:0"
    assert text =~ "wingston-prod"
  end

  test "an unanswered alert right after a restart says so (correlation)" do
    restart = %{
      type: :endpoint_down,
      swarm: "wingston-prod",
      at_ms: 1_000_000,
      summary: ~s(dashboard fetch failed: {:http_status, 404, "{\"error\":\"swarm_not_found\"}"}),
      evidence: %{"reason" => "..."},
      cids: []
    }

    alert = %{
      type: :unanswered,
      swarm: "wingston-prod",
      at_ms: 1_000_000 + 5 * 60_000,
      summary: "request tg:1:0 unanswered for 15 min",
      evidence: %{"waited_minutes" => 15},
      cids: ["tg:1:0"]
    }

    card = Outbox.alert_card(alert, %{}, [restart])
    assert card_text(card) =~ "restart"

    # ...but not when the restart was long ago or another swarm
    old_restart = %{restart | at_ms: alert.at_ms - 60 * 60_000}
    refute Outbox.alert_card(alert, %{}, [old_restart]) |> card_text() =~ "restart"

    other_swarm = %{restart | swarm: "elsewhere"}
    refute Outbox.alert_card(alert, %{}, [other_swarm]) |> card_text() =~ "restart"
  end

  test "a POSITIVE restart (swarm_restarted) also powers the unanswered correlation" do
    restart = %{
      type: :swarm_restarted,
      swarm: "wingston-prod",
      at_ms: 1_000_000,
      summary: "pod restarted (rehydrated 812 feed rows)",
      evidence: %{"count" => 1, "rehydrated_rows" => 812},
      cids: []
    }

    alert = %{
      type: :unanswered,
      swarm: "wingston-prod",
      at_ms: 1_000_000 + 5 * 60_000,
      summary: "request tg:1:0 unanswered for 15 min",
      evidence: %{"waited_minutes" => 15},
      cids: ["tg:1:0"]
    }

    assert Outbox.alert_card(alert, %{}, [restart]) |> card_text() =~ "restart"
  end

  test "swarm_restarted renders a quiet human card: deploy hint, row count, no investigate tail" do
    alert = %{
      type: :swarm_restarted,
      swarm: "wingston-prod",
      at_ms: 1,
      summary: "pod restarted (rehydrated 812 feed rows)",
      evidence: %{"count" => 1, "rehydrated_rows" => 812},
      cids: []
    }

    card = Outbox.alert_card(alert, %{"dashboard_url" => "http://internal-elb"})
    text = card_text(card)

    assert card["title"] == "🔄 wingston-prod: pod restarted"
    assert text =~ "deploy"
    assert text =~ "812"
    refute text =~ "investigate"
    refute text =~ "internal-elb"
  end

  test "a multi-boot tick shows the count in the title" do
    alert = %{
      type: :swarm_restarted,
      swarm: "w",
      at_ms: 1,
      summary: "pod restarted ×2",
      evidence: %{"count" => 2},
      cids: []
    }

    assert Outbox.alert_card(alert, %{})["title"] =~ "×2"
  end

  test "restart_loop is investigable: explanation + machine tail" do
    alert = %{
      type: :restart_loop,
      swarm: "wingston-prod",
      at_ms: 1,
      summary: "4 pod restarts in 1800s",
      evidence: %{"count" => 4, "window_s" => 1800},
      cids: []
    }

    card = Outbox.alert_card(alert, %{})
    text = card_text(card)

    assert card["title"] =~ "restart loop"
    assert card["title"] =~ "4"
    assert text =~ "💡"
    assert text =~ "investigate"
  end

  test "endpoint_down swarm_not_found reads as a deploy/restart blip without an investigate tail" do
    alert = %{
      type: :endpoint_down,
      swarm: "wingston-prod",
      at_ms: 1,
      summary: ~s(dashboard fetch failed: {:http_status, 404, "{\"error\":\"swarm_not_found\"}"}),
      evidence: %{"reason" => ~s({:http_status, 404, "{\"error\":\"swarm_not_found\"}"})},
      cids: []
    }

    card = Outbox.alert_card(alert, %{"dashboard_url" => "http://internal-elb", "repo" => "x/y"})
    text = card_text(card)

    assert card["title"] =~ "restart" or card["title"] =~ "unreachable"
    assert text =~ "deploy"
    refute text =~ "investigate"
    refute text =~ "internal-elb"
    # no escaped-JSON soup
    refute text =~ "\\\""
  end

  test "types without a custom body keep the 💡 explanation and get compact evidence lines" do
    alert = %{
      type: :pool_saturated,
      swarm: "wingston",
      at_ms: 1,
      summary: "pool saturated",
      evidence: %{"leased" => 48, "size" => 48},
      cids: []
    }

    card = Outbox.alert_card(alert, %{})
    text = card_text(card)

    assert text =~ "💡"
    assert text =~ "leased 48" or text =~ "leased: 48"
    refute text =~ "{\""
  end

  @tag regression: "F14"
  test "health_rules_gone alert cards include guidance" do
    alert = %{
      type: :health_rules_gone,
      swarm: "wingston",
      at_ms: 1,
      summary: "rules disappeared",
      evidence: %{"block" => "telegram_poller"},
      cids: []
    }

    card = Outbox.alert_card(alert, %{"dashboard_url" => "http://dash", "repo" => nil})
    text = card["blocks"] |> Enum.map(& &1["text"]) |> Enum.join("\n")

    assert text =~ "💡"
    assert text =~ "health_rules"
  end

  @tag regression: "F14"
  test "every alert type minted in lib has an explanation" do
    minted =
      Path.wildcard("lib/genswarms/observer/**/*.ex")
      |> Enum.flat_map(fn path ->
        source = File.read!(path)

        # \s* after the paren: the formatter breaks longer calls across lines
        # (alert(\n  :error_burst, ...) in detectors.ex) — a type minted that
        # way must not escape this scan.
        Regex.scan(~r/type:\s*:([a-zA-Z0-9_]+)/, source, capture: :all_but_first) ++
          Regex.scan(~r/alert\(\s*:([a-zA-Z0-9_]+)/, source, capture: :all_but_first) ++
          Regex.scan(~r/synthetic\(\s*:([a-zA-Z0-9_]+)/, source, capture: :all_but_first)
      end)
      |> List.flatten()
      |> Enum.map(&String.to_atom/1)
      |> MapSet.new()

    missing =
      minted
      |> Enum.reject(&Outbox.explain/1)
      |> Enum.sort()

    assert missing == []
  end
end
