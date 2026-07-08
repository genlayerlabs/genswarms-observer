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

        Regex.scan(~r/type:\s*:([a-zA-Z0-9_]+)/, source, capture: :all_but_first) ++
          Regex.scan(~r/alert\(:([a-zA-Z0-9_]+)/, source, capture: :all_but_first) ++
          Regex.scan(~r/synthetic\(:([a-zA-Z0-9_]+)/, source, capture: :all_but_first)
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
