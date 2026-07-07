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

  test "alert_card's dashboard link uses the entry's remote name; title and MCP hints keep the registry key" do
    alert = %{swarm: "wingston-prod", type: :endpoint_down, summary: "s", evidence: %{}}
    entry = %{"dashboard_url" => "http://remote.example", "repo" => nil, "name" => "wingston"}

    card = Outbox.alert_card(alert, entry)

    # The link must resolve on the remote host (which knows the swarm as
    # "wingston"); the human-facing identity stays the registry key.
    assert card["title"] =~ "wingston-prod"
    assert Enum.any?(card["blocks"], fn b ->
             b["text"] =~ "http://remote.example/api/swarms/wingston/dashboard"
           end)

    refute Enum.any?(card["blocks"], fn b -> b["text"] =~ "/api/swarms/wingston-prod/" end)
  end
  test "cards explain what the alert MEANS in plain language, evidence stays readable" do
    alert = %{
      swarm: "wingston",
      type: :endpoint_down,
      summary: "dashboard fetch failed: {:failed_connect, ...}",
      evidence: %{"reason" => String.duplicate("x", 400)}
    }

    entry = %{"dashboard_url" => "http://a.example", "repo" => nil}
    card = Outbox.alert_card(alert, entry)
    texts = Enum.map(card["blocks"], & &1["text"])

    assert Enum.any?(texts, &(&1 =~ "💡" and &1 =~ "deploy/restart"))
    # the technical term stays visible but bounded — never a wall of Erlang
    evidence = Enum.find(texts, &String.starts_with?(&1, "evidence:"))
    assert evidence =~ "reason: xxx"
    assert String.length(evidence) < 400
    assert Enum.any?(texts, &(&1 =~ "cannot answer here"))
  end

  test "an unknown alert type carries no explainer block (no made-up guidance)" do
    alert = %{swarm: "s", type: :something_custom, summary: "s", evidence: %{}}
    card = Outbox.alert_card(alert, %{"dashboard_url" => "http://a", "repo" => nil})
    refute Enum.any?(card["blocks"], &(&1["text"] =~ "💡"))
  end
end
