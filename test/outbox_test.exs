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
end
