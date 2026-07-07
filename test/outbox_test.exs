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
end
