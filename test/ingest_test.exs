defmodule Genswarms.Observer.IngestTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Client
  alias Genswarms.Observer.Ingest

  @entry %{"dashboard_url" => "http://dash.example:4994", "token_env" => nil, "repo" => nil}

  # Client.Fake threads `since` into the `events_feed` fixture (a 1-arity fun
  # of `since`, see client/fake.ex) — the existing drain tests in
  # scope_test.exs script multi-page feeds this way. Reused verbatim here
  # rather than inventing a `feed_pages:`/`feed:` vocabulary.
  defp fake_with(events_feed_fixture) do
    {:ok, fake} =
      Client.Fake.start_link(%{
        "wingston" => %{
          dashboard: {:ok, %{"swarm" => "wingston"}},
          events: {:ok, []},
          events_feed: events_feed_fixture
        }
      })

    fake
  end

  test "first read (nil cursor) drains all pages to head" do
    fake =
      fake_with(fn
        0 -> {:ok, %{events: [%{"kind" => "request_open", "cid" => "a", "seq" => 1, "ts" => 1.0}], seq: 1}}
        1 -> {:ok, %{events: [%{"kind" => "reply_sent", "cid" => "a", "ok" => true, "seq" => 2, "ts" => 2.0}], seq: 2}}
        2 -> {:ok, %{events: [], seq: 2}}
      end)

    {data, proposed} = Ingest.fetch(Client.Fake, [fake: fake], "wingston", @entry, nil, 10)

    assert {:ok, events} = data.feed
    assert length(events) == 2
    assert proposed == 2
  end

  @tag regression: "F5"
  test "steady state (integer cursor) ALSO drains past a single page boundary" do
    # Backlog of 2 pages while the observer had cursor 10: a single-page read
    # would split an open/reply pair across ticks (F5). The drain must union
    # both pages in ONE fetch.
    fake =
      fake_with(fn
        10 -> {:ok, %{events: [%{"kind" => "request_open", "cid" => "x", "seq" => 11, "ts" => 5.0}], seq: 11}}
        11 -> {:ok, %{events: [%{"kind" => "reply_sent", "cid" => "x", "ok" => true, "seq" => 12, "ts" => 6.0}], seq: 12}}
        12 -> {:ok, %{events: [], seq: 12}}
      end)

    {data, proposed} = Ingest.fetch(Client.Fake, [fake: fake], "wingston", @entry, 10, 10)

    assert {:ok, events} = data.feed
    assert Enum.map(events, & &1["seq"]) == [11, 12]
    assert proposed == 12
  end

  test ~s(entry "name" overrides the swarm name on the remote wire — the registry key stays the local identity) do
    # Two deployments of the same swarm (local + prod) need distinct registry
    # keys, but both remotes serve /api/swarms/wingston/…. Client.Fake keys
    # fixtures by the name the real client puts in the URL path, so resolving
    # the "wingston" fixture under key "wingston-prod" proves the wire saw the
    # entry's name — without it, every call answers {:error, :not_configured}.
    fake = fake_with(fn 0 -> {:ok, %{events: [], seq: 0}} end)

    entry = Map.put(@entry, "name", "wingston")

    {data, _proposed} = Ingest.fetch(Client.Fake, [fake: fake], "wingston-prod", entry, nil, 10)

    assert {:ok, %{"swarm" => "wingston"}} = data.dashboard
    assert {:ok, []} = data.events
    assert {:ok, []} = data.feed
  end

  test "feed error leaves the proposal nil and reports the error" do
    fake = fake_with({:error, :boom})

    {data, proposed} = Ingest.fetch(Client.Fake, [fake: fake], "wingston", @entry, 7, 10)
    assert {:error, :boom} = data.feed
    assert proposed == nil
  end

  test "mid-drain failure discards the partial union (no partial windows)" do
    fake =
      fake_with(fn
        0 -> {:ok, %{events: [%{"kind" => "request_open", "cid" => "y", "seq" => 1, "ts" => 1.0}], seq: 1}}
        _ -> {:error, :flake}
      end)

    {data, proposed} = Ingest.fetch(Client.Fake, [fake: fake], "wingston", @entry, nil, 10)
    assert {:error, :flake} = data.feed
    assert proposed == nil
  end
end
