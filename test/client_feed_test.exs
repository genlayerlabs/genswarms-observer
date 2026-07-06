defmodule Genswarms.Observer.ClientFeedTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Client

  # ── REAL wire envelopes (ground truth, F1) ────────────────────────────────
  #
  # `GET /api/swarms/:name/events/feed?since=N` is served by the vendored
  # dashboard backend — wingston vendor/genswarms-dashboard/backend/lib/
  # genswarms_dashboard/plug.ex:118-129:
  #
  #     %{events: events, seq: seq, source: "feed"}
  #     %{events: [], seq: 0, source: "unavailable"}
  #
  # The "unavailable" envelope answers when no EventsSource is configured, or
  # when the source returns :unavailable / raises / exits (backend README
  # §Events feed envelope) — it is a legitimate steady state, not an error.
  # Event maps inside "events" are host-opaque and relayed verbatim.

  describe "Http.parse_feed_envelope/1" do
    test "a feed answer maps to {:ok, %{events, seq}}" do
      envelope = %{
        "events" => [%{"kind" => "request_open", "cid" => "tg:1:0", "seq" => 7, "ts" => 1_751_734_800.0}],
        "seq" => 7,
        "source" => "feed"
      }

      assert {:ok, %{events: [%{"kind" => "request_open"}], seq: 7}} =
               Client.Http.parse_feed_envelope(envelope)
    end

    test ~s(source "unavailable" maps to :unavailable) do
      # exact envelope from plug.ex:127
      assert Client.Http.parse_feed_envelope(%{"events" => [], "seq" => 0, "source" => "unavailable"}) ==
               :unavailable
    end

    test "malformed envelopes map to a tagged error, never raise" do
      for bad <- [
            %{"events" => "nope", "seq" => 1, "source" => "feed"},
            %{"events" => [], "seq" => "one", "source" => "feed"},
            %{"events" => [], "seq" => -3, "source" => "feed"},
            %{"unexpected" => true},
            %{}
          ] do
        assert {:error, {:bad_feed_envelope, _}} = Client.Http.parse_feed_envelope(bad)
      end
    end
  end

  describe "Fake.get_events_feed/5" do
    test "answers the configured fixture and records the since cursor" do
      {:ok, fake} =
        Client.Fake.start_link(%{
          "w" => %{events_feed: {:ok, %{events: [%{"kind" => "typing"}], seq: 3}}}
        })

      assert {:ok, %{events: [%{"kind" => "typing"}], seq: 3}} =
               Client.Fake.get_events_feed("http://x", "w", 0, nil, fake: fake)

      assert [%{swarm: "w", kind: :events_feed, since: 0, token: nil}] = Client.Fake.calls(fake)
    end

    test "a fun fixture is applied to since (for cursor-threading tests)" do
      {:ok, fake} =
        Client.Fake.start_link(%{"w" => %{events_feed: fn since -> {:ok, %{events: [], seq: since + 5}} end}})

      assert {:ok, %{seq: 5}} = Client.Fake.get_events_feed("http://x", "w", 0, nil, fake: fake)
      assert {:ok, %{seq: 10}} = Client.Fake.get_events_feed("http://x", "w", 5, nil, fake: fake)
    end

    test "a configured swarm without :events_feed answers :unavailable (host has no EventsSource)" do
      {:ok, fake} = Client.Fake.start_link(%{"w" => %{dashboard: {:ok, %{}}}})

      assert Client.Fake.get_events_feed("http://x", "w", 0, nil, fake: fake) == :unavailable
    end

    test "an unknown swarm answers {:error, :not_configured} — a dead endpoint fails everything" do
      {:ok, fake} = Client.Fake.start_link(%{})

      assert Client.Fake.get_events_feed("http://x", "ghost", 0, nil, fake: fake) ==
               {:error, :not_configured}
    end
  end
end
