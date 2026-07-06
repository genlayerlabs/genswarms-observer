defmodule Genswarms.Observer.DigestTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Digest

  defp period(id, opts \\ []) do
    %{
      "period_id" => id,
      "final" => Keyword.get(opts, :final, true),
      "status" => Keyword.get(opts, :status, "ok"),
      "generated_at" => Keyword.get(opts, :generated_at, "#{id}T00:00:00Z"),
      "source_watermark" => Keyword.get(opts, :source_watermark, 1),
      "topics" => Keyword.get(opts, :topics, []),
      "counts" => Keyword.get(opts, :counts, %{"conversations" => 1, "turns" => 2}),
      "signals" => Keyword.get(opts, :signals, [])
    }
  end

  defp envelope(periods, opts \\ []) do
    %{
      "swarm" => Keyword.get(opts, :swarm, "wingston"),
      "extensions" => %{
        "conversation_topics" => %{
          "v" => Keyword.get(opts, :v, 1),
          "coverage" => Keyword.get(opts, :coverage, "dm"),
          "periods" => periods
        }
      }
    }
  end

  defp texts(card), do: Enum.map(card["blocks"], & &1["text"])

  # ── shape / version gates ────────────────────────────────────────────────

  test "unknown v is ignored entirely" do
    assert Digest.plan(envelope([period("2026-07-01")], v: 2), MapSet.new()) == {[], []}
    assert Digest.plan(envelope([period("2026-07-01")], v: "1"), MapSet.new()) == {[], []}
    assert Digest.plan(envelope([period("2026-07-01")], v: nil), MapSet.new()) == {[], []}
  end

  test "missing extension yields no cards" do
    assert Digest.plan(%{"swarm" => "wingston"}, MapSet.new()) == {[], []}
    assert Digest.plan(%{"swarm" => "wingston", "extensions" => %{}}, MapSet.new()) == {[], []}
  end

  test "periods not a list yields no cards" do
    env = %{
      "swarm" => "wingston",
      "extensions" => %{
        "conversation_topics" => %{"v" => 1, "coverage" => "dm", "periods" => "nope"}
      }
    }

    assert Digest.plan(env, MapSet.new()) == {[], []}
  end

  test "non-final periods never render and never enter newly_seen" do
    env = envelope([period("2026-07-01", final: false), period("2026-07-02", final: false)])
    assert Digest.plan(env, MapSet.new()) == {[], []}
  end

  test "garbage top-level input never crashes" do
    garbage = [
      nil,
      "a string",
      42,
      [1, 2, 3],
      %{},
      %{"extensions" => "nope"},
      %{"extensions" => nil},
      %{"extensions" => %{"conversation_topics" => "nope"}},
      %{"extensions" => %{"conversation_topics" => %{"v" => 1, "periods" => nil}}},
      %{
        "extensions" => %{"conversation_topics" => %{"v" => 1, "periods" => [1, "two", %{}, nil]}}
      },
      %{
        "extensions" => %{"conversation_topics" => %{"v" => 1, "periods" => [%{"final" => true}]}}
      }
    ]

    for env <- garbage do
      assert Digest.plan(env, MapSet.new()) == {[], []}
      assert Digest.plan(env, :not_a_mapset) == {[], []}
    end
  end

  # ── selection: ascending, newest-full + coalesce ─────────────────────────

  test "ascending selection: newest of the unseen final periods is picked for the full card" do
    # deliberately out of order in the source list
    env =
      envelope([
        period("2026-07-03"),
        period("2026-07-01"),
        period("2026-07-02")
      ])

    {[full, _coalesced], newly_seen} = Digest.plan(env, MapSet.new())
    assert full["title"] =~ "2026-07-03"
    assert Enum.sort(newly_seen) == ["2026-07-01", "2026-07-02", "2026-07-03"]
  end

  test "single unseen final period yields exactly one full card" do
    env = envelope([period("2026-07-01")])
    {cards, newly_seen} = Digest.plan(env, MapSet.new())
    assert length(cards) == 1
    assert newly_seen == ["2026-07-01"]
  end

  test "two unseen final periods: newest full + one coalesced card for the rest" do
    env = envelope([period("2026-07-01"), period("2026-07-02")])
    {cards, newly_seen} = Digest.plan(env, MapSet.new())

    assert length(cards) == 2
    [full, coalesced] = cards
    assert full["title"] =~ "2026-07-02"
    assert coalesced["title"] =~ "missed 1 periods"
    assert Enum.sort(newly_seen) == ["2026-07-01", "2026-07-02"]
  end

  test "9 unseen final periods yields exactly 2 cards, all 9 marked newly_seen" do
    periods = for d <- 1..9, do: period("2026-07-0#{d}")
    env = envelope(periods)

    {cards, newly_seen} = Digest.plan(env, MapSet.new())
    assert length(cards) == 2

    [full, coalesced] = cards
    assert full["title"] =~ "2026-07-09"
    assert coalesced["title"] =~ "missed 8 periods"
    assert length(newly_seen) == 9
  end

  test "already-seen periods are excluded from selection and from newly_seen" do
    env = envelope([period("2026-07-01"), period("2026-07-02"), period("2026-07-03")])
    seen = MapSet.new(["2026-07-01"])

    {cards, newly_seen} = Digest.plan(env, seen)
    assert length(cards) == 2
    assert Enum.sort(newly_seen) == ["2026-07-02", "2026-07-03"]
  end

  test "all periods already seen yields no cards" do
    env = envelope([period("2026-07-01"), period("2026-07-02")])
    seen = MapSet.new(["2026-07-01", "2026-07-02"])
    assert Digest.plan(env, seen) == {[], []}
  end

  test "coalesced card sums counts across the older periods" do
    env =
      envelope([
        period("2026-07-01", counts: %{"conversations" => 3, "turns" => 10}),
        period("2026-07-02", counts: %{"conversations" => 5, "turns" => 1}),
        period("2026-07-03", counts: %{"conversations" => 1, "turns" => 1})
      ])

    {[_full, coalesced], _newly_seen} = Digest.plan(env, MapSet.new())
    assert texts(coalesced) |> Enum.any?(&(&1 =~ "conversations 8, turns 11"))
    assert texts(coalesced) |> Enum.any?(&(&1 =~ "period range: 2026-07-01 to 2026-07-02"))
  end

  # ── error_redacted wording ────────────────────────────────────────────────

  test "error_redacted status renders a single unavailable-summary block" do
    env = envelope([period("2026-07-01", status: "error_redacted")])
    {[full], _} = Digest.plan(env, MapSet.new())

    assert [%{"kind" => "paragraph", "text" => text}] = full["blocks"]
    assert text =~ "summary unavailable for this period"
    assert text =~ "aggregation failed upstream"
  end

  # ── coverage wording ──────────────────────────────────────────────────────

  test "coverage line renders per-coverage wording" do
    for {coverage, expect} <- [
          {"dm", "coverage: dm — DM conversations only"},
          {"group", "coverage: group — group conversations only"},
          {"all", "coverage: all — DM and group conversations"}
        ] do
      env = envelope([period("2026-07-01")], coverage: coverage)
      {[full], _} = Digest.plan(env, MapSet.new())
      assert texts(full) |> Enum.any?(&(&1 == expect))
    end
  end

  test "unknown coverage falls back without crashing" do
    env = envelope([period("2026-07-01")], coverage: nil)
    {[full], _} = Digest.plan(env, MapSet.new())
    assert texts(full) |> Enum.any?(&(&1 == "coverage: unknown"))
  end

  # ── topics / signals rendering ───────────────────────────────────────────

  test "topics render as bullet lines with label and count" do
    env =
      envelope([
        period("2026-07-01",
          topics: [
            %{"label" => "billing questions", "count" => 4},
            %{"label" => "onboarding", "count" => 2}
          ]
        )
      ])

    {[full], _} = Digest.plan(env, MapSet.new())
    topics_text = texts(full) |> Enum.find(&(&1 =~ "billing"))
    assert topics_text =~ "• billing questions (4)"
    assert topics_text =~ "• onboarding (2)"
  end

  test "signals render when present, omitted when absent" do
    env = envelope([period("2026-07-01", signals: [%{"kind" => "frustration", "count" => 3}])])
    {[full], _} = Digest.plan(env, MapSet.new())
    assert texts(full) |> Enum.any?(&(&1 == "signals: frustration (3)"))

    env2 = envelope([period("2026-07-02", signals: [])])
    {[full2], _} = Digest.plan(env2, MapSet.new())
    refute texts(full2) |> Enum.any?(&String.starts_with?(&1, "signals:"))
  end

  test "a topic whose label sanitizes to empty is dropped, not rendered blank" do
    env =
      envelope([period("2026-07-01", topics: [%{"label" => "6001112223334445", "count" => 9}])])

    {[full], _} = Digest.plan(env, MapSet.new())
    refute texts(full) |> Enum.any?(&(&1 =~ "•"))
  end

  test "malformed topic/signal entries are skipped, not fatal" do
    env =
      envelope([
        period("2026-07-01",
          topics: [%{"label" => "ok one", "count" => 1}, %{"nope" => true}, "garbage", nil],
          signals: [%{"kind" => "confusion", "count" => 1}, %{"bad" => 1}, 42]
        )
      ])

    {[full], _} = Digest.plan(env, MapSet.new())
    assert texts(full) |> Enum.any?(&(&1 =~ "• ok one (1)"))
    assert texts(full) |> Enum.any?(&(&1 == "signals: confusion (1)"))
  end

  # ── adversarial labels ────────────────────────────────────────────────────

  describe "sanitize_label/1" do
    test "strips PII: phone, email, url, bare digit runs" do
      assert Digest.sanitize_label("call me at +34 600 111 222 or a@b.com") =~ ~r/^\S/
      refute Digest.sanitize_label("call +34 600 111 222") =~ ~r/\d{3}/
      refute Digest.sanitize_label("mail a@b.com ya") =~ "@"
      refute Digest.sanitize_label("go to https://evil.example now") =~ "http"
      refute Digest.sanitize_label("code 123456 done") =~ ~r/\d{6,}/
    end

    test "bidi override + zero-width chars are stripped" do
      nasty = "ok" <> <<0x202E::utf8>> <> "gnihsihp" <> <<0x200B::utf8>>
      out = Digest.sanitize_label(nasty)
      refute out =~ <<0x202E::utf8>>
      refute out =~ <<0x200B::utf8>>
    end

    test "zero-width space split inside a url scheme scrubs clean (defeats scrub-first ordering)" do
      out = Digest.sanitize_label("topic htt" <> <<0x200B::utf8>> <> "ps://evil.example/x here")
      refute out =~ "http"
      refute out =~ "evil.example"
    end

    test "bidi override split inside a url scheme scrubs clean" do
      out = Digest.sanitize_label("topic ht" <> <<0x202E::utf8>> <> "tps://evil.example here")
      refute out =~ "evil.example"
    end

    test "zero-width space split inside a bare digit run scrubs clean" do
      out = Digest.sanitize_label("call 1234" <> <<0x200B::utf8>> <> "5678 today")
      refute out =~ ~r/\d{6,}/
    end

    test "caps at 80 chars" do
      assert String.length(Digest.sanitize_label(String.duplicate("a", 200))) == 80
    end

    test "nested unicode homoglyph label survives without crashing" do
      nasty =
        "Салес" <>
          <<0x202E::utf8>> <> "ⲥⲧ" <> <<0x200B::utf8>> <> "phishing"

      result = Digest.sanitize_label(nasty)
      assert is_binary(result)
      assert String.length(result) <= 80
    end

    test "escapes Telegram MarkdownV2 metacharacters" do
      out = Digest.sanitize_label("hello_world *bold* [link](url) ~tilde~ `code` #tag")
      assert out =~ "\\_"
      assert out =~ "\\*"
      assert out =~ "\\["
      assert out =~ "\\]"
      assert out =~ "\\("
      assert out =~ "\\)"
      assert out =~ "\\~"
      assert out =~ "\\`"
      assert out =~ "\\#"
    end

    test "non-binary input sanitizes to empty string without crashing" do
      assert Digest.sanitize_label(nil) == ""
      assert Digest.sanitize_label(42) == ""
      assert Digest.sanitize_label(%{}) == ""
    end

    test "property: never exceeds 80 chars or leaks PII across a nasty-input corpus" do
      zwsp = <<0x200B::utf8>>

      corpus = [
        "phishing" <> <<0x202E::utf8>> <> "reversed" <> zwsp <> "text",
        "topic ht" <> <<0x202E::utf8>> <> "tps://evil.example here",
        String.duplicate("x", 500),
        "call +34" <> <<0x2060::utf8>> <> "600111222 today",
        "mixed @handle and https://evil.example and 1234567890",
        String.duplicate(zwsp, 50) <> "clean text",
        ""
      ]

      for input <- corpus do
        out = Digest.sanitize_label(input)
        assert String.length(out) <= 80
        refute out =~ ~r/\d{6,}/
        refute out =~ "http"
      end
    end
  end
end
