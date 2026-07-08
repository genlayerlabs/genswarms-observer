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
  defp card_text(card), do: Enum.join([card["title"] | texts(card)], "\n")

  # plan/3 takes the trusted swarm name explicitly — tests default it to
  # "wingston" to match envelope/2's default "swarm" field, except where a
  # test deliberately mismatches them to prove the envelope field is inert.
  defp plan(envelope, seen, swarm \\ "wingston"), do: Digest.plan(swarm, envelope, seen)

  # ── shape / version gates ────────────────────────────────────────────────

  test "unknown v is ignored entirely" do
    assert plan(envelope([period("2026-07-01")], v: 2), MapSet.new()) == {[], []}
    assert plan(envelope([period("2026-07-01")], v: "1"), MapSet.new()) == {[], []}
    assert plan(envelope([period("2026-07-01")], v: nil), MapSet.new()) == {[], []}
  end

  test "missing extension yields no cards" do
    assert plan(%{"swarm" => "wingston"}, MapSet.new()) == {[], []}
    assert plan(%{"swarm" => "wingston", "extensions" => %{}}, MapSet.new()) == {[], []}
  end

  test "periods not a list yields no cards" do
    env = %{
      "swarm" => "wingston",
      "extensions" => %{
        "conversation_topics" => %{"v" => 1, "coverage" => "dm", "periods" => "nope"}
      }
    }

    assert plan(env, MapSet.new()) == {[], []}
  end

  test "non-final periods never render and never enter newly_seen" do
    env = envelope([period("2026-07-01", final: false), period("2026-07-02", final: false)])
    assert plan(env, MapSet.new()) == {[], []}
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
      assert plan(env, MapSet.new()) == {[], []}
      assert plan(env, :not_a_mapset) == {[], []}
    end
  end

  # ── trusted swarm name (untrusted envelope["swarm"] must not leak in) ────

  test "card title uses the trusted swarm arg, never the untrusted envelope field" do
    evil_swarm = "evil" <> <<0x202E::utf8>> <> "name*[link](x)"
    env = envelope([period("2026-07-01")], swarm: evil_swarm)
    {[full], _} = Digest.plan("wingston", env, MapSet.new())

    assert full["title"] =~ "digest: wingston · 2026-07-01"
    refute full["title"] =~ "evil"
    refute full["title"] =~ <<0x202E::utf8>>
  end

  test "coalesced card title also uses the trusted swarm arg" do
    evil_swarm = "evil" <> <<0x202E::utf8>> <> "name*[link](x)"

    env =
      envelope(
        [period("2026-07-01"), period("2026-07-02")],
        swarm: evil_swarm
      )

    {[_full, coalesced], _newly_seen} = Digest.plan("wingston", env, MapSet.new())

    assert coalesced["title"] =~ "digest: wingston · missed 1 periods"
    refute coalesced["title"] =~ "evil"
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

    {[full, _coalesced], newly_seen} = plan(env, MapSet.new())
    assert full["title"] =~ "2026-07-03"
    assert Enum.sort(newly_seen) == ["2026-07-01", "2026-07-02", "2026-07-03"]
  end

  test "single unseen final period yields exactly one full card" do
    env = envelope([period("2026-07-01")])
    {cards, newly_seen} = plan(env, MapSet.new())
    assert length(cards) == 1
    assert newly_seen == ["2026-07-01"]
  end

  test "two unseen final periods: newest full + one coalesced card for the rest" do
    env = envelope([period("2026-07-01"), period("2026-07-02")])
    {cards, newly_seen} = plan(env, MapSet.new())

    assert length(cards) == 2
    [full, coalesced] = cards
    assert full["title"] =~ "2026-07-02"
    assert coalesced["title"] =~ "missed 1 periods"
    assert Enum.sort(newly_seen) == ["2026-07-01", "2026-07-02"]
  end

  test "9 unseen final periods yields exactly 2 cards, all 9 marked newly_seen" do
    periods = for d <- 1..9, do: period("2026-07-0#{d}")
    env = envelope(periods)

    {cards, newly_seen} = plan(env, MapSet.new())
    assert length(cards) == 2

    [full, coalesced] = cards
    assert full["title"] =~ "2026-07-09"
    assert coalesced["title"] =~ "missed 8 periods"
    assert length(newly_seen) == 9
  end

  test "already-seen periods are excluded from selection and from newly_seen" do
    env = envelope([period("2026-07-01"), period("2026-07-02"), period("2026-07-03")])
    seen = MapSet.new(["2026-07-01"])

    {cards, newly_seen} = plan(env, seen)
    assert length(cards) == 2
    assert Enum.sort(newly_seen) == ["2026-07-02", "2026-07-03"]
  end

  test "all periods already seen yields no cards" do
    env = envelope([period("2026-07-01"), period("2026-07-02")])
    seen = MapSet.new(["2026-07-01", "2026-07-02"])
    assert plan(env, seen) == {[], []}
  end

  test "coalesced card sums counts across the older periods" do
    env =
      envelope([
        period("2026-07-01", counts: %{"conversations" => 3, "turns" => 10}),
        period("2026-07-02", counts: %{"conversations" => 5, "turns" => 1}),
        period("2026-07-03", counts: %{"conversations" => 1, "turns" => 1})
      ])

    {[_full, coalesced], _newly_seen} = plan(env, MapSet.new())
    assert texts(coalesced) |> Enum.any?(&(&1 =~ "conversations 8, turns 11"))
    assert texts(coalesced) |> Enum.any?(&(&1 =~ "period range: 2026-07-01 to 2026-07-02"))
  end

  # ── error_redacted wording ────────────────────────────────────────────────

  test "error_redacted status renders a single unavailable-summary block" do
    env = envelope([period("2026-07-01", status: "error_redacted")])
    {[full], _} = plan(env, MapSet.new())

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
      {[full], _} = plan(env, MapSet.new())
      assert texts(full) |> Enum.any?(&(&1 == expect))
    end
  end

  test "unknown coverage falls back without crashing" do
    env = envelope([period("2026-07-01")], coverage: nil)
    {[full], _} = plan(env, MapSet.new())
    assert texts(full) |> Enum.any?(&(&1 == "coverage: unknown"))
  end

  test "an untrusted string coverage value is sanitized before rendering" do
    nasty = "x" <> <<0x202E::utf8>> <> "y*_[inj]"
    env = envelope([period("2026-07-01")], coverage: nasty)
    {[full], _} = plan(env, MapSet.new())

    line = texts(full) |> Enum.find(&String.starts_with?(&1, "coverage:"))
    refute line =~ <<0x202E::utf8>>
    # markup chars are plain text (sender renders HTML and escapes itself)
    assert line == "coverage: xy*_[inj]"
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

    {[full], _} = plan(env, MapSet.new())
    topics_text = texts(full) |> Enum.find(&(&1 =~ "billing"))
    assert topics_text =~ "• billing questions (4)"
    assert topics_text =~ "• onboarding (2)"
  end

  test "signals render when present, omitted when absent" do
    env = envelope([period("2026-07-01", signals: [%{"kind" => "frustration", "count" => 3}])])
    {[full], _} = plan(env, MapSet.new())
    assert texts(full) |> Enum.any?(&(&1 == "signals: frustration (3)"))

    env2 = envelope([period("2026-07-02", signals: [])])
    {[full2], _} = plan(env2, MapSet.new())
    refute texts(full2) |> Enum.any?(&String.starts_with?(&1, "signals:"))
  end

  test "a topic whose label sanitizes to empty is dropped, not rendered blank" do
    env =
      envelope([period("2026-07-01", topics: [%{"label" => "6001112223334445", "count" => 9}])])

    {[full], _} = plan(env, MapSet.new())
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

    {[full], _} = plan(env, MapSet.new())
    assert texts(full) |> Enum.any?(&(&1 =~ "• ok one (1)"))
    assert texts(full) |> Enum.any?(&(&1 == "signals: confusion (1)"))
  end

  @tag regression: "F12"
  test "topics are entry-capped with a remainder line and the card stays deliverable" do
    topics = for i <- 1..300, do: %{"label" => "topic #{i}", "count" => i}
    env = envelope([period("2026-07-01", topics: topics)])

    {[full], _} = plan(env, MapSet.new())
    topics_text = texts(full) |> Enum.find(&(&1 =~ "• topic"))

    assert topics_text |> String.split("\n") |> length() == 13
    assert topics_text =~ "… +288 more"
    assert String.length(card_text(full)) < 3500
  end

  @tag regression: "F12"
  test "adversarially giant rendered text is truncated at the final card guard" do
    huge_count = String.to_integer(String.duplicate("9", 400))
    topics = for i <- 1..12, do: %{"label" => "topic #{i}", "count" => huge_count}
    signals = for i <- 1..12, do: %{"kind" => "signal #{i}", "count" => huge_count}
    env = envelope([period("2026-07-01", topics: topics, signals: signals)])

    {[full], _} = plan(env, MapSet.new())

    assert String.length(card_text(full)) <= 3500
    assert card_text(full) =~ "… [truncated]"
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

    test "markup metacharacters pass through unescaped — cards render as HTML at the sender" do
      # The telegram sender's card renderer emits HTML and does its own
      # `& < > "` escaping; MarkdownV2 backslash-escaping here would show
      # up as literal visible backslashes in every digest card. Security
      # scrubs (PII, invisibles) still apply — but markup chars are just
      # text.
      label = "hello_world *bold* [link] ~tilde~ `code` #tag"
      assert Digest.sanitize_label(label) == label
      refute Digest.sanitize_label(label) =~ "\\"
    end

    test "an input backslash is preserved verbatim, never doubled" do
      # Input is the 3 chars `\*x` — with no MarkdownV2 escaping applied,
      # the label reaches the card exactly as written.
      assert Digest.sanitize_label("\\*x") == "\\*x"
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

  # ── decode_health/1 (O7) ─────────────────────────────────────────────────

  describe "decode_health/1" do
    test "ok for a well-formed extension (regardless of pending/empty periods)" do
      assert Digest.decode_health(envelope([period("2026-07-01")])) == :ok
      assert Digest.decode_health(envelope([])) == :ok
      assert Digest.decode_health(envelope([period("2026-07-01", final: false)])) == :ok
      # an unknown future version still DECODES — version gating is plan/3's job
      assert Digest.decode_health(envelope([period("2026-07-01")], v: 2)) == :ok
    end

    test "absent when the extension key is not there at all" do
      assert Digest.decode_health(%{"swarm" => "wingston"}) == :absent
      assert Digest.decode_health(%{"swarm" => "wingston", "extensions" => %{}}) == :absent
      assert Digest.decode_health(%{}) == :absent
    end

    test "malformed when the key is present but the block is broken" do
      # extension not a map
      assert Digest.decode_health(%{"extensions" => %{"conversation_topics" => "nope"}}) ==
               :malformed

      assert Digest.decode_health(%{"extensions" => %{"conversation_topics" => nil}}) ==
               :malformed

      assert Digest.decode_health(%{"extensions" => %{"conversation_topics" => [1, 2]}}) ==
               :malformed

      # map but no periods list
      assert Digest.decode_health(%{"extensions" => %{"conversation_topics" => %{"v" => 1}}}) ==
               :malformed

      # periods not a list
      assert Digest.decode_health(%{
               "extensions" => %{"conversation_topics" => %{"v" => 1, "periods" => "nope"}}
             }) == :malformed

      # periods a list of non-maps
      assert Digest.decode_health(%{
               "extensions" => %{"conversation_topics" => %{"v" => 1, "periods" => [1, "two"]}}
             }) == :malformed

      # extensions itself not a map, or the envelope not a map — broken, never a crash
      assert Digest.decode_health(%{"extensions" => "nope"}) == :malformed
      assert Digest.decode_health(nil) == :malformed
      assert Digest.decode_health("garbage") == :malformed
    end
  end
end
