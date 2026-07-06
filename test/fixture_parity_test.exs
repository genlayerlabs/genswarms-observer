defmodule Genswarms.Observer.FixtureParityTest do
  @moduledoc """
  Cross-repo wire-dialect parity: the fixture under test/support was produced
  by wingston's REAL producer pipeline (Wingston.Topics.Schema.validate_llm_output
  -> Schema.period_payload -> the close_topic_period Jason round-trip ->
  Wingston.DashboardSource.topics_extension_block), NOT hand-written. If these
  assertions break, the producer and consumer have diverged on the wire dialect.

  Fixture contents (3 final periods, ascending):
    - 2026-07-02  status "ok", 6 sanitized topics + 2 signals (adversarial
      labels already scrubbed at the source; a k<2 topic and an invalid
      signal kind were dropped by the producer)
    - 2026-07-03  status "ok", empty (nothing happened that period)
    - 2026-07-04  status "error_redacted", zero topics, real counts
  """
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Digest
  alias Genswarms.Observer.Detectors.TopicsStale

  @fixture_path Path.expand("support/wingston_conversation_topics_fixture.json", __DIR__)

  defp fixture_extensions do
    @fixture_path |> File.read!() |> Jason.decode!()
  end

  defp fixture_envelope do
    %{"swarm" => "wingston", "extensions" => fixture_extensions()}
  end

  test "plan/3 renders cards for the producer's final periods; newly_seen is exactly the closed period ids, ascending" do
    {cards, newly_seen} = Digest.plan("wingston", fixture_envelope(), MapSet.new())

    assert newly_seen == ["2026-07-02", "2026-07-03", "2026-07-04"]

    # newest gets the full card, the two older ok periods coalesce -> 2 cards
    assert [full, coalesced] = cards
    assert full["title"] == "📊 digest: wingston · 2026-07-04"
    assert coalesced["title"] == "📊 digest: wingston · missed 2 periods"

    coalesced_texts = Enum.map(coalesced["blocks"], & &1["text"])
    assert "period range: 2026-07-02 to 2026-07-03" in coalesced_texts
    # summed counts across the coalesced periods: 11+0 conversations, 47+0 turns
    assert "counts: conversations 11, turns 47" in coalesced_texts
  end

  test "the error_redacted period (newest in the fixture) renders the summary-unavailable card" do
    {[full | _], _newly_seen} = Digest.plan("wingston", fixture_envelope(), MapSet.new())

    assert full["blocks"] == [
             %{
               "kind" => "paragraph",
               "text" => "summary unavailable for this period (aggregation failed upstream)"
             }
           ]
  end

  test "with the two older periods already seen, the ok period's topics render sanitized producer labels" do
    seen = MapSet.new(["2026-07-03", "2026-07-04"])
    {[full], ["2026-07-02"]} = Digest.plan("wingston", fixture_envelope(), seen)

    assert full["title"] == "📊 digest: wingston · 2026-07-02"
    texts = Enum.map(full["blocks"], & &1["text"])
    assert "coverage: dm — DM conversations only" in texts
    assert "counts: conversations 11, turns 47" in texts

    topics_text = Enum.find(texts, &String.contains?(&1, "rally onboarding confusion"))
    assert topics_text =~ "• rally onboarding confusion (4)"
    # producer already scrubbed the URL/@handle/phone/zero-width payloads
    assert topics_text =~ "• claim rewards at (3)"
    assert topics_text =~ "• contact about payouts (2)"
    assert topics_text =~ "• call for support (2)"
    assert topics_text =~ "• verify at now (2)"
    # markdown metachars survive the producer; the observer escapes at render
    assert topics_text =~ "• pricing \\*volume\\* \\[discounts\\] (3)"
    refute topics_text =~ "wallet linking errors"
    refute topics_text =~ "http"
    refute topics_text =~ "@wingston_admin"
    refute topics_text =~ "555"

    signals_text = Enum.find(texts, &String.starts_with?(&1, "signals:"))
    assert signals_text == "signals: frustration (3), churn\\_risk (2)"
  end

  test "decode_health/1 reads the producer envelope as :ok" do
    assert Digest.decode_health(fixture_envelope()) == :ok
  end

  test "TopicsStale accepts the producer envelope without alerting when periods are current" do
    fetched = %{dashboard: {:ok, fixture_envelope()}, events: {:ok, []}}

    # newest final period in the fixture is 2026-07-04; with the default
    # threshold of 1 period, "today" = 2026-07-05 gives cutoff 2026-07-04
    # -> newest == cutoff, not stale.
    now_ms = DateTime.to_unix(~U[2026-07-05 09:00:00Z], :millisecond)

    ctx = %{
      swarm: "wingston",
      thresholds: TopicsStale.default_thresholds(),
      state: TopicsStale.init(),
      now_ms: now_ms
    }

    assert {[], %{ever_seen: true}} = TopicsStale.detect(fetched, ctx)
  end

  test "JSON round-trip of the fixture has zero atom keys and no cids key anywhere" do
    decoded = fixture_extensions()
    round_tripped = decoded |> Jason.encode!() |> Jason.decode!()
    assert round_tripped == decoded

    assert_clean(round_tripped)
  end

  defp assert_clean(%{} = map) do
    Enum.each(map, fn {k, v} ->
      assert is_binary(k), "non-binary (atom?) key: #{inspect(k)}"
      refute k == "cids", "cids key leaked onto the wire"
      assert_clean(v)
    end)
  end

  defp assert_clean(list) when is_list(list), do: Enum.each(list, &assert_clean/1)

  defp assert_clean(other) do
    refute is_atom(other) and not is_boolean(other) and not is_nil(other),
           "atom value leaked: #{inspect(other)}"
  end
end
