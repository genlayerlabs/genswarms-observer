defmodule Genswarms.Observer.OpsDigestTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.OpsDigest

  # 2026-07-12 08:00:00 UTC
  @at_8am 1_783_843_200_000
  @today "2026-07-12"
  @yesterday "2026-07-11"

  defp envelope do
    %{
      "swarm" => "wingston",
      "extensions" => %{
        "audience" => %{
          "reachable_dm" => 1231,
          "push_eligible" => 1073,
          "blocked" => 111,
          "opted_out" => 0
        },
        "dashboard_pages" => [
          %{
            "id" => "growth",
            "label" => "Growth",
            "sections" => [
              %{"type" => "metrics", "title" => "Audience · now", "items" => []},
              %{
                "type" => "table",
                "title" => "Last 7 days",
                "rows" => [
                  %{"day" => @today, "replies" => 40, "blocked" => 1, "said-less" => 0},
                  %{"day" => @yesterday, "replies" => 162, "blocked" => 2, "said-less" => 1},
                  %{"day" => "2026-07-10", "replies" => 150, "blocked" => 0, "said-less" => 0}
                ]
              }
            ]
          },
          %{
            "id" => "proxy-router",
            "sections" => [
              %{
                "type" => "table",
                "title" => "History · last 30 days",
                "rows" => [
                  %{"day" => @yesterday, "spent" => "$2.39", "req" => 424}
                ]
              }
            ]
          }
        ]
      }
    }
  end

  defp config(overrides \\ %{}) do
    OpsDigest.build!(
      Map.merge(
        %{
          "hour_utc" => 7,
          "sections" => [
            %{"kind" => "block", "block" => "audience", "title" => "audience now"},
            %{
              "kind" => "page_row",
              "page" => "growth",
              "section" => "Last 7 days",
              "title" => "engagement",
              "columns" => ["replies", "blocked", "said-less"]
            },
            %{"kind" => "page_row", "page" => "proxy-router", "section" => "History", "title" => "llm"}
          ]
        },
        overrides
      )
    )
  end

  defp text({card, _day}), do: card["blocks"] |> Enum.map(& &1["text"]) |> Enum.join("\n")

  test "renders one card from blocks and yesterday's table rows, marked with today" do
    assert {card, @today} = result = OpsDigest.plan("wingston", envelope(), config(), nil, @at_8am)
    text = text(result)

    assert card["title"] =~ "wingston"
    assert card["title"] =~ @today

    # block section: configured extension scalars
    assert text =~ "audience now"
    assert text =~ "blocked 111"
    assert text =~ "reachable_dm 1231"

    # page_row latest_closed = yesterday's durable final, NOT today's partial
    assert text =~ "engagement (#{@yesterday})"
    assert text =~ "replies 162"
    refute text =~ "replies 40"

    # prefix section match + display-string values pass through
    assert text =~ "llm (#{@yesterday})"
    assert text =~ "$2.39"
  end

  test "explicit columns keep their order and drop absent ones" do
    cfg =
      config(%{
        "sections" => [
          %{
            "kind" => "page_row",
            "page" => "growth",
            "section" => "Last 7 days",
            "columns" => ["said-less", "replies", "not_a_column"]
          }
        ]
      })

    text = text(OpsDigest.plan("wingston", envelope(), cfg, nil, @at_8am))
    assert text =~ "said-less 1 · replies 162"
    refute text =~ "not_a_column"
  end

  test ~s(row "today" reads today's partial row instead) do
    cfg =
      config(%{
        "sections" => [
          %{"kind" => "page_row", "page" => "growth", "section" => "Last 7 days", "row" => "today"}
        ]
      })

    assert text(OpsDigest.plan("wingston", envelope(), cfg, nil, @at_8am)) =~ "replies 40"
  end

  test "gates: before hour_utc, already sent today, filtered swarm" do
    before_7am = @at_8am - 2 * 3_600_000
    assert :skip = OpsDigest.plan("wingston", envelope(), config(), nil, before_7am)
    assert :skip = OpsDigest.plan("wingston", envelope(), config(), @today, @at_8am)

    cfg = config(%{"swarms" => ["elsewhere"]})
    assert :skip = OpsDigest.plan("wingston", envelope(), cfg, nil, @at_8am)

    # yesterday's mark does NOT gate today
    assert {_, @today} = OpsDigest.plan("wingston", envelope(), config(), @yesterday, @at_8am)
  end

  test "no config, no resolvable section, malformed envelope → :skip, never a crash" do
    assert :skip = OpsDigest.plan("wingston", envelope(), nil, nil, @at_8am)

    cfg = config(%{"sections" => [%{"kind" => "block", "block" => "not_there"}]})
    assert :skip = OpsDigest.plan("wingston", envelope(), cfg, nil, @at_8am)

    for bad <- [%{}, %{"extensions" => "junk"}, %{"extensions" => %{"dashboard_pages" => "junk"}}] do
      assert :skip = OpsDigest.plan("wingston", bad, config(), nil, @at_8am)
    end
  end

  test "a section that resolves keeps the card even when siblings don't" do
    cfg =
      config(%{
        "sections" => [
          %{"kind" => "page_row", "page" => "gone", "section" => "x"},
          %{"kind" => "block", "block" => "audience"}
        ]
      })

    assert {card, @today} = OpsDigest.plan("wingston", envelope(), cfg, nil, @at_8am)
    assert length(card["blocks"]) == 1
  end

  test "envelope strings are sanitized before rendering (remote data)" do
    env =
      put_in(
        envelope(),
        ["extensions", "audience"],
        %{"note" => "visit https://evil.example now", "blocked" => 3}
      )

    cfg = config(%{"sections" => [%{"kind" => "block", "block" => "audience"}]})
    text = text(OpsDigest.plan("wingston", env, cfg, nil, @at_8am))
    refute text =~ "evil.example"
    assert text =~ "blocked 3"
  end

  test "numeric display strings survive the scrubber, arbitrary digit strings don't" do
    env =
      put_in(
        envelope(),
        ["extensions", "audience"],
        %{"spent" => "$8.123456", "cache" => "66%", "phone" => "+34 600123456"}
      )

    cfg = config(%{"sections" => [%{"kind" => "block", "block" => "audience"}]})
    text = text(OpsDigest.plan("wingston", env, cfg, nil, @at_8am))
    assert text =~ "spent $8.123456"
    assert text =~ "cache 66%"
    refute text =~ "600123456"
  end

  test "build! is fail-closed on malformed operator config" do
    assert OpsDigest.build!(nil) == nil

    for bad <- [
          "nope",
          %{"hour_utc" => 25, "sections" => [%{"kind" => "block", "block" => "a"}]},
          %{"sections" => []},
          %{"sections" => [%{"kind" => "nope"}]},
          %{"sections" => [%{"kind" => "block"}]},
          %{"sections" => [%{"kind" => "page_row", "page" => "g", "section" => "s", "row" => "eventually"}]},
          %{"sections" => [%{"kind" => "block", "block" => "a", "keys" => [1]}]},
          %{"swarms" => "wingston", "sections" => [%{"kind" => "block", "block" => "a"}]}
        ] do
      assert_raise ArgumentError, fn -> OpsDigest.build!(bad) end
    end

    # atom-keyed (Elixir swarm def) input normalizes like string-keyed
    cfg = OpsDigest.build!(%{hour_utc: 9, sections: [%{kind: "block", block: "audience"}]})
    assert cfg["hour_utc"] == 9
    assert [%{"kind" => "block", "block" => "audience"}] = cfg["sections"]
  end
end
