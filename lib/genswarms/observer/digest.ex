defmodule Genswarms.Observer.Digest do
  @moduledoc """
  Pure planner for the `conversation_topics` dashboard extension digest.

  `plan/2` takes a fetched dashboard envelope and the set of period ids
  already delivered for that swarm, and decides what (if anything) to send
  this tick. It is total and side-effect free: no HTTP, no clock, no store
  access, no crashing — any unexpected shape simply yields no cards and no
  newly-seen ids. `Genswarms.Observer.Objects.Scope` owns delivery and the
  seen-after-send invariant (marking a period seen only once its card has
  actually been delivered).

  Extension shape (normative, string keys), read from
  `envelope["extensions"]["conversation_topics"]`:

      %{"v" => 1, "coverage" => "dm" | "group" | "all", "periods" => [
        %{
          "period_id" => "YYYY-MM-DD",
          "final" => bool,
          "status" => "ok" | "error_redacted",
          "generated_at" => iso8601,
          "source_watermark" => term,
          "topics" => [%{"label" => str, "count" => int}],
          "counts" => %{"conversations" => int, "turns" => int},
          "signals" => [%{"kind" => str, "count" => int}]
        }
      ]}

  Any `v` other than the literal integer `1` (missing extension, wrong
  version, wrong shape) yields `{[], []}`. Non-final periods never render
  and never enter `newly_seen`.

  Of the unseen final periods (ascending by `period_id`), the newest gets
  a full card; anything older is coalesced into a single second card — at
  most 2 cards per call, by construction (the brief's per-tick digest
  cap). `newly_seen` covers every period considered this call (both the
  one rendered in full and the ones folded into the coalesced card): a
  bounded backlog is always fully cleared in one tick.

  `sanitize_label/1` is an independent implementation of the same
  scrubbing rules wingston applies at the source (NFC normalize, strip
  bidi/control/zero-width/format chars, remove URL/email/@handle
  fragments and 6+-digit runs, collapse whitespace, cap at 80) — defense
  in depth against a compromised or buggy upstream. It also escapes
  Telegram MarkdownV2 metacharacters, since digest cards may render with
  `parse_mode` set.
  """

  @period_re ~r/^\d{4}-\d{2}-\d{2}$/
  @label_cap 80

  # Order matters (lesson from the wingston-side review): strip
  # control/bidi/zero-width/format chars BEFORE scrubbing structured PII.
  # A zero-width char or bidi override spliced into a URL or phone number
  # defeats the PII regex on a single scrub-first pass, then reassembles
  # into a live URL/phone once the charset filter finally runs — so
  # charset stripping must happen first, and the strip+scrub pass runs a
  # SECOND time in case any residue from the first pass reassembles into
  # a fresh match.
  @strip_chars ~r/[\x00-\x1F\x7F\x{00AD}\x{200B}-\x{200F}\x{2028}\x{2029}\x{202A}-\x{202E}\x{2060}\x{2066}-\x{2069}\x{FEFF}]/u

  @pii_patterns [
    ~r/https?:\/\/\S+/iu,
    ~r/\S+@\S+\.\S+/u,
    ~r/@\w{3,}/u,
    ~r/\+?\d[\d\s().-]{6,}\d/u,
    ~r/\d{6,}/u
  ]

  @markdown_escape_chars [
    "_",
    "*",
    "[",
    "]",
    "(",
    ")",
    "~",
    "`",
    ">",
    "#",
    "+",
    "-",
    "=",
    "|",
    "{",
    "}",
    ".",
    "!"
  ]

  @doc """
  Pure. `envelope` is the dashboard payload for one swarm; `seen` is the
  `MapSet` of period ids already delivered for that swarm. Returns
  `{cards, newly_seen}` — `cards` is what to send this tick (already in
  the same `%{"title" => ..., "blocks" => [%{"kind" => "paragraph", "text"
  => ...}]}` shape `emit_alert/3` uses), `newly_seen` is the list of
  period ids to fold into the store ONLY after every card for this swarm
  has delivered successfully.

  Total: any malformed/absent extension, wrong `v`, or garbage argument
  (including a non-map `envelope` or a `seen` that isn't a `MapSet`)
  yields `{[], []}` rather than raising.
  """
  @spec plan(map, MapSet.t()) :: {[map], [String.t()]}
  def plan(envelope, seen) when is_map(envelope) do
    seen = normalize_seen(seen)
    swarm = swarm_of(envelope)

    case get_extension(envelope) do
      %{"v" => 1, "periods" => periods} = ext when is_list(periods) ->
        coverage = Map.get(ext, "coverage")

        unseen =
          periods
          |> Enum.filter(&valid_final_period?/1)
          |> Enum.reject(&MapSet.member?(seen, &1["period_id"]))
          |> Enum.uniq_by(& &1["period_id"])
          |> Enum.sort_by(& &1["period_id"])

        build_cards(unseen, coverage, swarm)

      _ ->
        {[], []}
    end
  end

  def plan(_envelope, _seen), do: {[], []}

  # ── extraction / validation ────────────────────────────────────────────────

  defp get_extension(envelope) do
    case Map.get(envelope, "extensions") do
      m when is_map(m) -> Map.get(m, "conversation_topics")
      _ -> nil
    end
  end

  defp swarm_of(envelope) do
    case Map.get(envelope, "swarm") do
      s when is_binary(s) and s != "" -> s
      _ -> "unknown"
    end
  end

  defp normalize_seen(%MapSet{} = s), do: s
  defp normalize_seen(_), do: MapSet.new()

  defp valid_final_period?(%{"final" => true, "period_id" => id}) when is_binary(id) do
    Regex.match?(@period_re, id) and match?({:ok, _}, Date.from_iso8601(id))
  end

  defp valid_final_period?(_), do: false

  # ── card construction ────────────────────────────────────────────────────

  defp build_cards([], _coverage, _swarm), do: {[], []}

  defp build_cards(unseen, coverage, swarm) do
    {older, [newest]} = Enum.split(unseen, -1)

    newly_seen = Enum.map(unseen, & &1["period_id"])
    full = full_card(swarm, coverage, newest)

    cards =
      case older do
        [] -> [full]
        _ -> [full, coalesced_card(swarm, older)]
      end

    {cards, newly_seen}
  end

  defp full_card(swarm, coverage, period) do
    title = "📊 digest: #{swarm} · #{period["period_id"]}"

    blocks =
      if period["status"] == "error_redacted" do
        [
          %{
            "kind" => "paragraph",
            "text" => "summary unavailable for this period (aggregation failed upstream)"
          }
        ]
      else
        full_blocks(coverage, period)
      end

    %{"title" => title, "blocks" => blocks}
  end

  defp full_blocks(coverage, period) do
    [coverage_block(coverage), counts_block(period["counts"])] ++
      topics_block(period["topics"]) ++
      signals_block(period["signals"])
  end

  defp coverage_block(coverage) do
    text =
      case coverage do
        "dm" -> "coverage: dm — DM conversations only"
        "group" -> "coverage: group — group conversations only"
        "all" -> "coverage: all — DM and group conversations"
        other when is_binary(other) -> "coverage: #{other}"
        _ -> "coverage: unknown"
      end

    %{"kind" => "paragraph", "text" => text}
  end

  defp counts_block(counts) do
    %{
      "kind" => "paragraph",
      "text" =>
        "counts: conversations #{count_of(counts, "conversations")}, turns #{count_of(counts, "turns")}"
    }
  end

  defp topics_block(topics) when is_list(topics) do
    case topics |> Enum.map(&topic_line/1) |> Enum.reject(&is_nil/1) do
      [] -> []
      lines -> [%{"kind" => "paragraph", "text" => Enum.join(lines, "\n")}]
    end
  end

  defp topics_block(_), do: []

  defp topic_line(%{"label" => label, "count" => count}) when is_binary(label) do
    case sanitize_label(label) do
      "" -> nil
      safe -> "• #{safe} (#{safe_int(count)})"
    end
  end

  defp topic_line(_), do: nil

  defp signals_block(signals) when is_list(signals) do
    case signals |> Enum.map(&signal_part/1) |> Enum.reject(&is_nil/1) do
      [] -> []
      parts -> [%{"kind" => "paragraph", "text" => "signals: " <> Enum.join(parts, ", ")}]
    end
  end

  defp signals_block(_), do: []

  defp signal_part(%{"kind" => kind, "count" => count}) when is_binary(kind) do
    case sanitize_label(kind) do
      "" -> nil
      safe -> "#{safe} (#{safe_int(count)})"
    end
  end

  defp signal_part(_), do: nil

  defp coalesced_card(swarm, older) do
    ids = Enum.map(older, & &1["period_id"])
    oldest = List.first(ids)
    newest = List.last(ids)
    n = length(older)

    range_text =
      if oldest == newest,
        do: "period: #{oldest}",
        else: "period range: #{oldest} to #{newest}"

    total_conversations = sum_counts(older, "conversations")
    total_turns = sum_counts(older, "turns")

    %{
      "title" => "📊 digest: #{swarm} · missed #{n} periods",
      "blocks" => [
        %{"kind" => "paragraph", "text" => range_text},
        %{
          "kind" => "paragraph",
          "text" => "counts: conversations #{total_conversations}, turns #{total_turns}"
        },
        %{"kind" => "paragraph", "text" => "details in each period's row on the dashboard"}
      ]
    }
  end

  defp sum_counts(periods, key) do
    Enum.reduce(periods, 0, fn p, acc -> acc + count_of(p["counts"], key) end)
  end

  defp count_of(counts, key) when is_map(counts), do: safe_int(Map.get(counts, key))
  defp count_of(_, _), do: 0

  defp safe_int(n) when is_integer(n), do: n
  defp safe_int(_), do: 0

  # ── label sanitization ──────────────────────────────────────────────────

  @doc """
  Sanitizes a single label (topic or signal kind) for rendering into a
  digest card: NFC-normalize, strip bidi/control/zero-width/format chars,
  scrub structured PII (URLs, emails, @handles, phone-shaped and bare 6+
  digit runs), repeat the strip+scrub pass a second time (belt and braces
  against reassembly), collapse whitespace, trim, cap at #{@label_cap}
  chars, then escape Telegram MarkdownV2 metacharacters. Non-binary input
  sanitizes to `""`.
  """
  @spec sanitize_label(term) :: String.t()
  def sanitize_label(s) when is_binary(s) do
    s
    |> String.normalize(:nfc)
    |> strip_invisibles()
    |> scrub_pii()
    |> strip_invisibles()
    |> scrub_pii()
    |> String.replace(~r/\s+/u, " ")
    |> String.trim()
    |> String.slice(0, @label_cap)
    |> escape_markdown()
  end

  def sanitize_label(_), do: ""

  defp strip_invisibles(s), do: String.replace(s, @strip_chars, "")

  defp scrub_pii(s), do: Enum.reduce(@pii_patterns, s, &Regex.replace(&1, &2, ""))

  defp escape_markdown(s) do
    Enum.reduce(@markdown_escape_chars, s, fn char, acc ->
      String.replace(acc, char, "\\" <> char)
    end)
  end
end
