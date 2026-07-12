defmodule Genswarms.Observer.OpsDigest do
  @moduledoc """
  Pure planner for the once-daily ops digest card — the operator's morning
  glance at the numbers they'd otherwise open the dashboard for.

  Entirely config-driven (`ops_digest`, boot-only, validated fail-closed at
  `Genswarms.Observer.Objects.Scope.init/1`): the observer knows the two
  RENDERING conventions of the dashboard envelope, never any consumer's key
  names. Two section kinds:

  - `"block"` — scalar keys out of one extension block:
        %{"kind" => "block", "block" => "audience", "title" => "audience now",
          "keys" => ["reachable_dm", "blocked"]}       # keys optional = all scalars
  - `"page_row"` — one row of a `dashboard_pages` table section (the
    convention packages use for durable per-day history — yesterday's row
    is final by construction):
        %{"kind" => "page_row", "page" => "growth", "section" => "Last 7 days",
          "row" => "latest_closed", "title" => "engagement yesterday",
          "columns" => ["replies", "blocked"]}          # columns optional = all
    `section` is a PREFIX match against the section title (titles carry
    variable suffixes like "History · last 30 days"). `row` is
    `"latest_closed"` (newest row whose day key is before today UTC — the
    default) or `"today"`. `row_key` names the date column (default "day").

  `plan/5` takes the trusted swarm name, the envelope, the validated
  config, the last day already delivered for this swarm, and now_ms. It
  returns `:skip` (not due, already sent today, swarm filtered out, or no
  section resolved any data) or `{card, day}` — the caller owns delivery
  and marks `day` only after the card actually delivered (`Digest`'s
  seen-after-send invariant). Total and side-effect free: malformed
  envelope shapes drop sections, never raise.

  Every string that reaches the card came from the observed swarm's
  envelope and is passed through `Digest.sanitize_label/1` — same
  defense-in-depth as topic labels.
  """

  alias Genswarms.Observer.Digest

  @max_pairs_per_section 12

  @spec plan(String.t(), map, map | nil, String.t() | nil, integer) ::
          :skip | {map, String.t()}
  def plan(swarm, envelope, config, last_sent_day, now_ms)

  def plan(_swarm, _envelope, nil, _last_sent_day, _now_ms), do: :skip

  def plan(swarm, envelope, config, last_sent_day, now_ms)
      when is_map(envelope) and is_map(config) do
    today = now_ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_date()
    hour = now_ms |> DateTime.from_unix!(:millisecond) |> Map.fetch!(:hour)
    day = Date.to_iso8601(today)

    cond do
      not swarm_included?(swarm, config) -> :skip
      last_sent_day == day -> :skip
      hour < Map.fetch!(config, "hour_utc") -> :skip
      true -> build(swarm, envelope, config, today, day)
    end
  end

  def plan(_swarm, _envelope, _config, _last_sent_day, _now_ms), do: :skip

  defp build(swarm, envelope, config, today, day) do
    blocks =
      config
      |> Map.fetch!("sections")
      |> Enum.flat_map(&section_block(&1, envelope, today))

    case blocks do
      [] ->
        # Nothing resolved (block absent, page missing) — don't mark the
        # day: if the data shows up on a later tick today, the digest still
        # goes out. A swarm that never carries the configured blocks simply
        # never sends, at zero cost.
        :skip

      _ ->
        title = Map.get(config, "title", "daily ops")
        {%{"title" => "🌅 #{swarm} · #{title} · #{day}", "blocks" => blocks}, day}
    end
  end

  defp swarm_included?(swarm, config) do
    case Map.get(config, "swarms") do
      list when is_list(list) and list != [] -> swarm in list
      _ -> true
    end
  end

  # ── sections ──────────────────────────────────────────────────────────────

  defp section_block(%{"kind" => "block"} = section, envelope, _today) do
    with block when is_map(block) <- extension(envelope, Map.fetch!(section, "block")) do
      block
      |> pick_pairs(Map.get(section, "keys"))
      |> render_section(Map.get(section, "title", Map.fetch!(section, "block")))
    else
      _ -> []
    end
  end

  defp section_block(%{"kind" => "page_row"} = section, envelope, today) do
    row_key = Map.get(section, "row_key", "day")

    with rows when is_list(rows) <- table_rows(envelope, section),
         {date, row} <- select_row(rows, row_key, Map.get(section, "row", "latest_closed"), today) do
      base = Map.get(section, "title", Map.fetch!(section, "page"))

      row
      |> Map.delete(row_key)
      |> pick_pairs(Map.get(section, "columns"))
      # the date is Date.from_iso8601-validated in select_row — safe by
      # construction (and the PII scrubber would eat it as phone-shaped)
      |> render_section("#{base} (#{Date.to_iso8601(date)})")
    else
      _ -> []
    end
  end

  defp section_block(_section, _envelope, _today), do: []

  defp extension(envelope, key) do
    case Map.get(envelope, "extensions") do
      exts when is_map(exts) -> Map.get(exts, key)
      _ -> nil
    end
  end

  defp table_rows(envelope, section) do
    prefix = Map.fetch!(section, "section")

    with pages when is_list(pages) <- extension(envelope, "dashboard_pages"),
         page when is_map(page) <- Enum.find(pages, &(is_map(&1) and &1["id"] == section["page"])),
         sections when is_list(sections) <- Map.get(page, "sections"),
         table when is_map(table) <-
           Enum.find(sections, fn s ->
             is_map(s) and s["type"] == "table" and is_binary(s["title"]) and
               String.starts_with?(s["title"], prefix)
           end) do
      Map.get(table, "rows")
    else
      _ -> nil
    end
  end

  defp select_row(rows, row_key, which, today) do
    dated =
      rows
      |> Enum.filter(&is_map/1)
      |> Enum.flat_map(fn row ->
        with day when is_binary(day) <- Map.get(row, row_key),
             {:ok, date} <- Date.from_iso8601(day) do
          [{date, row}]
        else
          _ -> []
        end
      end)

    case which do
      "today" ->
        Enum.find(dated, fn {date, _row} -> date == today end)

      _latest_closed ->
        dated
        |> Enum.filter(fn {date, _row} -> Date.compare(date, today) == :lt end)
        |> Enum.max_by(fn {date, _row} -> Date.to_iso8601(date) end, fn -> nil end)
    end
  end

  # ── rendering ─────────────────────────────────────────────────────────────

  # keys/columns nil = every scalar in the map, alphabetical; an explicit
  # list keeps ITS order and includes only keys actually present.
  defp pick_pairs(map, nil) do
    map
    |> Enum.filter(fn {k, v} -> is_binary(k) and scalar?(v) end)
    |> Enum.sort()
  end

  defp pick_pairs(map, keys) when is_list(keys) do
    Enum.flat_map(keys, fn k ->
      case Map.get(map, k) do
        v when v != nil -> if scalar?(v), do: [{k, v}], else: []
        nil -> []
      end
    end)
  end

  defp pick_pairs(_map, _keys), do: []

  defp scalar?(v), do: is_number(v) or is_binary(v) or is_boolean(v)

  defp render_section([], _title), do: []

  # The title is operator config (plus a Date-validated suffix) — trusted,
  # rendered raw. Keys and values come from the envelope — sanitized.
  defp render_section(pairs, title) do
    line =
      pairs
      |> Enum.take(@max_pairs_per_section)
      |> Enum.map_join(" · ", fn {k, v} -> "#{safe(k)} #{safe_value(v)}" end)

    [%{"kind" => "paragraph", "text" => "#{title}: #{line}"}]
  end

  defp safe(s) when is_binary(s), do: Digest.sanitize_label(s)
  defp safe(other), do: other |> to_string() |> Digest.sanitize_label()

  # Numbers render as-is. Envelope strings are remote data → sanitized —
  # EXCEPT pure numeric display values ("$8.123456", "66%", "2.387737"):
  # they carry no PII surface by construction, and the scrubber's
  # phone/digit-run patterns would eat exactly the money the digest exists
  # to show.
  @numericish ~r/^[$€£]?\d[\d,]*(\.\d+)?%?$/
  defp safe_value(v) when is_number(v), do: to_string(v)
  defp safe_value(v) when is_boolean(v), do: to_string(v)

  defp safe_value(v) when is_binary(v) do
    if byte_size(v) <= 24 and Regex.match?(@numericish, v),
      do: v,
      else: Digest.sanitize_label(v)
  end

  # ── config validation (fail-closed, called from Scope.init/1) ────────────

  @doc """
  Validates and normalizes the boot-time `ops_digest` config. `nil`/absent
  → `nil` (feature off). Anything present must be well-formed — operator
  config is trusted-but-verified at boot exactly like `signal_rules`: a
  malformed entry RAISES with its reason, it is never silently dropped.
  Accepts atom or string keys; returns a string-keyed map.
  """
  @spec build!(term) :: map | nil
  def build!(nil), do: nil

  def build!(config) when is_map(config) do
    hour = key(config, :hour_utc, 7)

    unless is_integer(hour) and hour in 0..23 do
      raise ArgumentError, "ops_digest.hour_utc must be an integer 0..23, got #{inspect(hour)}"
    end

    sections = key(config, :sections, nil)

    unless is_list(sections) and sections != [] do
      raise ArgumentError, "ops_digest.sections must be a non-empty list"
    end

    swarms = key(config, :swarms, nil)

    unless is_nil(swarms) or (is_list(swarms) and Enum.all?(swarms, &is_binary/1)) do
      raise ArgumentError, "ops_digest.swarms must be a list of swarm names"
    end

    title = key(config, :title, "daily ops")

    unless is_binary(title) do
      raise ArgumentError, "ops_digest.title must be a string"
    end

    %{
      "hour_utc" => hour,
      "swarms" => swarms,
      "title" => title,
      "sections" => Enum.map(sections, &build_section!/1)
    }
  end

  def build!(other) do
    raise ArgumentError, "ops_digest must be a map, got #{inspect(other)}"
  end

  defp build_section!(section) when is_map(section) do
    case key(section, :kind, nil) do
      "block" ->
        %{
          "kind" => "block",
          "block" => required_string!(section, :block),
          "title" => optional_string!(section, :title),
          "keys" => optional_string_list!(section, :keys)
        }
        |> reject_nils()

      "page_row" ->
        row = key(section, :row, "latest_closed")

        unless row in ["latest_closed", "today"] do
          raise ArgumentError,
                ~s(ops_digest page_row "row" must be "latest_closed" or "today", got #{inspect(row)})
        end

        %{
          "kind" => "page_row",
          "page" => required_string!(section, :page),
          "section" => required_string!(section, :section),
          "row" => row,
          "row_key" => key(section, :row_key, "day"),
          "title" => optional_string!(section, :title),
          "columns" => optional_string_list!(section, :columns)
        }
        |> reject_nils()

      other ->
        raise ArgumentError,
              ~s(ops_digest section kind must be "block" or "page_row", got #{inspect(other)})
    end
  end

  defp build_section!(other) do
    raise ArgumentError, "ops_digest sections must be maps, got #{inspect(other)}"
  end

  defp required_string!(section, k) do
    case key(section, k, nil) do
      s when is_binary(s) and s != "" -> s
      other -> raise ArgumentError, "ops_digest section #{k} must be a string, got #{inspect(other)}"
    end
  end

  defp optional_string!(section, k) do
    case key(section, k, nil) do
      nil -> nil
      s when is_binary(s) -> s
      other -> raise ArgumentError, "ops_digest section #{k} must be a string, got #{inspect(other)}"
    end
  end

  defp optional_string_list!(section, k) do
    case key(section, k, nil) do
      nil ->
        nil

      list when is_list(list) ->
        unless Enum.all?(list, &is_binary/1) do
          raise ArgumentError, "ops_digest section #{k} must be a list of strings"
        end

        list

      other ->
        raise ArgumentError, "ops_digest section #{k} must be a list of strings, got #{inspect(other)}"
    end
  end

  defp reject_nils(map), do: map |> Enum.reject(fn {_k, v} -> is_nil(v) end) |> Map.new()

  defp key(map, k, default), do: Map.get(map, k, Map.get(map, to_string(k), default))
end
