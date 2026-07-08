defmodule Genswarms.Observer.Signals do
  @moduledoc """
  Pure evaluator for the v1 declarative `health_rules` grammar (structured
  JSON conds, not a string DSL). Packages ship rules inside their dashboard
  extension blocks; operators may add their own via `Objects.Scope`'s
  `signal_rules` config. This module knows nothing about either source —
  Scope (Task 6) decides trust (fail-soft vs fail-closed) and threads
  samples/persistence; this module is just grammar + evaluation.

  Grammar (see the plan's Global Constraints for the authoritative copy):

      rule     = %{"id" => <=32 [a-z0-9_], "severity" => "warn"|"info" (default "warn"),
                   "card" => <=200 chars, "each" => optional path, "where" => optional cond,
                   "when" => required cond}
      cond     = %{"op" => "gt"|"gte"|"lt"|"lte"|"eq"|"neq", "lhs" => operand, "rhs" => operand}
      operand  = number | "now" | %{"lit" => any} | %{"path" => "dot.path"}
               | %{"delta" => "dot.path"} | %{"add"|"sub"|"mul"|"div" => [operand, operand]}

  Bounds: <=16 rules/block, <=32 cond/operand nodes per rule (recursive,
  `when` + `where` combined). Absence-tolerant throughout: a missing path, a
  non-numeric arithmetic operand, div-by-zero, or a first-sight `delta` all
  make the rule no-op silently — never raise, never false-alarm. `eq`/`neq`
  compare any terms; ordered ops (`gt`/`gte`/`lt`/`lte`) require both sides
  numeric. Numeric strings are NEVER coerced — a block publishing a numeric
  value as a string is a producer bug, treated as absence.

  `"../"` is a host-trust escape: a `path`/`delta` string starting with
  `"../"` resolves the remainder against the WHOLE `extensions` map instead
  of the item/block — only host-published blocks (wingston is the envelope
  owner) use it.
  """

  @absent :__signals_absent__

  @type cond_t :: %{String.t() => term}
  @type rule :: %{String.t() => term}

  # ── validate_rules/1 ────────────────────────────────────────────────────

  @spec validate_rules(term) :: {:ok, [rule]} | {:error, String.t()}
  def validate_rules(rules) when is_list(rules) do
    if length(rules) > 16 do
      {:error, "too many rules (#{length(rules)} > 16, max 16 per block)"}
    else
      rules
      |> Enum.with_index()
      |> Enum.reduce_while({:ok, []}, fn {rule, idx}, {:ok, acc} ->
        case validate_rule(rule, idx) do
          {:ok, validated} -> {:cont, {:ok, [validated | acc]}}
          {:error, _} = err -> {:halt, err}
        end
      end)
      |> case do
        {:ok, acc} -> {:ok, Enum.reverse(acc)}
        {:error, _} = err -> err
      end
    end
  end

  def validate_rules(other), do: {:error, "health_rules must be a list, got #{inspect(other)}"}

  defp validate_rule(rule, idx) when is_map(rule) do
    with {:ok, id} <- validate_id(rule, idx),
         {:ok, severity} <- validate_severity(rule, id, idx),
         {:ok, card} <- validate_card(rule, id, idx),
         {:ok, each} <- validate_each(rule, id, idx),
         {:ok, where_cond} <- validate_cond(rule["where"], id, idx, "where", :optional),
         {:ok, when_cond} <- validate_cond(rule["when"], id, idx, "when", :required),
         :ok <- validate_node_count(where_cond, when_cond, id, idx) do
      {:ok,
       rule
       |> Map.put("id", id)
       |> Map.put("severity", severity)
       |> Map.put("card", card)
       |> Map.put("each", each)
       |> Map.put("where", where_cond)
       |> Map.put("when", when_cond)}
    end
  end

  defp validate_rule(other, idx),
    do: {:error, "rule at index #{idx} is not a map (#{inspect(other)})"}

  defp validate_id(rule, idx) do
    id = rule["id"]

    if is_binary(id) and Regex.match?(~r/^[a-z0-9_]{1,32}$/, id) do
      {:ok, id}
    else
      {:error,
       "rule at index #{idx} has invalid id #{inspect(id)} (must match ^[a-z0-9_]{1,32}$)"}
    end
  end

  defp validate_severity(rule, id, idx) do
    case rule["severity"] do
      nil -> {:ok, "warn"}
      s when s in ["warn", "info"] -> {:ok, s}
      other -> {:error, "rule #{id} (index #{idx}) has invalid severity #{inspect(other)}"}
    end
  end

  defp validate_card(rule, id, idx) do
    case rule["card"] do
      c when is_binary(c) and byte_size(c) <= 200 ->
        {:ok, c}

      other ->
        {:error,
         "rule #{id} (index #{idx}) has invalid/missing card #{inspect(other)} (string <=200 chars required)"}
    end
  end

  defp validate_each(rule, id, idx) do
    case rule["each"] do
      nil ->
        {:ok, nil}

      p when is_binary(p) ->
        {:ok, p}

      other ->
        {:error,
         "rule #{id} (index #{idx}) has invalid each #{inspect(other)} (must be a path string)"}
    end
  end

  defp validate_cond(nil, id, idx, field, :required),
    do: {:error, "rule #{id} (index #{idx}) is missing required #{field}"}

  defp validate_cond(nil, _id, _idx, _field, :optional), do: {:ok, nil}

  defp validate_cond(cond, id, idx, field, _req) when is_map(cond) do
    op = cond["op"]

    if op in ["gt", "gte", "lt", "lte", "eq", "neq"] do
      with {:ok, _} <- validate_operand(cond["lhs"], id, idx, "#{field}.lhs"),
           {:ok, _} <- validate_operand(cond["rhs"], id, idx, "#{field}.rhs") do
        {:ok, cond}
      end
    else
      {:error, "rule #{id} (index #{idx}) #{field} has unknown op #{inspect(op)}"}
    end
  end

  defp validate_cond(other, id, idx, field, _req),
    do: {:error, "rule #{id} (index #{idx}) #{field} is not a valid cond (#{inspect(other)})"}

  defp validate_operand(op, _id, _idx, _at) when is_number(op), do: {:ok, op}
  defp validate_operand("now", _id, _idx, _at), do: {:ok, "now"}
  defp validate_operand(%{"lit" => _} = m, _id, _idx, _at) when map_size(m) == 1, do: {:ok, m}

  defp validate_operand(%{"path" => p} = m, id, idx, at) when map_size(m) == 1 do
    if is_binary(p),
      do: {:ok, m},
      else: {:error, "rule #{id} (index #{idx}) #{at} has invalid path #{inspect(p)}"}
  end

  defp validate_operand(%{"delta" => p} = m, id, idx, at) when map_size(m) == 1 do
    if is_binary(p),
      do: {:ok, m},
      else: {:error, "rule #{id} (index #{idx}) #{at} has invalid delta #{inspect(p)}"}
  end

  defp validate_operand(%{"add" => args} = m, id, idx, at) when map_size(m) == 1,
    do: validate_arith(args, id, idx, at)

  defp validate_operand(%{"sub" => args} = m, id, idx, at) when map_size(m) == 1,
    do: validate_arith(args, id, idx, at)

  defp validate_operand(%{"mul" => args} = m, id, idx, at) when map_size(m) == 1,
    do: validate_arith(args, id, idx, at)

  defp validate_operand(%{"div" => args} = m, id, idx, at) when map_size(m) == 1,
    do: validate_arith(args, id, idx, at)

  defp validate_operand(other, id, idx, at),
    do: {:error, "rule #{id} (index #{idx}) #{at} has invalid operand #{inspect(other)}"}

  defp validate_arith([a, b], id, idx, at) do
    with {:ok, _} <- validate_operand(a, id, idx, "#{at}[0]"),
         {:ok, _} <- validate_operand(b, id, idx, "#{at}[1]") do
      {:ok, %{}}
    end
  end

  defp validate_arith(other, id, idx, at),
    do:
      {:error,
       "rule #{id} (index #{idx}) #{at} arithmetic operand must be a 2-element list (#{inspect(other)})"}

  defp validate_node_count(where_cond, when_cond, id, idx) do
    total = count_cond_nodes(where_cond) + count_cond_nodes(when_cond)

    if total <= 32,
      do: :ok,
      else: {:error, "rule #{id} (index #{idx}) exceeds max node count (#{total} > 32)"}
  end

  defp count_cond_nodes(nil), do: 0

  defp count_cond_nodes(cond) when is_map(cond),
    do: 1 + count_operand_nodes(cond["lhs"]) + count_operand_nodes(cond["rhs"])

  defp count_operand_nodes(%{"add" => args}), do: 1 + arith_node_count(args)
  defp count_operand_nodes(%{"sub" => args}), do: 1 + arith_node_count(args)
  defp count_operand_nodes(%{"mul" => args}), do: 1 + arith_node_count(args)
  defp count_operand_nodes(%{"div" => args}), do: 1 + arith_node_count(args)
  defp count_operand_nodes(_leaf), do: 1

  defp arith_node_count([a, b]), do: count_operand_nodes(a) + count_operand_nodes(b)
  defp arith_node_count(_other), do: 0

  # ── evaluate/7 ──────────────────────────────────────────────────────────

  @spec evaluate(String.t(), map, [rule], map, map, String.t(), integer) :: {[map], map}
  def evaluate(block_key, block, rules, extensions, samples, swarm, now_ms)
      when is_list(rules) and is_map(block) do
    Enum.reduce(rules, {[], %{prev: samples, next: samples}}, fn rule, {alerts_acc, samples_acc} ->
      {alerts, new_samples} =
        safe_eval_rule(rule, block_key, block, extensions, samples_acc, swarm, now_ms)

      {alerts_acc ++ alerts, new_samples}
    end)
    |> then(fn {alerts, sample_state} -> {alerts, sample_state.next} end)
  end

  def evaluate(_block_key, _block, _rules, _extensions, samples, _swarm, _now_ms),
    do: {[], samples}

  # Total against garbage that slipped validation: any raise inside a single
  # rule's evaluation drops just that rule's alerts, keeping prior samples.
  defp safe_eval_rule(rule, block_key, block, extensions, samples, swarm, now_ms) do
    eval_rule(rule, block_key, block, extensions, samples, swarm, now_ms)
  rescue
    _ -> {[], samples}
  end

  defp eval_rule(rule, block_key, block, extensions, samples, swarm, now_ms) do
    case rule["each"] do
      nil ->
        eval_item(rule, block, block, extensions, samples, block_key, swarm, now_ms)

      path when is_binary(path) ->
        case resolve_dotpath(block, path) do
          {:ok, list} when is_list(list) ->
            Enum.reduce(list, {[], samples}, fn item, {alerts_acc, samples_acc} ->
              {alerts, new_samples} =
                eval_item(rule, item, block, extensions, samples_acc, block_key, swarm, now_ms)

              {alerts_acc ++ alerts, new_samples}
            end)

          _not_a_list_or_missing ->
            {[], samples}
        end

      _invalid_each ->
        {[], samples}
    end
  end

  defp eval_item(rule, item, block, extensions, samples, block_key, swarm, now_ms) do
    id = rule["id"]

    {where_ok, samples1} =
      case rule["where"] do
        nil ->
          {true, samples}

        where_cond ->
          eval_cond(where_cond, item, block, extensions, samples, block_key, id, now_ms)
      end

    if where_ok do
      {fired, samples2} =
        eval_cond(rule["when"], item, block, extensions, samples1, block_key, id, now_ms)

      if fired,
        do: {[build_alert(rule, item, block, block_key, swarm, now_ms)], samples2},
        else: {[], samples2}
    else
      {[], samples1}
    end
  end

  defp build_alert(rule, item, block, block_key, swarm, now_ms) do
    %{
      type: :health_rule,
      swarm: swarm,
      at_ms: now_ms,
      summary: interpolate(rule["card"], item, block) |> String.slice(0, 200),
      evidence: %{
        "block" => block_key,
        "rule_id" => rule["id"],
        "severity" => rule["severity"] || "warn"
      },
      key: {swarm, :health_rule, block_key, rule["id"], item_key(item)},
      cids: []
    }
  end

  defp item_key(item) when is_map(item), do: item["name"] || item["id"] || nil
  defp item_key(_), do: nil

  defp interpolate(card, item, block) when is_binary(card) do
    Regex.replace(~r/\{(\w+)\}/, card, fn _whole, field ->
      value =
        (is_map(item) && Map.get(item, field)) ||
          (is_map(block) && Map.get(block, field))

      case value do
        nil -> "?"
        false -> "false"
        v -> safe_to_string(v)
      end
    end)
  end

  defp interpolate(_card, _item, _block), do: "?"

  defp safe_to_string(v) do
    to_string(v)
  rescue
    _ -> "?"
  end

  # ── cond / operand evaluation ───────────────────────────────────────────

  defp eval_cond(cond, item, block, extensions, samples, block_key, rule_id, now_ms)
       when is_map(cond) do
    {lhs, samples1} =
      eval_operand(cond["lhs"], item, block, extensions, samples, block_key, rule_id, now_ms)

    {rhs, samples2} =
      eval_operand(cond["rhs"], item, block, extensions, samples1, block_key, rule_id, now_ms)

    {compare(cond["op"], lhs, rhs), samples2}
  end

  defp eval_cond(_not_a_map, _item, _block, _extensions, samples, _block_key, _rule_id, _now_ms),
    do: {false, samples}

  defp compare(op, lhs, rhs) when op in ["gt", "gte", "lt", "lte"] do
    if is_number(lhs) and is_number(rhs) do
      case op do
        "gt" -> lhs > rhs
        "gte" -> lhs >= rhs
        "lt" -> lhs < rhs
        "lte" -> lhs <= rhs
      end
    else
      false
    end
  end

  defp compare("eq", lhs, rhs), do: lhs != @absent and rhs != @absent and lhs == rhs
  defp compare("neq", lhs, rhs), do: lhs != @absent and rhs != @absent and lhs != rhs
  defp compare(_unknown_op, _lhs, _rhs), do: false

  defp eval_operand(op, _item, _block, _extensions, samples, _bk, _rid, _now) when is_number(op),
    do: {op, samples}

  defp eval_operand("now", _item, _block, _extensions, samples, _bk, _rid, now_ms),
    do: {now_ms, samples}

  defp eval_operand(%{"lit" => v}, _item, _block, _extensions, samples, _bk, _rid, _now),
    do: {v, samples}

  defp eval_operand(%{"path" => p}, item, _block, extensions, samples, _bk, _rid, _now)
       when is_binary(p) do
    case resolve_rooted(p, item, extensions) do
      {:ok, v} -> {v, samples}
      :error -> {@absent, samples}
    end
  end

  defp eval_operand(%{"delta" => p}, _item, block, extensions, samples, block_key, rule_id, _now)
       when is_binary(p) do
    eval_delta(p, block, extensions, samples, block_key, rule_id)
  end

  defp eval_operand(%{"add" => [a, b]}, item, block, extensions, samples, bk, rid, now),
    do: eval_arith(:add, a, b, item, block, extensions, samples, bk, rid, now)

  defp eval_operand(%{"sub" => [a, b]}, item, block, extensions, samples, bk, rid, now),
    do: eval_arith(:sub, a, b, item, block, extensions, samples, bk, rid, now)

  defp eval_operand(%{"mul" => [a, b]}, item, block, extensions, samples, bk, rid, now),
    do: eval_arith(:mul, a, b, item, block, extensions, samples, bk, rid, now)

  defp eval_operand(%{"div" => [a, b]}, item, block, extensions, samples, bk, rid, now),
    do: eval_arith(:div, a, b, item, block, extensions, samples, bk, rid, now)

  defp eval_operand(_unrecognized, _item, _block, _extensions, samples, _bk, _rid, _now),
    do: {@absent, samples}

  defp eval_arith(op, a, b, item, block, extensions, samples, bk, rid, now) do
    {va, samples1} = eval_operand(a, item, block, extensions, samples, bk, rid, now)
    {vb, samples2} = eval_operand(b, item, block, extensions, samples1, bk, rid, now)

    if is_number(va) and is_number(vb) do
      case op do
        :add -> {va + vb, samples2}
        :sub -> {va - vb, samples2}
        :mul -> {va * vb, samples2}
        :div -> if vb == 0, do: {@absent, samples2}, else: {va / vb, samples2}
      end
    else
      {@absent, samples2}
    end
  end

  # delta is always block-relative (or extensions-relative via "../") — never
  # item-relative, even inside an `each`. It reads only the pre-tick snapshot
  # and records the current numeric reading into the next snapshot.
  defp eval_delta(path, block, extensions, %{prev: prev, next: next} = samples, block_key, rule_id) do
    case resolve_rooted(path, block, extensions) do
      {:ok, cur} when is_number(cur) ->
        sample_key = {block_key, rule_id, path}
        new_samples = %{samples | next: Map.put(next, sample_key, cur)}

        case Map.fetch(prev, sample_key) do
          {:ok, prev} when is_number(prev) -> {cur - prev, new_samples}
          _ -> {@absent, new_samples}
        end

      _not_numeric_or_missing ->
        {@absent, samples}
    end
  end

  # A leading "../" escapes the item/block root to the whole extensions map
  # (host-trust escape — package rules never span blocks, only the host's
  # own rules do).
  defp resolve_rooted(path, base, extensions) do
    case path do
      "../" <> rest -> resolve_dotpath(extensions, rest)
      _ -> resolve_dotpath(base, path)
    end
  end

  defp resolve_dotpath(root, path) do
    path
    |> String.split(".")
    |> Enum.reduce_while({:ok, root}, fn seg, {:ok, cur} ->
      if is_map(cur) and Map.has_key?(cur, seg) do
        {:cont, {:ok, Map.get(cur, seg)}}
      else
        {:halt, :error}
      end
    end)
  end
end
