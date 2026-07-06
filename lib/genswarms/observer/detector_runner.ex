defmodule Genswarms.Observer.DetectorRunner do
  @moduledoc """
  Runs a list of `Genswarms.Observer.Detector` modules for one swarm tick,
  in isolation from each other and from the caller:

  - A crash or timeout in one detector never takes down the tick, never
    corrupts its state, and never stops the remaining detectors from
    running. It contributes a `detector_crashed` alert instead of its own.
  - A well-formed-but-invalid return (bad alert shape) drops the offending
    alerts, does NOT commit the new state, and contributes a
    `detector_invalid` alert.
  - State only ever commits on a valid, successful return.
  - Every alert is normalized before being handed back: `key` defaults to
    `{swarm, type}`, `cids` defaults to `[]`, and `source` is stamped with
    the detector module that produced it — this is the provenance tag the
    caller (scope.ex) uses to decide builtin-vs-custom trust; it is set by
    the runner itself and is never taken from the detector's own return
    value, so a detector cannot forge it. Evidence that Jason can't encode
    (pids/tuples inside an otherwise-valid map) is replaced with a bounded
    inspect, so the caller's `Jason.encode!` on it can never crash a tick.

  `states` and the returned states map are keyed by detector module.
  """

  alias Genswarms.Observer.Detector

  @default_timeout_ms 2_000

  @type health_entry :: %{module: module, ok: boolean, error: term | nil}

  @doc """
  Runs every module in `modules` against `fetched` for `swarm`, threading
  each detector's own state from `states` (keyed by module). Returns
  `{alerts, states, health}`.
  """
  @spec run([module], Detector.fetched(), String.t(), map, map, integer, non_neg_integer) ::
          {[Detector.alert()], map, [health_entry]}
  def run(modules, fetched, swarm, global_thresholds, states, now_ms, timeout_ms \\ @default_timeout_ms) do
    Enum.reduce(modules, {[], states, []}, fn mod, {alerts, sts, health} ->
      prior = Map.get(sts, mod, initial_state(mod))
      thresholds = Map.merge(defaults(mod), global_thresholds || %{})
      ctx = %{swarm: swarm, thresholds: thresholds, state: prior, now_ms: now_ms}

      case run_one(mod, fetched, ctx, timeout_ms) do
        {:ok, raw, new_state} when is_list(raw) ->
          {valid, invalid} = Enum.split_with(raw, &valid_alert?/1)
          normalized = Enum.map(valid, &normalize(&1, swarm, mod))

          if invalid == [] do
            {alerts ++ normalized, Map.put(sts, mod, new_state),
             health ++ [%{module: mod, ok: true, error: nil}]}
          else
            {alerts ++ normalized ++ [synthetic(:detector_invalid, mod, swarm, now_ms, invalid)],
             sts, health ++ [%{module: mod, ok: false, error: :invalid_alerts}]}
          end

        {:malformed, reason} ->
          {alerts ++ [synthetic(:detector_invalid, mod, swarm, now_ms, reason)], sts,
           health ++ [%{module: mod, ok: false, error: :invalid_return}]}

        {:crashed, reason} ->
          {alerts ++ [synthetic(:detector_crashed, mod, swarm, now_ms, reason)], sts,
           health ++ [%{module: mod, ok: false, error: reason}]}
      end
    end)
  end

  # Runs `mod.detect/2` under a timeout guard AND with the raise/throw/exit
  # caught *inside* the task — Task.async links the task process to the
  # caller, so letting an exception escape would crash the caller (and, in
  # the engine, the whole object) instead of just this one detector.
  defp run_one(mod, fetched, ctx, timeout_ms) do
    task =
      Task.async(fn ->
        try do
          {:ok, mod.detect(fetched, ctx)}
        rescue
          e -> {:crashed, Exception.format(:error, e, __STACKTRACE__)}
        catch
          kind, reason -> {:crashed, inspect({kind, reason})}
        end
      end)

    case Task.yield(task, timeout_ms) do
      {:ok, {:ok, {raw, new_state}}} when is_list(raw) ->
        {:ok, raw, new_state}

      {:ok, {:ok, other}} ->
        {:malformed, "detect/2 returned #{inspect(other)}, expected {[alert], state}"}

      {:ok, {:crashed, reason}} ->
        {:crashed, reason}

      nil ->
        Task.shutdown(task, :brutal_kill)
        {:crashed, "timeout after #{timeout_ms}ms"}
    end
  end

  defp valid_alert?(%{type: type, swarm: swarm, at_ms: at_ms, summary: summary, evidence: evidence})
       when is_atom(type) and is_binary(swarm) and is_integer(at_ms) and is_binary(summary) and
              is_map(evidence),
       do: true

  defp valid_alert?(_), do: false

  defp normalize(alert, swarm, mod) do
    alert
    |> Map.put_new(:key, {swarm, alert.type})
    |> Map.put_new(:cids, [])
    |> Map.put(:source, mod)
    |> ensure_encodable_evidence()
  end

  # Alert evidence is Jason.encode!'d downstream (scope.ex: alert_card,
  # escalate, the coalesced-summary card) — a map-shaped evidence passes
  # valid_alert?/1 but can still smuggle pids/refs/tuples in its VALUES,
  # which would crash the whole tick at the encode. Sanitize at the
  # normalization boundary: keep the alert, swap the evidence for a bounded
  # inspect. Jason.encode/1 returns {:error, _} for these, but wrap the
  # raise path too — this guard must never itself take the tick down.
  defp ensure_encodable_evidence(alert) do
    case Jason.encode(alert.evidence) do
      {:ok, _} -> alert
      {:error, _} -> replace_evidence(alert)
    end
  rescue
    _ -> replace_evidence(alert)
  end

  defp replace_evidence(alert) do
    %{alert | evidence: %{"unencodable" => inspect(alert.evidence, limit: 20, printable_limit: 500)}}
  end

  defp synthetic(type, mod, swarm, now_ms, reason) do
    %{
      type: type,
      swarm: swarm,
      at_ms: now_ms,
      summary: "detector #{inspect(mod)} #{describe(type)}",
      evidence: %{"module" => inspect(mod), "reason" => inspect(reason)},
      key: {swarm, type},
      cids: [],
      source: mod
    }
  end

  defp describe(:detector_crashed), do: "crashed or timed out"
  defp describe(:detector_invalid), do: "returned malformed alerts"

  defp defaults(mod) do
    Code.ensure_loaded(mod)

    if function_exported?(mod, :default_thresholds, 0) do
      mod.default_thresholds()
    else
      %{}
    end
  end

  defp initial_state(mod) do
    Code.ensure_loaded(mod)

    if function_exported?(mod, :init, 0) do
      mod.init()
    else
      nil
    end
  end
end
