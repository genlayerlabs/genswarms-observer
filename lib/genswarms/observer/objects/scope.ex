defmodule Genswarms.Observer.Objects.Scope do
  @moduledoc """
  The observer's only stateful piece: the registry of observed swarms, the
  per-tick fetch, and the alert pipeline. ObjectHandler by convention (no
  engine compile dep — the engine is reached via guarded apply).

  Trust model (the ecosystem's non-negotiables):
  - Only THIS object does HTTP; agents ask it via the topology, never sockets.
  - Tokens enter as env-var NAMES (`token_env`, x-secret contract §14.2.1)
    and are resolved at fetch time — never stored in config, state dumps are
    still safe because the resolved value never leaves the fetch closure.
  - Detection is `Genswarms.Observer.Detectors` — pure, deterministic, no LLM.
  - Dedupe + cooldown per `{swarm, type}` lives here (wingston roster
    pattern): a persisting condition alerts once per cooldown window, not
    once per tick.

  Actions (all allowlisted, fail-closed — empty list means nobody):
  - `tick` (tick_sources, normally just cron): fetch + detect + alert.
  - `status` (read_sources): registry, last tick, recent alerts.
  - `get_dashboard` / `get_events` (read_sources): fresh reads of one
    observed swarm, for agents (fase 3's :diagnostico asks here).
  """

  alias Genswarms.Observer.Detectors

  require Logger

  @alerts_kept 50

  # ── init ──────────────────────────────────────────────────────────────────

  def init(config) do
    swarm_name = cfg(config, :swarm_name, "observer")

    state = %{
      swarm_name: swarm_name,
      name: node_ref(cfg(config, :name, :scope)),
      registry: normalize_registry(cfg(config, :registry, %{})),
      thresholds: normalize_thresholds(cfg(config, :thresholds, %{})),
      cooldown_minutes: cfg(config, :cooldown_minutes, 30),
      tick_sources: MapSet.new(cfg(config, :tick_sources, []) |> Enum.map(&to_string/1)),
      read_sources: MapSet.new(cfg(config, :read_sources, []) |> Enum.map(&to_string/1)),
      sender: node_ref(cfg(config, :sender, :sender)),
      escalate_to: escalate_ref(cfg(config, :escalate_to, nil)),
      alert_conversation_id: cfg(config, :alert_conversation_id, nil),
      client: module_ref(cfg(config, :client, Genswarms.Observer.Client.Http)),
      client_opts: cfg(config, :client_opts, []),
      now_fn: cfg(config, :now_fn, fn -> System.system_time(:millisecond) end),
      deliver_fn: cfg(config, :deliver_fn, default_deliver_fn(swarm_name)),
      det: %{},
      last_alert: %{},
      last_tick_ms: nil,
      alerts: []
    }

    {:ok, state}
  end

  def interface do
    %{
      tick: %{
        input: ~s({"action":"tick"}),
        output: ~s({"ok":true,"checked":2,"alerts":1,"suppressed":0})
      },
      status: %{
        input: ~s({"action":"status"}),
        output: ~s({"ok":true,"watching":["wingston"],"last_tick_ms":123,"recent_alerts":[...]})
      },
      get_dashboard: %{
        input: ~s({"action":"get_dashboard","swarm":"wingston"}),
        output: "the observed swarm's live dashboard envelope"
      },
      get_events: %{
        input: ~s({"action":"get_events","swarm":"wingston"}),
        output: ~s({"ok":true,"events":[...]})
      }
    }
  end

  # ── messages ──────────────────────────────────────────────────────────────

  def handle_message(from, content, state) do
    case Jason.decode(content) do
      {:ok, %{"action" => "tick"}} ->
        if trusted?(from, state.tick_sources), do: tick(state), else: drop(from, "tick", state)

      {:ok, %{"action" => "status"}} ->
        if trusted?(from, state.read_sources) or trusted?(from, state.tick_sources),
          do: status(state),
          else: drop(from, "status", state)

      {:ok, %{"action" => "get_dashboard", "swarm" => swarm}} ->
        if trusted?(from, state.read_sources),
          do: read_remote(:dashboard, swarm, state),
          else: drop(from, "get_dashboard", state)

      {:ok, %{"action" => "get_events", "swarm" => swarm}} ->
        if trusted?(from, state.read_sources),
          do: read_remote(:events, swarm, state),
          else: drop(from, "get_events", state)

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  def dashboard(state) do
    [
      %{
        kind: :extension,
        name: "observer",
        data: %{
          count: map_size(state.registry),
          items:
            Enum.map(state.alerts, fn a ->
              %{swarm: a.swarm, type: a.type, at_ms: a.at_ms, summary: a.summary}
            end)
        }
      }
    ]
  end

  # ── tick ──────────────────────────────────────────────────────────────────

  defp tick(state) do
    now = state.now_fn.()

    {state, fired, suppressed} =
      Enum.reduce(state.registry, {state, 0, 0}, fn {swarm, entry}, {st, fired, supp} ->
        data = fetch(swarm, entry, st)

        {alerts, det_state} =
          Detectors.detect(swarm, data, st.thresholds, Map.get(st.det, swarm), now)

        st = %{st | det: Map.put(st.det, swarm, det_state)}

        Enum.reduce(alerts, {st, fired, supp}, fn alert, {st, fired, supp} ->
          if cooled_down?(st, alert, now) do
            {emit_alert(st, alert, entry), fired + 1, supp}
          else
            {st, fired, supp + 1}
          end
        end)
      end)

    state = %{state | last_tick_ms: now}

    {:reply,
     Jason.encode!(%{
       ok: true,
       checked: map_size(state.registry),
       alerts: fired,
       suppressed: suppressed
     }), state}
  end

  defp fetch(swarm, entry, state) do
    token = resolve_token(entry)
    base = entry["dashboard_url"]

    %{
      dashboard: safe_client(state, :get_dashboard, [base, swarm, token, state.client_opts]),
      events: safe_client(state, :get_events, [base, swarm, token, state.client_opts])
    }
  end

  # A crashing client must read as endpoint_down, never take the object down.
  defp safe_client(state, fun, args) do
    apply(state.client, fun, args)
  rescue
    e -> {:error, {:client_crash, Exception.message(e)}}
  catch
    :exit, reason -> {:error, {:client_exit, reason}}
  end

  defp resolve_token(entry) do
    case entry["token_env"] do
      env when is_binary(env) and env != "" ->
        case System.get_env(env) do
          t when is_binary(t) and t != "" -> t
          _ -> nil
        end

      _ ->
        nil
    end
  end

  # ── alerting: dedupe + cooldown, then card to :sender ─────────────────────

  defp cooled_down?(state, alert, now) do
    window_ms = state.cooldown_minutes * 60_000

    case Map.get(state.last_alert, {alert.swarm, alert.type}) do
      nil -> true
      last_ms -> now - last_ms >= window_ms
    end
  end

  defp emit_alert(state, alert, entry) do
    card = alert_card(alert, entry)

    payload =
      Jason.encode!(%{
        "action" => "send_card",
        "conversation_id" => state.alert_conversation_id,
        "card" => card
      })

    case state.deliver_fn.(state.sender, state.name, payload) do
      :ok ->
        :ok

      other ->
        Logger.warning(
          "[observer] alert delivery to #{inspect(state.sender)} returned #{inspect(other)}"
        )
    end

    escalate(state, alert)

    %{
      state
      | last_alert: Map.put(state.last_alert, {alert.swarm, alert.type}, alert.at_ms),
        alerts: Enum.take([alert | state.alerts], @alerts_kept)
    }
  end

  # Fase 3: the same alert (already cooldown-deduped) escalates as a TASK to
  # the diagnosis agent. The agent has no network towards the swarms — the
  # prompt reminds it to ask :scope through the topology.
  defp escalate(%{escalate_to: nil}, _alert), do: :ok

  defp escalate(state, alert) do
    task = """
    Observer ALERT — diagnose it.
    swarm: #{alert.swarm}
    type: #{alert.type}
    summary: #{alert.summary}
    evidence: #{Jason.encode!(alert.evidence)}

    You have NO network towards the swarms. Ask `scope` for data via swarm-msg ask:
      {"action":"get_dashboard","swarm":"#{alert.swarm}"}
      {"action":"get_events","swarm":"#{alert.swarm}"}
      {"action":"status"}
    Write a diagnosis: symptom, concrete evidence, hypotheses and the next
    actionable step.
    """

    case state.deliver_fn.(state.escalate_to, state.name, task) do
      :ok ->
        :ok

      other ->
        Logger.warning(
          "[observer] escalation to #{inspect(state.escalate_to)} returned #{inspect(other)}"
        )
    end
  end

  defp alert_card(alert, entry) do
    dashboard_link = "#{entry["dashboard_url"]}/api/swarms/#{alert.swarm}/dashboard"

    repo_line =
      case entry["repo"] do
        repo when is_binary(repo) and repo != "" -> "\nrepo: https://github.com/#{repo}"
        _ -> ""
      end

    %{
      "title" => "⚠️ observer: #{alert.swarm} · #{alert.type}",
      "blocks" => [
        %{"kind" => "paragraph", "text" => alert.summary},
        %{"kind" => "paragraph", "text" => "evidence: #{Jason.encode!(alert.evidence)}"},
        %{"kind" => "paragraph", "text" => "dashboard: #{dashboard_link}#{repo_line}"},
        %{
          "kind" => "paragraph",
          "text" =>
            "investigate: connect the genswarms-fleet MCP and run " <>
              ~s{get_events("#{alert.swarm}", level: "error") and get_dashboard("#{alert.swarm}").}
        }
      ]
    }
  end

  # ── agent-facing reads ────────────────────────────────────────────────────

  defp read_remote(kind, swarm, state) do
    case Map.get(state.registry, to_string(swarm)) do
      nil ->
        {:reply, Jason.encode!(%{ok: false, error: "swarm #{swarm} is not observed"}), state}

      entry ->
        token = resolve_token(entry)
        fun = if kind == :dashboard, do: :get_dashboard, else: :get_events

        case safe_client(state, fun, [entry["dashboard_url"], to_string(swarm), token, state.client_opts]) do
          {:ok, result} when kind == :dashboard ->
            {:reply, Jason.encode!(%{ok: true, dashboard: result}), state}

          {:ok, events} ->
            {:reply, Jason.encode!(%{ok: true, events: events}), state}

          {:error, reason} ->
            {:reply, Jason.encode!(%{ok: false, error: inspect(reason)}), state}
        end
    end
  end

  defp status(state) do
    {:reply,
     Jason.encode!(%{
       ok: true,
       watching: state.registry |> Map.keys() |> Enum.sort(),
       thresholds: state.thresholds,
       cooldown_minutes: state.cooldown_minutes,
       last_tick_ms: state.last_tick_ms,
       recent_alerts:
         Enum.map(state.alerts, fn a ->
           %{swarm: a.swarm, type: a.type, at_ms: a.at_ms, summary: a.summary}
         end)
     }), state}
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp trusted?(from, allowlist), do: MapSet.member?(allowlist, to_string(from))

  defp drop(from, action, state) do
    Logger.warning("[observer] dropped #{action} from untrusted #{inspect(from)}")
    {:noreply, state}
  end

  # Config arrives atom-keyed (Elixir swarm defs) or string-keyed (JSON IR /
  # config patches) — accept both.
  defp cfg(config, key, default) do
    Map.get(config, key, Map.get(config, to_string(key), default))
  end

  defp normalize_registry(registry) when is_map(registry) do
    Map.new(registry, fn {swarm, entry} ->
      {to_string(swarm), normalize_entry(entry)}
    end)
  end

  defp normalize_registry(_), do: %{}

  defp normalize_entry(entry) when is_map(entry) do
    %{
      "dashboard_url" => entry_get(entry, :dashboard_url),
      "token_env" => entry_get(entry, :token_env),
      "repo" => entry_get(entry, :repo)
    }
  end

  defp normalize_entry(_), do: %{"dashboard_url" => nil, "token_env" => nil, "repo" => nil}

  defp entry_get(entry, key),
    do: Map.get(entry, key, Map.get(entry, to_string(key)))

  defp normalize_thresholds(thresholds) when is_map(thresholds) do
    Map.new(thresholds, fn {k, v} -> {to_string(k), v} end)
  end

  defp normalize_thresholds(_), do: %{}

  # Engine access without a compile dep (genswarms-email bump pattern).
  defp default_deliver_fn(swarm_name) do
    fn target, from, content ->
      mod = Genswarms.Objects.ObjectServer

      if Code.ensure_loaded?(mod) and function_exported?(mod, :deliver_message, 4) do
        apply(mod, :deliver_message, [swarm_name, target, from, content])
        :ok
      else
        {:error, :engine_unavailable}
      end
    end
  end

  defp escalate_ref(nil), do: nil
  defp escalate_ref(""), do: nil
  defp escalate_ref(name), do: node_ref(name)

  # Topology node names arrive as atoms (Elixir defs) or strings (JSON IR).
  # Strings resolve via to_existing_atom — cron's pattern, no atom minting.
  defp node_ref(name) when is_atom(name), do: name

  defp node_ref(name) when is_binary(name) do
    String.to_existing_atom(name)
  rescue
    ArgumentError -> name
  end

  defp module_ref(mod) when is_atom(mod), do: mod

  defp module_ref(name) when is_binary(name) do
    String.to_existing_atom("Elixir." <> String.trim_leading(name, "Elixir."))
  rescue
    ArgumentError -> Genswarms.Observer.Client.Http
  end
end
