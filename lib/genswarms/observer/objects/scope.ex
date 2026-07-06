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
  - Detection runs through `DetectorRunner`, over `state.detectors`
    (`Genswarms.Observer.Detectors` plus, from O5, any registered custom
    detectors) — pure, deterministic, no LLM. Each detector's state is
    isolated per `{swarm, module}` — a crash or malformed return in one
    detector never corrupts another's state or stops the tick.
  - Dedupe + cooldown per alert `key` (default `{swarm, type}`, wingston
    roster pattern) lives here: a persisting condition alerts once per
    cooldown window, not once per tick. A per-swarm-per-tick alert budget
    (default 6) caps how many cards one tick can emit for one swarm;
    overflow collapses into a single `:alerts_coalesced` summary alert.
  - Durability is injectable (`Genswarms.Observer.Store`, config key
    `store_mod`, default `Store.InMemory`): `init/1` loads `det`,
    `last_alert` and `seen_periods` back and VALIDATES them here (the store
    itself is a dumb bag of terms) — future period ids and future cooldown
    timestamps are dropped, and a loaded `save_seq` behind this session's
    own watermark (config key `save_seq`, default 0 — meaningful only to a
    caller/host that kept one across a restart) synthesizes a
    `:store_rollback` alert, queued in `pending_alerts` and drained through
    the normal cooldown gate on the next tick. `save/1` runs at the end of
    any tick that mutated `det` or `last_alert`. A raising/crashing store
    must never take :scope down — load/save are fail-open (log, keep
    going); the VALIDATION itself is fail-closed (reject, don't trust).

  Actions (all allowlisted, fail-closed — empty list means nobody):
  - `tick` (tick_sources, normally just cron): fetch + detect + alert.
  - `status` (read_sources): registry, last tick, recent alerts.
  - `get_dashboard` / `get_events` (read_sources): fresh reads of one
    observed swarm, for agents (fase 3's :diagnostico asks here).
  """

  alias Genswarms.Observer.Detectors
  alias Genswarms.Observer.DetectorRunner
  alias Genswarms.Observer.Digest

  require Logger

  @alerts_kept 50
  @alert_budget_per_swarm 6
  @period_re ~r/^\d{4}-\d{2}-\d{2}$/

  # ── init ──────────────────────────────────────────────────────────────────

  def init(config) do
    swarm_name = cfg(config, :swarm_name, "observer")
    now_fn = cfg(config, :now_fn, fn -> System.system_time(:millisecond) end)

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
      client:
        module_ref(cfg(config, :client, Genswarms.Observer.Client.Http), Genswarms.Observer.Client.Http),
      client_opts: cfg(config, :client_opts, []),
      now_fn: now_fn,
      deliver_fn: cfg(config, :deliver_fn, default_deliver_fn(swarm_name)),
      store_mod:
        module_ref(
          cfg(config, :store_mod, Genswarms.Observer.Store.InMemory),
          Genswarms.Observer.Store.InMemory
        ),
      # Built-ins today; custom detector registration (per-swarm scoped,
      # boot-time only, never x-mutable) lands in O5. NOT read from config.
      detectors: [
        Detectors,
        Genswarms.Observer.Detectors.Unanswered,
        Genswarms.Observer.Detectors.DeliveryFailureBurst,
        Genswarms.Observer.Detectors.TopicsStale
      ],
      # Nested per swarm, then per detector module: `det[swarm][module]`.
      # Isolates one detector's state from another's under DetectorRunner.
      det: %{},
      last_alert: %{},
      # O4: unseen digest period ids per swarm. Persisted, validated on load.
      seen_periods: %{},
      # This session's own durability watermark. A caller/host that kept
      # one across a restart passes it in; a genuinely first-ever boot has
      # nothing to compare against, so 0 never false-positives a rollback.
      save_seq: cfg(config, :save_seq, 0),
      # Rollback alert (if any) queued here at boot, drained through the
      # normal cooldown gate on the first tick.
      pending_alerts: [],
      last_tick_ms: nil,
      alerts: []
    }

    {:ok, load_store(state, now_fn.())}
  end

  # ── store: load + validate ───────────────────────────────────────────────

  defp load_store(state, now) do
    case safe_store_load(state.store_mod) do
      :empty ->
        state

      {:ok, saved} when is_map(saved) ->
        merge_loaded(state, saved, now)

      {:error, reason} ->
        Logger.warning("[observer] store.load/0 returned error #{inspect(reason)} — booting empty")
        state

      other ->
        Logger.warning("[observer] store.load/0 returned malformed #{inspect(other)} — booting empty")
        state
    end
  end

  # A raising/exiting store must never block boot — durability is fail-open.
  defp safe_store_load(store_mod) do
    store_mod.load()
  rescue
    e ->
      Logger.warning("[observer] store.load/0 raised #{Exception.message(e)} — booting empty")
      :empty
  catch
    kind, reason ->
      Logger.warning("[observer] store.load/0 #{kind} #{inspect(reason)} — booting empty")
      :empty
  end

  defp merge_loaded(state, saved, now) do
    loaded_seq = Map.get(saved, :save_seq, 0)
    det = saved |> Map.get(:det, %{}) |> validate_det()
    last_alert = saved |> Map.get(:last_alert, %{}) |> validate_last_alert(now)
    seen_periods = saved |> Map.get(:seen_periods, %{}) |> validate_seen_periods(now)

    if loaded_seq < state.save_seq do
      Logger.warning(
        "[observer] store rollback: loaded save_seq=#{loaded_seq} < session save_seq=#{state.save_seq}"
      )

      %{
        state
        | det: det,
          last_alert: last_alert,
          seen_periods: seen_periods,
          pending_alerts: [rollback_alert(state, loaded_seq, now)]
      }
    else
      %{state | det: det, last_alert: last_alert, seen_periods: seen_periods, save_seq: loaded_seq}
    end
  end

  defp rollback_alert(state, loaded_seq, now) do
    %{
      key: {:store, :rollback},
      type: :store_rollback,
      swarm: state.swarm_name,
      at_ms: now,
      summary:
        "observer store loaded save_seq=#{loaded_seq}, older than this session's known " <>
          "#{state.save_seq} — possible stale restore",
      evidence: %{"loaded_seq" => loaded_seq, "session_seq" => state.save_seq},
      cids: []
    }
  end

  # det is opaque to Scope — kept as-is, only type-checked so a poisoned
  # value can't crash DetectorRunner downstream.
  defp validate_det(det) when is_map(det), do: det
  defp validate_det(_), do: %{}

  defp validate_last_alert(map, now) when is_map(map) do
    Map.filter(map, fn {_key, at_ms} -> is_integer(at_ms) and at_ms <= now end)
  end

  defp validate_last_alert(_, _now), do: %{}

  defp validate_seen_periods(map, now) when is_map(map) do
    tomorrow = now |> DateTime.from_unix!(:millisecond) |> DateTime.to_date() |> Date.add(1)

    Map.new(map, fn {swarm, periods} ->
      valid =
        periods
        |> periods_to_list()
        |> Enum.filter(&valid_period?(&1, tomorrow))
        |> MapSet.new()

      {swarm, valid}
    end)
  end

  defp validate_seen_periods(_, _now), do: %{}

  defp periods_to_list(%MapSet{} = set), do: MapSet.to_list(set)
  defp periods_to_list(list) when is_list(list), do: list
  defp periods_to_list(_), do: []

  defp valid_period?(id, tomorrow) when is_binary(id) do
    Regex.match?(@period_re, id) and
      case Date.from_iso8601(id) do
        {:ok, date} -> Date.compare(date, tomorrow) != :gt
        _ -> false
      end
  end

  defp valid_period?(_, _tomorrow), do: false

  # ── store: save ───────────────────────────────────────────────────────────

  defp persist(state) do
    next_seq = state.save_seq + 1

    payload = %{
      det: state.det,
      last_alert: state.last_alert,
      seen_periods: state.seen_periods,
      save_seq: next_seq
    }

    case safe_store_save(state.store_mod, payload) do
      :ok -> %{state | save_seq: next_seq}
      _other -> state
    end
  end

  # A raising/exiting store must never take :scope down — durability is
  # fail-open: log and keep going with in-memory state for this tick.
  defp safe_store_save(store_mod, payload) do
    case store_mod.save(payload) do
      :ok ->
        :ok

      other ->
        Logger.warning("[observer] store.save/1 returned #{inspect(other)} — durability skipped this tick")
        other
    end
  rescue
    e ->
      Logger.warning("[observer] store.save/1 raised #{Exception.message(e)} — durability skipped this tick")
      {:error, {:raised, e}}
  catch
    kind, reason ->
      Logger.warning("[observer] store.save/1 #{kind} #{inspect(reason)} — durability skipped this tick")
      {:error, {kind, reason}}
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
    orig_det = state.det
    orig_last_alert = state.last_alert
    orig_seen_periods = state.seen_periods

    {state, pending_fired, pending_suppressed} = drain_pending_alerts(state, now)

    {state, fired, suppressed} =
      Enum.reduce(state.registry, {state, pending_fired, pending_suppressed}, fn {swarm, entry},
                                                                                  {st, fired, supp} ->
        data = fetch(swarm, entry, st)
        swarm_det_states = Map.get(st.det, swarm, %{})

        {alerts, swarm_det_states, _health} =
          DetectorRunner.run(st.detectors, data, swarm, st.thresholds, swarm_det_states, now)

        st = %{st | det: Map.put(st.det, swarm, swarm_det_states)}

        {passed, supp} =
          Enum.reduce(alerts, {[], supp}, fn alert, {passed, supp} ->
            if cooled_down?(st, alert, now) do
              {[alert | passed], supp}
            else
              {passed, supp + 1}
            end
          end)

        budgeted = apply_alert_budget(Enum.reverse(passed), swarm, now)

        st = Enum.reduce(budgeted, st, fn alert, st -> emit_alert(st, alert, entry) end)

        st = deliver_digest(st, swarm, data)

        {st, fired + length(budgeted), supp}
      end)

    state = %{state | last_tick_ms: now}

    state =
      if state.det != orig_det or state.last_alert != orig_last_alert or
           state.seen_periods != orig_seen_periods do
        persist(state)
      else
        state
      end

    {:reply,
     Jason.encode!(%{
       ok: true,
       checked: map_size(state.registry),
       alerts: fired,
       suppressed: suppressed
     }), state}
  end

  # Rollback (and, later, any other system-level) alerts detected outside a
  # per-swarm context are stashed at boot and drained through the SAME
  # cooldown gate as detector alerts on the first tick — no bespoke alerting
  # path for a case that's rare by construction.
  defp drain_pending_alerts(%{pending_alerts: []} = state, _now), do: {state, 0, 0}

  defp drain_pending_alerts(state, now) do
    Enum.reduce(state.pending_alerts, {%{state | pending_alerts: []}, 0, 0}, fn alert, {st, fired, supp} ->
      if cooled_down?(st, alert, now) do
        {emit_alert(st, alert, %{}), fired + 1, supp}
      else
        {st, fired, supp + 1}
      end
    end)
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

  defp alert_key(alert), do: Map.get(alert, :key, {alert.swarm, alert.type})

  defp cooled_down?(state, alert, now) do
    window_ms = state.cooldown_minutes * 60_000

    case Map.get(state.last_alert, alert_key(alert)) do
      nil -> true
      last_ms -> now - last_ms >= window_ms
    end
  end

  # Caps how many cards one tick can emit for one swarm: a misbehaving
  # swarm firing many distinct alert types in one tick must not flood
  # :sender. Overflow collapses into one synthetic summary alert instead
  # of being silently dropped.
  defp apply_alert_budget(alerts, swarm, now_ms) do
    if length(alerts) <= @alert_budget_per_swarm do
      alerts
    else
      {kept, dropped} = Enum.split(alerts, @alert_budget_per_swarm)

      dropped_counts =
        dropped
        |> Enum.group_by(& &1.type)
        |> Map.new(fn {type, list} -> {to_string(type), length(list)} end)

      coalesced = %{
        type: :alerts_coalesced,
        swarm: swarm,
        at_ms: now_ms,
        summary: "#{length(dropped)} additional alert(s) suppressed by the per-tick budget",
        evidence: %{"dropped" => dropped_counts},
        key: {swarm, :alerts_coalesced},
        cids: []
      }

      kept ++ [coalesced]
    end
  end

  # ── digest (O4): conversation_topics extension → cards, seen-after-send ────
  #
  # Runs AFTER the detector/alert phase for this swarm, over the SAME fetch
  # (`data`) already pulled this tick — no extra round-trip. `Digest.plan/2`
  # is pure and total: a missing/malformed extension yields `{[], []}` and
  # this is a no-op. Cards are sent one by one; `seen_periods` is only
  # merged (and the tick only marked dirty) when EVERY card for this swarm
  # delivered `:ok` this tick — a partial failure retries the whole batch
  # next tick rather than silently losing a period. Cards are idempotent
  # content, so re-sending an already-delivered one on retry is harmless.
  defp deliver_digest(state, swarm, %{dashboard: {:ok, envelope}}) do
    seen = Map.get(state.seen_periods, swarm, MapSet.new())
    {cards, newly_seen} = Digest.plan(envelope, seen)

    case send_cards(state, cards) do
      :ok when newly_seen != [] ->
        updated = MapSet.union(seen, MapSet.new(newly_seen))
        %{state | seen_periods: Map.put(state.seen_periods, swarm, updated)}

      _ ->
        state
    end
  end

  defp deliver_digest(state, _swarm, _data), do: state

  defp send_cards(_state, []), do: :ok

  defp send_cards(state, cards) do
    results = Enum.map(cards, &deliver_digest_card(state, &1))
    if Enum.all?(results, &(&1 == :ok)), do: :ok, else: :error
  end

  defp deliver_digest_card(state, card) do
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
          "[observer] digest delivery to #{inspect(state.sender)} returned #{inspect(other)}"
        )

        other
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
      | last_alert: Map.put(state.last_alert, alert_key(alert), alert.at_ms),
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

  defp module_ref(mod, _default) when is_atom(mod), do: mod

  defp module_ref(name, default) when is_binary(name) do
    String.to_existing_atom("Elixir." <> String.trim_leading(name, "Elixir."))
  rescue
    ArgumentError -> default
  end
end
