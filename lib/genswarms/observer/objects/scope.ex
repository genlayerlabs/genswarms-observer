defmodule Genswarms.Observer.Objects.Scope do
  @moduledoc """
  The observer's only stateful piece: the registry of observed swarms, the
  per-tick fetch, and the alert pipeline. ObjectHandler by convention (no
  engine compile dep — the engine is reached via guarded apply).

  Tick pipeline (per swarm, commit-together semantics):
    Ingest.fetch      → data + a PROPOSED feed cursor (never committed there)
    Signals stage     → declarative health_rules alerts (package + operator
                        rules, same evaluator) + the sovereign rules_gone
                        check; alerts join the SAME Lifecycle batch as
                        detector/quarantine alerts (see run_signals/4)
    DetectorRunner.run → alerts + per-module states (commit-on-success)
    quarantine        → 3 consecutive failures disable a module + drop its state
    cursor commit     → ONLY if every active detector succeeded this tick
    Lifecycle.process → cooldown, dedupe, budget, last_alert stamp + evict
    on_emitted        → re-fire guards applied ONLY to alerts that emitted
    Outbox            → cards / escalation / digest delivery at the edge

  Trust model (the ecosystem's non-negotiables):
  - Only THIS object does HTTP; agents ask it via the topology, never sockets.
  - Tokens enter as env-var NAMES (`token_env`, x-secret contract §14.2.1)
    and are resolved at fetch time — never stored in config, state dumps are
    still safe because the resolved value never leaves the fetch closure.
  - Detection runs through `DetectorRunner`, over the built-ins
    (`state.detectors`) plus, per swarm, any scoped `custom_detectors` —
    pure, deterministic, no LLM. Each detector's state is isolated per
    `{swarm, module}` — a crash or malformed return in one detector never
    corrupts another's state or stops the tick.
  - `custom_detectors` (config key, boot-time only, NEVER x-mutable) is the
    operator's explicit allowlist for third-party detector code sharing this
    trust boundary — vendored via `gsp` and digest-pinned by the notary.
    Resolution here is fail-CLOSED: an unresolvable module or one missing
    `detect/2` raises at `init/1` (a boot error), it is never skipped —
    a typo silently dropping a detector is worse than a crash. Boot also
    collects `default_thresholds/0` from every detector module (built-ins
    and customs) and raises on a threshold KEY COLLISION across modules
    (two modules declaring the same key); an undeclared-but-referenced
    threshold key can't be known statically and is NOT checked here.
  - Task 6: `Genswarms.Observer.Signals` evaluates the declarative
    `health_rules` grammar (see README "Health rules (declarative)") over
    two sources with different trust: package-shipped rules (inside a
    dashboard extension block's `"health_rules"` key — remote data) are
    fail-SOFT, a malformed block's rules are dropped and named in the
    `:signals` per-stage health, every other block still evaluates;
    operator rules (config `signal_rules`, boot-only, NEVER x-mutable,
    grouped by their `"block"`) are fail-CLOSED — an invalid one raises at
    `init/1`, same trust class as `custom_detectors`. Delta samples are
    swarm-scoped and persisted (`state.signals`); the sovereign
    `rules_gone` check watches which PACKAGE blocks are actually
    publishing `health_rules` this tick (never operator-configured
    blocks, which are static config) and fires only after 2 CONSECUTIVE
    dashboard-fetched-ok ticks with the block absent — see
    `run_signals/4`.
  - Dedupe + cooldown per alert `key` (default `{swarm, type}`, the
    roster-nudge pattern): a persisting condition alerts once per cooldown window,
    not once per tick. This POLICY lives in `Genswarms.Observer.Lifecycle`
    (pure, called from `tick/1`); `Scope` threads the state through and
    executes the resulting deliveries. A per-swarm-per-tick alert budget
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
  - `status` (read_sources): registry, last tick, recent alerts, relay log.
  - `get_dashboard` / `get_events` (read_sources): fresh reads of one
    observed swarm, for agents (fase 3's :diagnostico asks here).
  - `get_session_history` (read_sources, ADDITIONALLY gated — O6): the one
    window through which a diagnosis agent can read a conversation's
    transcript. `cid` is only eligible when it is named by an alert, for
    THAT swarm, of a deterministic built-in type, fresher than 60 minutes,
    still in `state.alerts` — and capped at 3 relays per alert (tracked in
    `state.relay_counts`, keyed by the alert's `key`). Every attempt
    (allowed or denied) appends to `state.relay_log` (kept 50, surfaced in
    `status/1`) — call metadata only, transcript content is never logged.
    Denials are fail-closed and ordered: unknown swarm, then no eligible
    alert (covers stale/wrong-type/wrong-swarm/no-matching-cid), then the
    per-alert budget.
  """

  alias Genswarms.Observer.Detectors
  alias Genswarms.Observer.DetectorRunner
  alias Genswarms.Observer.Digest
  alias Genswarms.Observer.Ingest
  alias Genswarms.Observer.Lifecycle
  alias Genswarms.Observer.Outbox
  alias Genswarms.Observer.Signals

  require Logger

  @alerts_kept 50
  @alert_budget_per_swarm 6
  @period_re ~r/^\d{4}-\d{2}-\d{2}$/
  @quarantine_after 3

  # Task 6 controller addendum: `:health_rule` / `:health_rules_gone`
  # (Signals stage) are deliberately absent from `@builtin_relay_types`
  # below AND never stamp a `:source` module — both relay gates
  # (`@builtin_relay_types` membership below and `@builtin_detector_modules`
  # provenance further down) already exclude them from
  # `get_session_history` eligibility. Documentation only, no code change.
  @item_key_max 64
  @alerts_per_rule_cap 10

  # O6: cid-bound diagnosis relay. The eligible-type set is a literal list,
  # not derived from the loaded modules — deriving it would mean executing
  # every detector (same limitation noted on `check_threshold_collisions!/1`
  # below). Deliberately excludes package-namespaced custom-detector types,
  # `:topics_stale` (never carries cids) and every synthetic/system alert
  # (`:alerts_coalesced`, `:detector_crashed`, `:detector_invalid`,
  # `:store_rollback`) — only the classic health detectors plus the two
  # cid-carrying delivery detectors.
  #
  # O6 fix wave: this type allowlist is defense-in-depth only, kept for
  # belt-and-suspenders — it is NOT what makes the gate trustworthy, because
  # a custom detector can simply return one of these type atoms unprefixed.
  # The load-bearing check is `@builtin_detector_modules` below, matched
  # against `alert.source` (the detector module `DetectorRunner` actually
  # ran, stamped onto the alert by the runner itself).
  @builtin_relay_types MapSet.new([
                         :unanswered,
                         :delivery_failure_burst,
                         :stall,
                         :error_burst,
                         :budget_block,
                         :pool_saturated,
                         :endpoint_down
                       ])
  @relay_window_ms 60 * 60_000
  @relay_budget_per_alert 3
  @relay_log_kept 50

  @builtin_detectors [
    Detectors,
    Genswarms.Observer.Detectors.Unanswered,
    Genswarms.Observer.Detectors.DeliveryFailureBurst,
    Genswarms.Observer.Detectors.TopicsStale,
    Genswarms.Observer.Detectors.Restarted
  ]

  # O6 fix wave: the LOAD-BEARING relay gate. `@builtin_relay_types` alone is
  # just a type-atom allowlist — a custom detector can return an unprefixed
  # `:unanswered`/`:stall`/etc alert and forge eligibility. `DetectorRunner`
  # stamps every alert with `source: mod` (the module that produced it,
  # never taken from the detector's own return value — see
  # `DetectorRunner.normalize/3`), so provenance is checked against the
  # module that actually ran, not a self-reported type name.
  @builtin_detector_modules MapSet.new(@builtin_detectors)

  # ── init ──────────────────────────────────────────────────────────────────

  def init(config) do
    swarm_name = cfg(config, :swarm_name, "observer")
    now_fn = cfg(config, :now_fn, fn -> System.system_time(:millisecond) end)

    custom_detectors = resolve_custom_detectors!(cfg(config, :custom_detectors, []))
    check_threshold_collisions!(@builtin_detectors ++ Enum.map(custom_detectors, & &1.module))
    signal_rules_by_block = build_signal_rules!(cfg(config, :signal_rules, []))

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
        store_module_ref!(
          cfg(config, :store_mod, nil),
          Genswarms.Observer.Store.InMemory
        ),
      detectors: @builtin_detectors,
      # O5: config key `custom_detectors`, resolved fail-closed above.
      # Each entry `%{module: mod, swarms: nil | [name, ...]}` — `nil`/`[]`
      # means global (runs for every observed swarm); otherwise scoped to
      # exactly those swarm names. Boot-time only, never read again after
      # init, NEVER x-mutable (loading third-party code is the operator's
      # explicit allowlist, not a hot-tunable).
      custom_detectors: custom_detectors,
      # Task 6: operator-configured health_rules, grouped by their "block"
      # key, already validated fail-CLOSED by build_signal_rules!/1 above
      # (raises at boot on a malformed entry — same trust class as
      # custom_detectors). Boot-only, never re-read, NEVER x-mutable.
      signal_rules_by_block: signal_rules_by_block,
      # Nested per swarm, then per detector module: `det[swarm][module]`.
      # Isolates one detector's state from another's under DetectorRunner.
      det: %{},
      # Per-swarm cursor into the observed swarm's display event feed
      # (GET .../events/feed?since=N). Session-local by design, deliberately
      # NOT persisted: on an observer restart the host's ring replays
      # ascending from seq 1 and every already-answered
      # request_open/reply_sent pair cancels out inside the feed detectors —
      # a replay costs nothing, while a persisted-but-stale cursor against a
      # restarted feed (seqs reset to 1) would need rollback bookkeeping.
      # A swarm ABSENT from this map means "never read this session": the
      # first read drains the ring to head in one union (see `fetch/3`) so
      # the replay's answered pairs cancel inside a single detect/2 instead
      # of false-alerting across server page boundaries.
      feed_cursors: %{},
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
      alerts: [],
      # O6: diagnosis relay bookkeeping. `relay_counts` is keyed by the
      # SAME alert `key` used for cooldown, and pruned at every emit
      # (`emit_alert/4`, right where `alerts` is trimmed) down to the keys
      # still present in `alerts` — the budget is tied to the live alert
      # window, so a fresh same-key alert after the old one aged out
      # starts from a clean count instead of inheriting an exhausted one.
      # `relay_log` is metadata-only, never transcript content — see
      # `log_relay/6`.
      relay_counts: %{},
      relay_log: [],
      # O7: per-stage pipeline self-observability, per observed swarm:
      # `%{swarm => %{stage => %{last_success_ms: int | nil, last_error:
      # nil | string}}}` for stages :fetch / :decode / :detectors /
      # :digest / :sender. Updated inside tick/1 as each stage actually
      # runs — a stage that didn't run this tick keeps its previous entry.
      # Session-local bookkeeping, deliberately NOT persisted (health is a
      # statement about THIS process's pipeline, not durable history).
      health: %{},
      # F2: `{swarm, module} => consecutive failed ticks`. At
      # @quarantine_after the module stops running for that swarm, its
      # per-swarm det state is DROPPED (a poisoned entry can never heal by
      # replaying), and one :detector_quarantined alert goes through the
      # normal pipeline. Session-local by design: an observer restart is
      # the operator's reset lever.
      quarantine: %{},
      # Task 6: `Genswarms.Observer.Signals` bookkeeping. `samples` is
      # `%{{swarm, block_key, rule_id, path} => number}` (delta evaluator
      # state, PERSISTED — see store.ex); `rules_seen` is
      # `%{swarm => MapSet.t(block_key)}` and `rules_miss` is
      # `%{swarm => %{block_key => consecutive_miss_count}}`, the sovereign
      # rules_gone debounce (2 consecutive dashboard-ok misses before
      # firing). Both loaded/validated in load_store below.
      signals: %{samples: %{}, rules_seen: %{}, rules_miss: %{}}
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
    loaded_seq = validate_save_seq(Map.get(saved, :save_seq, 0))
    det = saved |> Map.get(:det, %{}) |> validate_det()
    last_alert = saved |> Map.get(:last_alert, %{}) |> validate_last_alert(now)
    seen_periods = saved |> Map.get(:seen_periods, %{}) |> validate_seen_periods(now)
    signals = saved |> Map.get(:signals, %{}) |> validate_signals()

    if loaded_seq < state.save_seq do
      Logger.warning(
        "[observer] store rollback: loaded save_seq=#{loaded_seq} < session save_seq=#{state.save_seq}"
      )

      %{
        state
        | det: det,
          last_alert: last_alert,
          seen_periods: seen_periods,
          signals: signals,
          pending_alerts: [rollback_alert(state, loaded_seq, now)]
      }
    else
      %{
        state
        | det: det,
          last_alert: last_alert,
          seen_periods: seen_periods,
          signals: signals,
          save_seq: loaded_seq
      }
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

  # det is opaque to Scope BELOW the per-swarm level — but the two levels
  # Scope/DetectorRunner navigate themselves are type-checked: the outer map
  # AND each per-swarm value (DetectorRunner does `Map.get(states, mod, _)`
  # on it — a non-map would BadMapError mid-tick). A poisoned per-swarm
  # entry is dropped (that swarm's detectors restart from init/0), the rest
  # is kept.
  defp validate_det(det) when is_map(det) do
    {valid, dropped} = Enum.split_with(det, fn {_swarm, per_swarm} -> is_map(per_swarm) end)

    Enum.each(dropped, fn {swarm, value} ->
      Logger.warning(
        "[observer] store det[#{inspect(swarm)}] is not a map (#{inspect(value)}) — dropped, " <>
          "that swarm's detectors restart fresh"
      )
    end)

    Map.new(valid)
  end

  defp validate_det(_), do: %{}

  # save_seq feeds `persist/1`'s `+ 1` and the rollback comparison — a
  # non-integer from a corrupt store must be neutralized HERE or the first
  # dirty tick dies in persist arithmetic. Treated as 0 = "fresh boot"
  # (0 never false-positives a rollback, see init/1).
  defp validate_save_seq(seq) when is_integer(seq) and seq >= 0, do: seq

  defp validate_save_seq(bad) do
    Logger.warning(
      "[observer] store save_seq #{inspect(bad)} is not a non-negative integer — treating as 0"
    )

    0
  end

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

  # Task 6: `signals` is opaque to Scope's callers but its own internal
  # shape IS checked here — a corrupt entry from a hand-edited/older store
  # must degrade to that piece's empty default, never crash boot or feed
  # garbage into Signals.evaluate/7 (which trusts its `samples` argument).
  defp validate_signals(signals) when is_map(signals) do
    %{
      samples: validate_signal_samples(Map.get(signals, :samples)),
      rules_seen: validate_signal_rules_seen(Map.get(signals, :rules_seen)),
      rules_miss: validate_signal_rules_miss(Map.get(signals, :rules_miss))
    }
  end

  defp validate_signals(_), do: %{samples: %{}, rules_seen: %{}, rules_miss: %{}}

  # Keys must be exactly the 4-tuple {swarm, block_key, rule_id, path} of
  # binaries with a numeric value — anything else (wrong arity, wrong
  # element types, a non-numeric value) is dropped, not the whole map.
  defp validate_signal_samples(samples) when is_map(samples) do
    for {{s, bk, rid, path}, v} <- samples,
        is_binary(s) and is_binary(bk) and is_binary(rid) and is_binary(path) and is_number(v),
        into: %{},
        do: {{s, bk, rid, path}, v}
  end

  defp validate_signal_samples(_), do: %{}

  defp validate_signal_rules_seen(map) when is_map(map) do
    for {swarm, set} <- map, is_binary(swarm), into: %{} do
      valid = set |> periods_to_list() |> Enum.filter(&is_binary/1) |> MapSet.new()
      {swarm, valid}
    end
  end

  defp validate_signal_rules_seen(_), do: %{}

  defp validate_signal_rules_miss(map) when is_map(map) do
    for {swarm, counts} <- map, is_binary(swarm), into: %{} do
      valid =
        case counts do
          c when is_map(c) ->
            for {block, n} <- c, is_binary(block), is_integer(n), n >= 0, into: %{}, do: {block, n}

          _ ->
            %{}
        end

      {swarm, valid}
    end
  end

  defp validate_signal_rules_miss(_), do: %{}

  # ── store: save ───────────────────────────────────────────────────────────

  defp persist(state) do
    next_seq = state.save_seq + 1

    payload = %{
      det: state.det,
      last_alert: state.last_alert,
      seen_periods: state.seen_periods,
      signals: state.signals,
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
        output: ~s({"ok":true,"watching":["myswarm"],"last_tick_ms":123,"recent_alerts":[...]})
      },
      get_dashboard: %{
        input: ~s({"action":"get_dashboard","swarm":"myswarm"}),
        output: "the observed swarm's live dashboard envelope"
      },
      get_events: %{
        input: ~s({"action":"get_events","swarm":"myswarm"}),
        output: ~s({"ok":true,"events":[...]})
      },
      get_session_history: %{
        input: ~s({"action":"get_session_history","swarm":"myswarm","cid":"tg:1:0"}),
        output:
          ~s({"ok":true,"history":{...}} — cid must be named by a fresh built-in alert, 3 relays max)
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

      {:ok, %{"action" => "get_session_history", "swarm" => swarm, "cid" => cid}}
      when is_binary(swarm) and is_binary(cid) ->
        if trusted?(from, state.read_sources),
          do: relay_session_history(from, swarm, cid, state),
          else: drop(from, "get_session_history", state)

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
            end),
          health: health_summary(state.health)
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
    orig_signals = state.signals

    {state, pending_fired, pending_suppressed} = drain_pending_alerts(state, now)

    {state, fired, suppressed} =
      Enum.reduce(state.registry, {state, pending_fired, pending_suppressed}, fn {swarm, entry},
                                                                                  {st, fired, supp} ->
        {data, proposed_cursor, st} = fetch(swarm, entry, st)

        st =
          st
          |> record_fetch_health(swarm, data, now)
          |> record_decode_health(swarm, data, now)

        {st, signal_alerts} = run_signals(st, swarm, data, now)

        swarm_det_states = Map.get(st.det, swarm, %{})

        {alerts, swarm_det_states, det_health} =
          DetectorRunner.run(detectors_for(st, swarm), data, swarm, st.thresholds, swarm_det_states, now)

        st = %{st | det: Map.put(st.det, swarm, swarm_det_states)}
        st = record_detector_health(st, swarm, det_health, now)
        {st, quarantine_alerts} = update_quarantine(st, swarm, det_health, now)

        # F1: the cursor is the read-side commit point and the detector
        # states are the compute-side commit point — they must move
        # TOGETHER. A tick where any ACTIVE detector failed keeps the old
        # cursor: the window replays next tick into detectors that are
        # replay-safe by contract (put_new opens, seq-keyed dedupe), while
        # the failed detector gets another shot at the same evidence.
        # Quarantined modules are not active (detectors_for excludes them),
        # so a permanently broken detector stops holding reads hostage
        # after @quarantine_after ticks.
        st =
          if is_integer(proposed_cursor) and Enum.all?(det_health, & &1.ok) do
            %{st | feed_cursors: Map.put(st.feed_cursors, swarm, proposed_cursor)}
          else
            st
          end

        %{emit: budgeted, suppressed: tick_suppressed, last_alert: new_last_alert} =
          Lifecycle.process(
            quarantine_alerts ++ alerts ++ signal_alerts,
            st.last_alert,
            st.cooldown_minutes * 60_000,
            @alert_budget_per_swarm,
            swarm,
            now
          )

        st = %{st | last_alert: new_last_alert}
        st = Enum.reduce(budgeted, st, fn alert, st -> emit_alert(st, alert, entry, now) end)
        st = apply_on_emitted(st, swarm, budgeted)

        st = deliver_digest(st, swarm, data, now)

        {st, fired + length(budgeted), supp + tick_suppressed}
      end)

    state = %{state | last_tick_ms: now}

    state =
      if state.det != orig_det or state.last_alert != orig_last_alert or
           state.seen_periods != orig_seen_periods or state.signals != orig_signals do
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
    %{emit: emit, suppressed: suppressed, last_alert: new_last_alert} =
      Lifecycle.process(
        state.pending_alerts,
        state.last_alert,
        state.cooldown_minutes * 60_000,
        @alert_budget_per_swarm,
        state.swarm_name,
        now
      )

    state = %{state | pending_alerts: [], last_alert: new_last_alert}
    state = Enum.reduce(emit, state, fn alert, st -> emit_alert(st, alert, %{}, now) end)

    {state, length(emit), suppressed}
  end

  # Drain page budget: the server pages the feed (the client asks for 500
  # per page, and a host may clamp lower — one reference host caps limit at
  # 1_000 while its ring holds 5_000, so "one huge read" is NOT reliably
  # achievable). 10 pages × 500 = 5_000 covers the known host ring sizes
  # (4_096 and 5_000) while bounding a pathological feed. Bounds
  # EVERY read, not just the first — see Ingest's moduledoc (F5):
  # steady-state reads drain to head exactly like the first read.
  @feed_max_pages 10

  # Returns {data, proposed_cursor, state} — the cursor is only a PROPOSAL
  # here; tick/1 commits it after the detector phase (F1: a tick where a
  # feed-consuming detector failed must re-read this window next tick).
  defp fetch(swarm, entry, state) do
    cursor = Map.get(state.feed_cursors, swarm)

    {data, proposed} =
      Ingest.fetch(state.client, state.client_opts, swarm, entry, cursor, @feed_max_pages)

    {data, proposed, state}
  end

  # A crashing client must read as endpoint_down, never take the object down.
  defp safe_client(state, fun, args) do
    apply(state.client, fun, args)
  rescue
    e -> {:error, {:client_crash, Exception.message(e)}}
  catch
    :exit, reason -> {:error, {:client_exit, reason}}
  end

  # ── alerting: dedupe + cooldown, then card to :sender ─────────────────────
  #
  # Cooldown/dedupe/budget policy itself lives in the pure `Lifecycle`
  # module (called from `tick/1` and `drain_pending_alerts/2`); this
  # delegate keeps `alert_key/1` as a single home while letting the relay
  # gate and `relay_counts` pruning below keep compiling unchanged.
  defp alert_key(alert), do: Lifecycle.alert_key(alert)

  # ── digest (O4): conversation_topics extension → cards, seen-after-send ────
  #
  # Runs AFTER the detector/alert phase for this swarm, over the SAME fetch
  # (`data`) already pulled this tick — no extra round-trip. `Digest.plan/3`
  # is passed `swarm`, the trusted registry key, so a compromised or buggy
  # upstream can never spoof a card title via `envelope["swarm"]`. It is
  # pure and total: a missing/malformed extension yields `{[], []}` and
  # this is a no-op. Cards are sent one by one; `seen_periods` is only
  # merged (and the tick only marked dirty) when EVERY card for this swarm
  # delivered `:ok` this tick — a partial failure retries the whole batch
  # next tick rather than silently losing a period. Cards are idempotent
  # content, so re-sending an already-delivered one on retry is harmless.
  defp deliver_digest(state, swarm, %{dashboard: {:ok, envelope}}, now) do
    seen = Map.get(state.seen_periods, swarm, MapSet.new())
    {cards, newly_seen} = Digest.plan(swarm, envelope, seen)

    results =
      Enum.map(cards, fn card ->
        Outbox.send_card(state.deliver_fn, state.sender, state.name, state.alert_conversation_id, card)
      end)

    delivered = Enum.count(results, &(&1 == :ok))

    state =
      state
      |> record_digest_health(swarm, length(cards), delivered, now)
      |> record_sender_batch(swarm, results, now)

    if delivered == length(cards) and newly_seen != [] do
      updated = MapSet.union(seen, MapSet.new(newly_seen))
      %{state | seen_periods: Map.put(state.seen_periods, swarm, updated)}
    else
      state
    end
  end

  defp deliver_digest(state, _swarm, _data, _now), do: state

  # ── Signals stage (Task 6): declarative health_rules → alerts ─────────────
  #
  # Runs right after `fetch`, BEFORE `DetectorRunner.run` — its alerts join
  # the SAME `Lifecycle.process` batch as quarantine/detector alerts, so
  # cooldown/dedupe/budget apply uniformly (see tick/1). Pure grammar
  # evaluation is `Signals`' job; this function owns the trust split and
  # the per-swarm state threading:
  #   - package rules (`block["health_rules"]`) are fail-SOFT: a malformed
  #     block's rules are dropped and the block is named in this tick's
  #     `:signals` health error — every OTHER block still evaluates.
  #   - operator rules (`state.signal_rules_by_block`, already validated
  #     fail-CLOSED at init/1) always run, even against a block the
  #     package never annotated with "health_rules" (evaluated against
  #     `Map.get(extensions, block_key, %{})`, absence-tolerant).
  #   - delta samples are swarm-scoped and persisted (`state.signals`); a
  #     block absent this tick is simply not evaluated, so its samples sit
  #     untouched — a delta rule compares against the last KNOWN reading
  #     whenever the block reappears (deltas may span blind ticks by
  #     design; see README "Health rules (declarative)").
  #   - the sovereign rules_gone check is independent bookkeeping over
  #     which PACKAGE blocks are actually publishing "health_rules" this
  #     tick (see advance_rules_gone/4).
  # Skipped ENTIRELY when the dashboard fetch failed/was unavailable this
  # tick — no-verdict discipline, matching :decode: samples/rules_seen/
  # rules_miss are untouched, no :signals health update either.
  defp run_signals(state, swarm, %{dashboard: {:ok, envelope}}, now) when is_map(envelope) do
    extensions =
      case Map.get(envelope, "extensions") do
        m when is_map(m) -> m
        _ -> %{}
      end

    package_block_keys =
      for {block_key, block} <- extensions,
          is_binary(block_key),
          is_map(block),
          Map.has_key?(block, "health_rules"),
          into: MapSet.new(),
          do: block_key

    operator_block_keys = state.signal_rules_by_block |> Map.keys() |> MapSet.new()
    all_block_keys = MapSet.union(package_block_keys, operator_block_keys)

    swarm_samples = samples_for_swarm(state.signals.samples, swarm)

    {alerts, new_swarm_samples, block_errors} =
      Enum.reduce(all_block_keys, {[], swarm_samples, []}, fn block_key,
                                                                {alerts_acc, samples_acc, errs_acc} ->
        block =
          case Map.get(extensions, block_key) do
            m when is_map(m) -> m
            _ -> %{}
          end

        {pkg_rules, errs_acc} =
          if MapSet.member?(package_block_keys, block_key) do
            case Signals.validate_rules(block["health_rules"]) do
              {:ok, rules} -> {rules, errs_acc}
              {:error, reason} -> {[], ["#{block_key}: #{reason}" | errs_acc]}
            end
          else
            {[], errs_acc}
          end

        operator_rules = Map.get(state.signal_rules_by_block, block_key, [])

        {new_alerts, new_samples} =
          Signals.evaluate(block_key, block, pkg_rules ++ operator_rules, extensions, samples_acc, swarm, now)

        {alerts_acc ++ new_alerts, new_samples, errs_acc}
      end)

    state = put_swarm_samples(state, swarm, new_swarm_samples)
    {state, gone_alerts} = advance_rules_gone(state, swarm, package_block_keys, now)
    state = record_signals_health(state, swarm, block_errors, now)

    {state, bound_signal_alerts(alerts) ++ gone_alerts}
  end

  defp run_signals(state, _swarm, _data, _now), do: {state, []}

  defp samples_for_swarm(samples, swarm) do
    for {{s, block_key, rule_id, path}, v} <- samples,
        s == swarm,
        into: %{},
        do: {{block_key, rule_id, path}, v}
  end

  defp put_swarm_samples(state, swarm, swarm_samples) do
    others = for {{s, _bk, _rid, _p}, _v} = entry <- state.signals.samples, s != swarm, into: %{}, do: entry
    mine = for {{bk, rid, p}, v} <- swarm_samples, into: %{}, do: {{swarm, bk, rid, p}, v}
    %{state | signals: %{state.signals | samples: Map.merge(others, mine)}}
  end

  # Sovereign rules_gone: independent of the delta/sample bookkeeping
  # above. A block that has published "health_rules" (present_keys, this
  # tick's package blocks) is tracked in rules_seen; once seen, a block
  # ABSENT from present_keys for 2 CONSECUTIVE dashboard-ok ticks fires
  # one candidate alert PER TICK thereafter (Lifecycle's cooldown, not
  # this counter, governs the actual re-fire cadence downstream — same
  # convention as every other detector-generated candidate). The 2-tick
  # debounce tolerates a component's brief absence (e.g. telegram_poller
  # missing for the few seconds a restarting bot process takes to come
  # back — block-absent-not-stale) without paging on the very first miss.
  # A dashboard fetch error/unavailable never reaches this function (see
  # run_signals/4's guard above) — it resets nothing and counts nothing,
  # exactly as if the tick never happened for this purpose.
  defp advance_rules_gone(state, swarm, present_keys, now) do
    seen = Map.get(state.signals.rules_seen, swarm, MapSet.new())
    miss = Map.get(state.signals.rules_miss, swarm, %{})
    known_keys = MapSet.union(seen, present_keys)

    {new_miss, alerts} =
      Enum.reduce(known_keys, {%{}, []}, fn block_key, {miss_acc, alerts_acc} ->
        if MapSet.member?(present_keys, block_key) do
          {Map.put(miss_acc, block_key, 0), alerts_acc}
        else
          count = Map.get(miss, block_key, 0) + 1
          miss_acc = Map.put(miss_acc, block_key, count)

          if count >= 2 do
            {miss_acc, [rules_gone_alert(swarm, block_key, now) | alerts_acc]}
          else
            {miss_acc, alerts_acc}
          end
        end
      end)

    state = %{
      state
      | signals: %{
          state.signals
          | rules_seen: Map.put(state.signals.rules_seen, swarm, known_keys),
            rules_miss: Map.put(state.signals.rules_miss, swarm, new_miss)
        }
    }

    {state, alerts}
  end

  defp rules_gone_alert(swarm, block_key, now) do
    %{
      type: :health_rules_gone,
      swarm: swarm,
      at_ms: now,
      summary:
        "block #{block_key} stopped publishing health_rules for 2 consecutive ticks — " <>
          "the package removed/regressed its rules, or the component is down",
      evidence: %{"block" => block_key},
      key: {swarm, :health_rules_gone, block_key},
      cids: []
    }
  end

  # Task 6 controller addendum: bounded HERE, not inside Signals —
  # signals.ex stays a pure grammar evaluator with no opinion on hostile
  # block sizes. A block that reports (or lies about) an oversized "each"
  # list must not mint unbounded alert keys/cooldown entries: cap alerts
  # per (block, rule_id) per tick at 10 (silently drop the rest — the
  # per-swarm alert budget downstream would coalesce any real overflow
  # anyway) and slice a string item_key to 64 chars before it becomes part
  # of a persisted-adjacent cooldown key.
  defp bound_signal_alerts(alerts) do
    alerts
    |> Enum.map(&bound_item_key/1)
    |> Enum.reduce({%{}, []}, fn alert, {counts, acc} ->
      group = signal_rule_group(alert)
      count = Map.get(counts, group, 0)

      if count < @alerts_per_rule_cap do
        {Map.put(counts, group, count + 1), [alert | acc]}
      else
        {counts, acc}
      end
    end)
    |> elem(1)
    |> Enum.reverse()
  end

  defp signal_rule_group(%{evidence: %{"block" => block, "rule_id" => id}}), do: {block, id}
  defp signal_rule_group(alert), do: Map.get(alert, :type)

  defp bound_item_key(%{key: {swarm, :health_rule, block_key, id, item_key}} = alert)
       when is_binary(item_key) do
    %{alert | key: {swarm, :health_rule, block_key, id, String.slice(item_key, 0, @item_key_max)}}
  end

  defp bound_item_key(alert), do: alert

  # :signals — package-rule validation failures this tick, per block. No
  # blocks published health_rules and no operator rules configured is a
  # healthy no-op, same convention as :digest.
  defp record_signals_health(state, swarm, [], now),
    do: record_health(state, swarm, :signals, now, :ok)

  defp record_signals_health(state, swarm, errors, now) do
    record_health(
      state,
      swarm,
      :signals,
      now,
      {:error, "invalid package rules: " <> Enum.join(Enum.sort(errors), "; ")}
    )
  end

  # ── pipeline health (O7): per-swarm, per-stage self-observability ─────────
  #
  # `state.health[swarm][stage] = %{last_success_ms: int | nil, last_error:
  # nil | string}` for the stages :fetch, :decode, :detectors, :digest and
  # :sender. Recording is strictly per stage-run: success stamps
  # `last_success_ms` and CLEARS `last_error`; failure sets `last_error`
  # and leaves `last_success_ms` at whatever it was (nil if the stage never
  # succeeded). A stage that didn't run this tick is not touched at all.
  # These helpers must NEVER raise — any weird shape degrades to an error
  # string via inspect, never a crash mid-tick.

  @health_error_cap 500

  # :fetch — all client calls of this tick's fetch. Any non-{:ok, _}
  # (including a shape a buggy client invented) counts as a fetch error —
  # EXCEPT the feed's :unavailable, which is the wire's own fail-soft
  # answer for "this host has no EventsSource" (plug.ex serves it as a
  # 200, not an error): a healthy state, not a fetch failure.
  defp record_fetch_health(state, swarm, data, now) do
    errors =
      Enum.flat_map(data, fn
        {_part, {:ok, _}} -> []
        {:feed, :unavailable} -> []
        {part, {:error, reason}} -> ["#{part}: #{health_error(reason)}"]
        {part, other} -> ["#{part}: malformed client return #{health_error(other)}"]
      end)

    case errors do
      [] -> record_health(state, swarm, :fetch, now, :ok)
      errs -> record_health(state, swarm, :fetch, now, {:error, Enum.join(Enum.sort(errs), "; ")})
    end
  end

  # :decode — the conversation_topics extension parse. Only runs when the
  # dashboard actually fetched (otherwise there is nothing to decode and
  # the stage keeps its previous entry). "Extension absent" is success
  # with nothing to do; "present but malformed" is the decode error.
  defp record_decode_health(state, swarm, %{dashboard: {:ok, envelope}}, now) do
    case Digest.decode_health(envelope) do
      :malformed ->
        record_health(
          state,
          swarm,
          :decode,
          now,
          {:error, "conversation_topics extension present but malformed"}
        )

      _ok_or_absent ->
        record_health(state, swarm, :decode, now, :ok)
    end
  end

  defp record_decode_health(state, _swarm, _data, _now), do: state

  # :detectors — from DetectorRunner's health return: any module ok:false
  # (crash, timeout, malformed return, invalid alerts) fails the stage,
  # naming the module(s).
  defp record_detector_health(state, swarm, det_health, now) do
    failing = for %{module: mod, ok: false} <- det_health, do: inspect(mod)

    case failing do
      [] ->
        record_health(state, swarm, :detectors, now, :ok)

      mods ->
        record_health(
          state,
          swarm,
          :detectors,
          now,
          {:error, "failing: " <> Enum.join(mods, ", ")}
        )
    end
  end

  # F2: success resets the streak; failure increments it. Crossing the
  # threshold drops the module's (possibly poisoned) state and emits one
  # synthetic alert through the NORMAL pipeline (cooldown/budget apply).
  defp update_quarantine(state, swarm, det_health, now) do
    Enum.reduce(det_health, {state, []}, fn %{module: mod, ok: ok}, {st, alerts} ->
      key = {swarm, mod}

      cond do
        ok ->
          {%{st | quarantine: Map.delete(st.quarantine, key)}, alerts}

        Map.get(st.quarantine, key, 0) + 1 == @quarantine_after ->
          st = %{
            st
            | quarantine: Map.put(st.quarantine, key, @quarantine_after),
              det: Map.update(st.det, swarm, %{}, &Map.delete(&1, mod))
          }

          {st, [quarantine_alert(swarm, mod, now) | alerts]}

        true ->
          {%{st | quarantine: Map.update(st.quarantine, key, 1, &(&1 + 1))}, alerts}
      end
    end)
  end

  defp quarantine_alert(swarm, mod, now) do
    %{
      type: :detector_quarantined,
      swarm: swarm,
      at_ms: now,
      summary:
        "detector #{inspect(mod)} failed #{@quarantine_after} consecutive ticks — " <>
          "disabled for #{swarm}, state dropped; restart the observer to re-enable",
      evidence: %{"module" => inspect(mod), "consecutive_failures" => @quarantine_after},
      key: {swarm, :detector_quarantined, mod},
      cids: [],
      source: __MODULE__
    }
  end

  # :digest — cards planned vs delivered. Zero planned cards is a healthy
  # no-op run (the digest machinery did its job: nothing to send).
  defp record_digest_health(state, swarm, planned, planned, now),
    do: record_health(state, swarm, :digest, now, :ok)

  defp record_digest_health(state, swarm, planned, delivered, now) do
    record_health(
      state,
      swarm,
      :digest,
      now,
      {:error, "planned #{planned} card(s), delivered #{delivered}"}
    )
  end

  # :sender — the raw deliver_fn outcome, shared by alert cards and digest
  # cards; the MOST RECENT outcome wins. An empty batch never ran the
  # sender, so it doesn't touch the stage.
  defp record_sender_batch(state, _swarm, [], _now), do: state

  defp record_sender_batch(state, swarm, results, now),
    do: record_sender_result(state, swarm, List.last(results), now)

  defp record_sender_result(state, swarm, :ok, now),
    do: record_health(state, swarm, :sender, now, :ok)

  defp record_sender_result(state, swarm, other, now) do
    record_health(
      state,
      swarm,
      :sender,
      now,
      {:error, "deliver_fn returned #{health_error(other)}"}
    )
  end

  defp record_health(state, swarm, stage, now, outcome) do
    if is_binary(swarm) and Map.has_key?(state.registry, swarm) do
      swarm_health = Map.get(state.health, swarm, %{})
      entry = Map.get(swarm_health, stage, %{last_success_ms: nil, last_error: nil})

      entry =
        case outcome do
          :ok -> %{entry | last_success_ms: now, last_error: nil}
          {:error, reason} -> %{entry | last_error: health_error(reason)}
        end

      %{state | health: Map.put(state.health, swarm, Map.put(swarm_health, stage, entry))}
    else
      # System-level alerts (e.g. :store_rollback) carry the observer's own
      # swarm name — never mint a phantom key in the per-observed-swarm map.
      state
    end
  end

  defp health_error(reason) when is_binary(reason), do: String.slice(reason, 0, @health_error_cap)
  defp health_error(reason), do: reason |> inspect() |> String.slice(0, @health_error_cap)

  # Compact per-swarm summary for the dashboard extension: healthy unless
  # some stage carries a live last_error. Never-run stages simply aren't
  # in the map, so they count as healthy.
  defp health_summary(health) do
    Map.new(health, fn {swarm, stages} ->
      failing = for {stage, %{last_error: err}} <- stages, err != nil, do: stage
      {swarm, %{ok: failing == [], failing: Enum.sort(failing)}}
    end)
  end

  # F4: feed back emission into the owning detector's state, so re-fire
  # guards reflect delivery, not generation. `source` is runner-stamped
  # provenance — never detector-supplied — so this can't be misdirected.
  defp apply_on_emitted(state, swarm, emitted) do
    Enum.reduce(emitted, state, fn alert, st ->
      mod = Map.get(alert, :source)

      # The runner stamps `source` on synthetic :detector_crashed /
      # :detector_invalid alerts too (it's the module that FAILED, not one
      # that emitted anything) — without this guard a module's on_emitted/2
      # could be invoked with an alert it never produced.
      if alert.type not in [:detector_crashed, :detector_invalid] and
           is_atom(mod) and Code.ensure_loaded?(mod) and function_exported?(mod, :on_emitted, 2) do
        update_in(st.det[swarm], fn
          nil -> nil
          per_swarm -> Map.update(per_swarm, mod, nil, &safe_on_emitted(mod, &1, alert))
        end)
      else
        st
      end
    end)
  end

  # A raising on_emitted must not take the tick down — keep prior state.
  defp safe_on_emitted(mod, det_state, alert) do
    mod.on_emitted(det_state, alert)
  rescue
    _ -> det_state
  catch
    _, _ -> det_state
  end

  defp emit_alert(state, alert, entry, now) do
    result =
      Outbox.send_card(
        state.deliver_fn,
        state.sender,
        state.name,
        state.alert_conversation_id,
        # state.alerts = the recently-emitted list — lets the card correlate
        # (an unanswered request minutes after an endpoint_down reads "restart").
        Outbox.alert_card(alert, entry, state.alerts)
      )

    state = maybe_escalate(state, alert, now)

    state = record_sender_result(state, alert.swarm, result, now)

    alerts = Enum.take([alert | state.alerts], @alerts_kept)

    %{
      state
      | alerts: alerts,
        # Relay budgets live only as long as their alert INSTANCE. Two
        # prunes, both needed:
        # - delete-on-emit: THIS alert already passed cooldown, so it is a
        #   fresh instance and starts its budget clean. Without it, an
        #   exhausted same-key alert lingering in `alerts` (quiet system —
        #   the cap-50 trim may not scroll it out for days) would bequeath
        #   its spent count to the re-alert and starve it of diagnosis
        #   reads. Bounded: at most #{@relay_budget_per_alert} reads per
        #   cooldown window, since only a cooled-down key can re-emit.
        # - Map.take: counts whose alert scrolled out of the just-trimmed
        #   list die with it, so the map never outgrows `alerts`.
        relay_counts:
          state.relay_counts
          |> Map.delete(alert_key(alert))
          |> Map.take(Enum.map(alerts, &alert_key/1))
    }
  end

  defp maybe_escalate(%{escalate_to: nil} = state, _alert, _now), do: state

  defp maybe_escalate(state, alert, now) do
    key = escalation_key(alert)
    cooldown_ms = state.cooldown_minutes * 60_000

    case Map.get(state.last_alert, key) do
      last_ms when is_integer(last_ms) and now - last_ms < cooldown_ms ->
        state

      _ ->
        Outbox.escalate(state.deliver_fn, state.escalate_to, state.name, alert)
        %{state | last_alert: Map.put(state.last_alert, key, now)}
    end
  end

  defp escalation_key(alert), do: {:escalation, alert.swarm, alert.type}

  # ── agent-facing reads ────────────────────────────────────────────────────

  defp read_remote(kind, swarm, state) do
    case Map.get(state.registry, to_string(swarm)) do
      nil ->
        {:reply, Jason.encode!(%{ok: false, error: "swarm #{swarm} is not observed"}), state}

      entry ->
        token = Ingest.resolve_token(entry)
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

  # ── diagnosis relay (O6): cid-bound, audited transcript read ─────────────
  #
  # Trust gate already passed (`read_sources`, in `handle_message/3`). From
  # here it's fail-closed on the shape of `state.alerts`, in order:
  #   1. swarm must be in the registry
  #   2. an alert for THAT swarm, of a built-in type, fresher than 60 min,
  #      naming this cid, must still be in `state.alerts`
  #   3. that alert's per-key relay budget (3) must not be exhausted
  # An unrecognized/malformed alert shape (missing :cids, non-atom :type,
  # etc.) simply fails to match the eligibility filter below — deny, never
  # raise. Every attempt is logged before replying, allowed or not.
  defp relay_session_history(from, swarm, cid, state) do
    swarm_s = to_string(swarm)
    cid_s = to_string(cid)
    now = state.now_fn.()

    case Map.get(state.registry, swarm_s) do
      nil ->
        deny_relay(state, now, from, swarm_s, cid_s, "swarm #{swarm_s} is not observed")

      entry ->
        case eligible_relay_alert(state, swarm_s, cid_s, now) do
          nil ->
            deny_relay(
              state,
              now,
              from,
              swarm_s,
              cid_s,
              "no fresh built-in alert for swarm #{swarm_s} names cid #{cid_s}"
            )

          alert ->
            key = alert_key(alert)
            count = Map.get(state.relay_counts, key, 0)

            if count >= @relay_budget_per_alert do
              deny_relay(
                state,
                now,
                from,
                swarm_s,
                cid_s,
                "relay budget exhausted for this alert (max #{@relay_budget_per_alert} relays)"
              )
            else
              allow_relay(state, now, from, swarm_s, cid_s, entry, key)
            end
        end
    end
  end

  defp eligible_relay_alert(state, swarm, cid, now) do
    Enum.find(state.alerts, fn alert ->
      to_string(alert.swarm) == swarm and
        MapSet.member?(@builtin_relay_types, alert.type) and
        MapSet.member?(@builtin_detector_modules, Map.get(alert, :source)) and
        alert.at_ms <= now and
        now - alert.at_ms <= @relay_window_ms and
        cid in Map.get(alert, :cids, [])
    end)
  end

  # The transcript itself is relayed verbatim-ish to the diagnosis agent
  # (never sanitized here — it isn't going to Telegram, and the escalation
  # prompt's warning is the mitigation) but is NEVER written into
  # `relay_log`, which carries call metadata only.
  defp allow_relay(state, now, from, swarm, cid, entry, key) do
    token = Ingest.resolve_token(entry)

    {reply, spent} =
      case safe_client(state, :get_session_history, [
             entry["dashboard_url"],
             swarm,
             cid,
             token,
             state.client_opts
           ]) do
        {:ok, history} -> {%{ok: true, history: history}, true}
        {:error, reason} -> {%{ok: false, error: inspect(reason)}, false}
      end

    # F7: the budget bounds how many TRANSCRIPTS an agent can read per alert
    # — a failed fetch delivered nothing, so it spends nothing. Transient
    # endpoint errors during an incident (exactly when diagnosis runs) must
    # not lock the agent out until the alert ages out.
    state =
      state
      |> log_relay(now, from, swarm, cid, spent, if(spent, do: nil, else: "fetch failed"))
      |> then(fn st ->
        if spent,
          do: Map.update!(st, :relay_counts, &Map.update(&1, key, 1, fn c -> c + 1 end)),
          else: st
      end)

    {:reply, Jason.encode!(reply), state}
  end

  defp deny_relay(state, now, from, swarm, cid, reason) do
    state = log_relay(state, now, from, swarm, cid, false, reason)
    {:reply, Jason.encode!(%{ok: false, error: reason}), state}
  end

  defp log_relay(state, now, from, swarm, cid, allowed, reason) do
    entry = %{
      at_ms: now,
      from: to_string(from),
      swarm: swarm,
      cid: cid,
      allowed: allowed,
      reason: reason
    }

    %{state | relay_log: Enum.take([entry | state.relay_log], @relay_log_kept)}
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
         end),
       relay_log: state.relay_log,
       health: state.health,
       # Once quarantined, a module vanishes from det_health entirely (it no
       # longer runs), so health alone reads "ok" and the trace would be
       # lost — status must still surface it: a restart is the only lever
       # that resets a quarantine streak.
       quarantine:
         Map.new(state.quarantine, fn {{s, m}, n} -> {"#{s}/#{inspect(m)}", n} end)
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

  defp store_module_ref!(nil, default), do: default
  defp store_module_ref!("", default), do: default
  defp store_module_ref!(mod, _default) when is_atom(mod), do: mod

  defp store_module_ref!(name, _default) when is_binary(name) do
    mod =
      name
      |> String.trim()
      |> String.trim_leading("Elixir.")
      |> String.split(".")
      |> Module.concat()

    if Code.ensure_loaded?(mod) do
      mod
    else
      raise ArgumentError, "store_mod #{name} could not be resolved to a loaded module"
    end
  end

  # ── custom detectors (O5) ────────────────────────────────────────────────
  #
  # `custom_detectors` config: a list of entries, each either a bare module
  # (atom from an Elixir swarm def, or "Elixir.Some.Module" string — global,
  # runs for every swarm) or `%{module: mod_ref, swarms: [name, ...]}`
  # (scoped to those swarm names; `swarms: nil` or `[]` is also global).
  # Resolution is fail-CLOSED: unlike `module_ref/2` (used for `client`/
  # `store_mod`, which fall back to a safe default), an unresolvable module
  # or one missing `detect/2` RAISES here — this key is the operator's
  # explicit allowlist for third-party code sharing the observer's trust
  # boundary, so a typo silently dropping a detector is worse than a boot
  # crash that names the offending module.

  defp detectors_for(state, swarm) do
    customs =
      state.custom_detectors
      |> Enum.filter(&custom_detector_scoped?(&1, swarm))
      |> Enum.map(& &1.module)

    Enum.reject(state.detectors ++ customs, fn mod ->
      Map.get(state.quarantine, {swarm, mod}, 0) >= @quarantine_after
    end)
  end

  defp custom_detector_scoped?(%{swarms: nil}, _swarm), do: true
  defp custom_detector_scoped?(%{swarms: []}, _swarm), do: true
  defp custom_detector_scoped?(%{swarms: swarms}, swarm), do: swarm in swarms

  defp resolve_custom_detectors!(entries) when is_list(entries) do
    Enum.map(entries, &resolve_custom_detector!/1)
  end

  defp resolve_custom_detectors!(other) do
    raise ArgumentError,
          "custom_detectors: expected a list, got #{inspect(other)}"
  end

  defp resolve_custom_detector!(entry) when is_map(entry) do
    mod_ref = entry_get(entry, :module)
    swarms = entry_get(entry, :swarms)
    %{module: resolve_detector_module!(mod_ref), swarms: normalize_custom_swarms!(swarms)}
  end

  defp resolve_custom_detector!(mod_ref) when is_binary(mod_ref) or is_atom(mod_ref) do
    %{module: resolve_detector_module!(mod_ref), swarms: nil}
  end

  defp resolve_custom_detector!(other) do
    raise ArgumentError,
          "custom_detectors: invalid entry #{inspect(other)} — expected a module " <>
            "(atom or \"Elixir.Some.Module\" string) or %{module: ref, swarms: [...]}"
  end

  defp normalize_custom_swarms!(nil), do: nil
  defp normalize_custom_swarms!([]), do: nil

  defp normalize_custom_swarms!(list) when is_list(list) do
    Enum.map(list, &to_string/1)
  end

  defp normalize_custom_swarms!(other) do
    raise ArgumentError,
          "custom_detectors: swarms must be a list of swarm names, got #{inspect(other)}"
  end

  defp resolve_detector_module!(nil) do
    raise ArgumentError, "custom_detectors: entry is missing its required :module key"
  end

  defp resolve_detector_module!(mod) when is_atom(mod) do
    ensure_detector_module!(mod, inspect(mod))
  end

  defp resolve_detector_module!(name) when is_binary(name) do
    mod =
      try do
        String.to_existing_atom("Elixir." <> String.trim_leading(name, "Elixir."))
      rescue
        ArgumentError ->
          raise ArgumentError,
                "custom_detectors: module #{inspect(name)} could not be resolved (not " <>
                  "loaded/known) — check the module name, and that the package providing it " <>
                  "is vendored (fail-closed: this key is an explicit allowlist, so a typo must " <>
                  "not silently drop a detector)"
      end

    ensure_detector_module!(mod, inspect(name))
  end

  defp resolve_detector_module!(other) do
    raise ArgumentError,
          "custom_detectors: module must be a string or atom, got #{inspect(other)}"
  end

  defp ensure_detector_module!(mod, label) do
    Code.ensure_loaded(mod)

    unless function_exported?(mod, :detect, 2) do
      raise ArgumentError,
            "custom_detectors: module #{label} (#{inspect(mod)}) does not export detect/2 — " <>
              "not a valid Genswarms.Observer.Detector"
    end

    mod
  end

  # Boot-time-only slice of threshold validation: two detector modules
  # (built-in or custom) declaring the SAME `default_thresholds/0` key is
  # unambiguously a config bug (whichever module's default wins is
  # accidental), so it raises here. Cannot check the complementary case —
  # a threshold key referenced by a detector but declared by none — that
  # would require executing every detector, which init/1 does not do.
  defp check_threshold_collisions!(modules) do
    key_owners =
      modules
      |> Enum.uniq()
      |> Enum.reduce(%{}, fn mod, acc ->
        Code.ensure_loaded(mod)

        if function_exported?(mod, :default_thresholds, 0) do
          mod.default_thresholds()
          |> Map.keys()
          |> Enum.reduce(acc, fn key, acc -> Map.update(acc, key, [mod], &[mod | &1]) end)
        else
          acc
        end
      end)

    collisions = for {key, mods} <- key_owners, length(mods) > 1, into: %{}, do: {key, mods}

    if map_size(collisions) > 0 do
      details =
        collisions
        |> Enum.map_join("; ", fn {key, mods} ->
          "#{inspect(key)} declared by #{Enum.map_join(mods, ", ", &inspect/1)}"
        end)

      raise ArgumentError,
            "observer boot: default_thresholds/0 key collision across detector modules: #{details}"
    end
  end

  # ── signal_rules (Task 6): operator health_rules, fail-CLOSED ───────────
  #
  # Config key `signal_rules`: a flat list of rule maps, each carrying its
  # own `"block"` key (the dashboard extension key it evaluates against)
  # plus the v1 rule grammar (id/severity/card/each/where/when — see
  # README "Health rules (declarative)"). Grouped by block here and
  # validated per-group via `Signals.validate_rules/1` at boot — same
  # trust class as `custom_detectors`: an operator explicitly wrote these,
  # so a malformed rule is a boot error naming it, never a silently
  # skipped rule (contrast with package-shipped rules, which are fail-SOFT
  # — see run_signals/4). Boot-only, never re-read, NEVER x-mutable.
  #
  # Entries are deep-stringified first: the engine's seed-config
  # normalization atomizes JSON map keys before they reach init/1, so an
  # operator's rules arrive atom-keyed even when written as valid JSON —
  # without this, fail-closed rejects every rule at boot.
  defp build_signal_rules!(entries) when is_list(entries) do
    entries
    |> Enum.map(&stringify_rule_keys/1)
    |> Enum.with_index()
    |> Enum.reduce(%{}, fn {entry, idx}, acc ->
      block_key = signal_rule_entry_block!(entry, idx)
      Map.update(acc, block_key, [entry], &(&1 ++ [entry]))
    end)
    |> Enum.into(%{}, fn {block_key, rules} ->
      case Signals.validate_rules(rules) do
        {:ok, validated} ->
          {block_key, validated}

        {:error, reason} ->
          raise ArgumentError,
                "signal_rules: invalid rule(s) for block #{inspect(block_key)}: #{reason}"
      end
    end)
  end

  defp build_signal_rules!(other) do
    raise ArgumentError, "signal_rules: expected a list, got #{inspect(other)}"
  end

  defp signal_rule_entry_block!(entry, idx) when is_map(entry) do
    case Map.get(entry, "block") do
      b when is_binary(b) and b != "" ->
        b

      other ->
        raise ArgumentError,
              "signal_rules: entry at index #{idx} has invalid/missing \"block\" #{inspect(other)}"
    end
  end

  defp signal_rule_entry_block!(other, idx) do
    raise ArgumentError, "signal_rules: entry at index #{idx} is not a map (#{inspect(other)})"
  end

  defp stringify_rule_keys(%{} = map) do
    Enum.into(map, %{}, fn {k, v} -> {stringify_rule_key(k), stringify_rule_keys(v)} end)
  end

  defp stringify_rule_keys(list) when is_list(list), do: Enum.map(list, &stringify_rule_keys/1)
  defp stringify_rule_keys(other), do: other

  defp stringify_rule_key(k) when is_atom(k), do: Atom.to_string(k)
  defp stringify_rule_key(k), do: k
end
