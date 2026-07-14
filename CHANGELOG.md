# Changelog

## Unreleased

## v0.3.0 — 2026-07-14

- Registry wire-name override: entry key `name` — the swarm name the
  observed BACKEND answers to, when it differs from the registry key
  (key `wingston-prod`, wire name `wingston`: two deployments of one swarm
  need distinct keys but each backend only knows its own name). All fetch
  paths (dashboard/events/feed/session history) go through
  `Ingest.wire_name/2`; the registry key stays the observer-side identity
  (alert titles, dedupe, relay eligibility). Until now the key doubled as
  the wire name and `normalize_entry` silently DROPPED `name` — a
  prod entry keyed by an alias fetched a wrong-name dashboard URL, which
  tolerant backends answered with a stub envelope (dashboard-based
  detectors, signals and digests silently blind) and strict backends
  (genswarms-dashboard ≥ 0.3.8) answer with 404 (`endpoint_down` noise).
- LLM spend burn-rate detector: new builtin
  `Genswarms.Observer.Detectors.LlmSpend` samples the envelope's cumulative
  daily spend (default `extensions.llm_proxy_budget.spent_usd`, retarget
  via `llm_spend.path`) and raises `:llm_spend_spike` (investigable) when
  the last `llm_spend.window_s` costs ≥ `llm_spend.factor` × the swarm's
  own trailing per-window baseline and ≥ `llm_spend.min_usd` — catches a
  runaway loop while it's still cheap, complementing the llm-proxy
  package's 75%/90%-of-ceiling rules. Reset-aware across midnight
  rollovers; silent during warm-up (`llm_spend.min_baseline_windows`).
- Daily ops digest: new boot-only `ops_digest` config
  (`OBSERVER_OPS_DIGEST_JSON`) renders one card per swarm per day at/after
  `hour_utc` straight from the envelope — `"block"` sections read scalar
  extension keys, `"page_row"` sections read one row of a
  `dashboard_pages` table (`latest_closed` = yesterday's durable final).
  Mark-after-send per swarm (persisted via `store_mod`), fail-closed
  config validation at boot, remote strings sanitized like topic labels.
- Recovery hint on restart-dropped users: new per-swarm registry key
  `recover_hint` — a restart-correlated `unanswered` card now renders the
  operator's template with `{cid}` substituted (e.g. `/reach {cid} …`), so
  the users whose replies died with the old pod can be reached from the
  card itself.
- Observability of the observer: each alerting tick logs
  `[observer] alerts swarm=<name> sent=<type:count,...> suppressed=<n>`
  and each ops digest delivery logs its day — the send history
  reconstructs from the log alone (it used to say only "sender received
  message from scope").
- POSITIVE restart detection: new builtin detector
  `Genswarms.Observer.Detectors.Restarted` consumes the host's
  `feed_rehydrated` display event (emitted exactly once per pod boot by the
  dashboard package's DisplayFeed) — until now restarts were only INFERRED
  from unreachability blips (`endpoint_down` + "swarm_not_found"), so a fast
  rollout between two ticks was invisible. Emits `:swarm_restarted` (quiet
  human card — deploy hint, rehydrated row count, no investigate tail) and
  escalates to `:restart_loop` (investigable) at `>= restart.loop_count`
  boots within `restart.loop_window_s`. Fresh-window gating
  (`restart.fresh_window_s`) keeps ring replays of old boots (newly
  registered swarms, observer restarts) silent; state is `{seq, ts}`-deduped
  (a restarted host may reuse a feed sequence) and pruned, same discipline as
  `DeliveryFailureBurst`. The `unanswered`
  correlation ("their reply died with the old pod") now also matches the
  positive detection, not just the endpoint_down inference. Hosts whose
  feed never carries `feed_rehydrated` simply never fire it.
- Fase 3 v1: `:diagnostico` bwrap body with isolated network (Σ_pol routing
  to the unhardcoded router, router-driven auto-compaction) + alert
  escalation from `:scope` as diagnosis tasks. Requires engine ≥ #79 for the
  bwrap workspace/seed fixes.
- Repo language normalized to English.
- Signals stage (Task 6): `Genswarms.Observer.Signals` (the v1 declarative
  `health_rules` evaluator) is now wired into `:scope`'s tick, right after
  `fetch` and before `DetectorRunner.run` — its alerts join the same
  `Lifecycle.process` batch as detector/quarantine alerts. Package-shipped
  rules (`block["health_rules"]`) are fail-soft (a malformed block is
  dropped + named in a new `:signals` per-stage health entry, other blocks
  still evaluate); the new `signal_rules` config (boot-only, never
  x-mutable, grouped by `"block"`) is fail-closed like `custom_detectors`.
  Delta samples and the sovereign `rules_gone` check (a package block
  absent for 2 consecutive dashboard-ok ticks) are persisted under a new
  `:signals` store payload key (`samples`/`rules_seen`/`rules_miss`),
  validated on load. Alerts are bounded at the Scope integration layer
  (10/rule/tick, item_key sliced to 64 chars) — `Signals` itself stays a
  pure, unbounded-input-tolerant grammar evaluator. See README "Health
  rules (declarative)" for the full grammar/trust/absence-tolerance
  contract.

### Changed
- Tick restructured into commit-together pipeline modules (`Ingest`,
  `Lifecycle`, `Outbox`): the feed cursor commits only when every active
  detector succeeded (F1); steady-state feed reads drain to head (F5);
  budget-dropped alerts re-fire via the new optional `on_emitted/2`
  detector callback (F4); `last_alert` is evicted past the cooldown
  window (F10).
- Detectors failing 3 consecutive ticks are quarantined (state dropped,
  one `:detector_quarantined` alert) instead of crash-looping (F2).
- Threshold overrides are type-validated against each module's defaults
  (numeric strings coerced, mismatches fall back) (F6).
- The tick reply's `suppressed` counter now counts every dropped alert
  (previously 0/1 per overflow) — dashboards/parsers watching it will see
  a step change.
- `relay_log.allowed` now records whether a transcript was actually
  SERVED: a gate-allowed but failed fetch logs `allowed: false` with
  reason "fetch failed" (previously logged `allowed: true`).

### Fixed
- `topics_stale`: no nightly false alarm in the midnight→producer-close
  gap (`topics_stale.grace_hours`, default 1) (F3); transient dashboard
  fetch errors are a no-op, not "extension missing" (F8).
- `delivery_failure_burst`: dedupe is seq-keyed — two distinct failures
  in the same millisecond both count (F9).
- Diagnosis relay budget spends only on successfully served transcripts (F7).

## v0.2.0 — 2026-07-05

First functional release (v0.1 was design only).

- `Genswarms.Observer.Detectors`: pure deterministic detectors
  (`endpoint_down`, `stall`, `error_burst`, `budget_block`,
  `pool_saturated` with sustainment via det_state).
- `Genswarms.Observer.Objects.Scope`: x-mutable registry of observed swarms,
  cron tick, dedupe+cooldown per (swarm, type), alert cards to
  genswarms-telegram, agent-facing reads (`status` / `get_dashboard` /
  `get_events`) behind fail-closed allowlists.
- `Client` seam (`Http` via :httpc / `Fake` for tests) — tokens as env var
  names, resolved at fetch time.
- `swarm-object.json` + schema↔init conformance test.
- `observer.swarm.exs` + `run_live.exs` + boot smoke with fakes.
