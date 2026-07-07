# Changelog

## Unreleased

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
