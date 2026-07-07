# Changelog

## Unreleased

- Fase 3 v1: `:diagnostico` bwrap body with isolated network (Σ_pol routing
  to the unhardcoded router, router-driven auto-compaction) + alert
  escalation from `:scope` as diagnosis tasks. Requires engine ≥ #79 for the
  bwrap workspace/seed fixes.
- Repo language normalized to English.

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
