# Changelog

## Unreleased

- Fase 3 v1: `:diagnostico` bwrap body with isolated network (Σ_pol routing
  to the unhardcoded router, router-driven auto-compaction) + alert
  escalation from `:scope` as diagnosis tasks. Requires engine ≥ #79 for the
  bwrap workspace/seed fixes.
- Repo language normalized to English.

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
