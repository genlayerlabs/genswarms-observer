# Changelog

## v0.2.0 — 2026-07-05

Primera versión funcional (v0.1 fue diseño).

- `Genswarms.Observer.Detectors`: detectores deterministas puros
  (`endpoint_down`, `stall`, `error_burst`, `budget_block`,
  `pool_saturated` con sostenimiento vía det_state).
- `Genswarms.Observer.Objects.Scope`: registry x-mutable de swarms
  observados, tick de cron, dedupe+cooldown por (swarm, tipo), alert cards
  a genswarms-telegram, lecturas agent-facing (`status` / `get_dashboard` /
  `get_events`) tras allowlist fail-closed.
- Seam `Client` (`Http` via :httpc / `Fake` para tests) — tokens como
  nombres de env vars, resueltos en fetch-time.
- `swarm-object.json` + test de conformance schema↔init.
- `observer.swarm.exs` + `run_live.exs` + boot smoke con fakes.
