# genswarms-observer

El swarm que vigila swarms. Un genswarms más: un cron le da tick a un objeto
`:scope` que lee el dashboard backend de cada swarm observado y corre
**detectores deterministas** (sin LLM); las alertas salen como cards de
Telegram con evidencia, deep-link y prompt de investigación listo.

```
:cron ──tick──▶ :scope ──card──▶ :sender (genswarms-telegram)
                  │
                  └── lecturas para agentes (:diagnostico, fase 3)
```

## Detectores (`Genswarms.Observer.Detectors`, puro)

| tipo             | condición                                                        |
|------------------|------------------------------------------------------------------|
| `endpoint_down`  | el fetch del dashboard falla                                     |
| `stall`          | agentes activos pero sin eventos en `stall_minutes` (10)         |
| `error_burst`    | ≥ `error_burst_count` (5) eventos error en `error_burst_window_s` (60) |
| `budget_block`   | `llm_proxy_global_block` visto en eventos                        |
| `pool_saturated` | `leased == size` sostenido `pool_saturated_s` (120)              |

Dedupe + cooldown por `(swarm, tipo)` en `:scope` (`cooldown_minutes`, 30).

## Principios

- Los detectores son funciones puras; el LLM solo diagnostica (fase 3).
- Solo `:scope` abre sockets; los agentes le preguntan por la topología.
- Tokens como **nombres** de env vars (`token_env`) — nunca literales.
- `registry` y `thresholds` son `x-mutable` (hot-patch desde el
  configurador); las allowlists (`tick_sources`, `read_sources`) NO.

## Uso

Ver `observer.swarm.exs` (cron + scope + sender + dashboard opcional) y
`swarm-object.json` (config schema de `:scope`). En vivo:

```bash
GENSWARMS_PATH=/path/al/engine \
OBSERVER_TELEGRAM_BOT_TOKEN=... \
OBSERVER_ALERT_CONVERSATION_ID=tg:...:0 \
mix run run_live.exs
```

Tests: `mix test` (detectores + scope + conformance schema↔init).
Boot smoke sin red: `GENSWARMS_PATH=... mix run tests/boot_smoke.exs`.

## Wire contract

Los shapes del dashboard/events son el golden contract del backend de
genswarms-dashboard (`backend/README.md`) — no inventar keys.
