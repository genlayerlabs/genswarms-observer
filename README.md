# genswarms-observer

The swarm that watches swarms. One more genswarms: a cron ticks a `:scope`
object that reads each observed swarm's dashboard backend and runs
**deterministic detectors** (no LLM); alerts go out as Telegram cards with
evidence, a deep-link and a ready-made investigation prompt — and escalate
as diagnosis tasks to an isolated agent.

```
:cron ──tick──▶ :scope ──card──▶ :sender (genswarms-telegram)
                  │  └──escalation──▶ :diagnostico (bwrap, isolated network)
                  └── read actions for agents (status / get_dashboard / get_events)
```

## Detectors (`Genswarms.Observer.Detectors`, pure)

| type             | condition                                                        |
|------------------|------------------------------------------------------------------|
| `endpoint_down`  | the dashboard fetch fails                                        |
| `stall`          | active agents but no events for `stall_minutes` (10)             |
| `error_burst`    | ≥ `error_burst_count` (5) error events within `error_burst_window_s` (60) |
| `budget_block`   | `llm_proxy_global_block` seen in events                          |
| `pool_saturated` | `leased == size` sustained for `pool_saturated_s` (120)          |

Dedupe + cooldown per `(swarm, type)` in `:scope` (`cooldown_minutes`, 30).

## Principles

- Detectors are pure functions; the LLM only diagnoses (fase 3).
- Only `:scope` opens sockets; agents ask it through the topology
  (`:diagnostico` runs with `network: :isolated` — nothing but the LLM
  router forwarder).
- Tokens as env var **names** (`token_env`) — never literals.
- `registry` and `thresholds` are `x-mutable` (hot-patch from the
  configurator); allowlists (`tick_sources`, `read_sources`,
  `escalate_to`) are NOT.
- The model is never named: `:diagnostico` carries a Σ_pol routing policy
  to the unhardcoded router; context sealing is the router's auto-compaction
  (`compact_extra`).

## Usage

See `observer.swarm.exs` (cron + scope + sender + diagnostico + optional
dashboard) and `swarm-object.json` (the `:scope` config schema). Live:

```bash
GENSWARMS_PATH=/path/to/engine \
SUBZEROCLAW_PATH=/path/to/subzeroclaw/subzeroclaw \
GENSWARMS_ALLOWED_ENDPOINTS=router.ygr.ai \
UNHARDCODED_CONSUMER_KEY=llmr_... \
OBSERVER_TELEGRAM_BOT_TOKEN=... \
OBSERVER_ALERT_CONVERSATION_ID=tg:...:0 \
mix run run_live.exs
```

Tests: `mix test` (detectors + scope + schema↔init conformance).
Network-less boot smoke: `GENSWARMS_PATH=... mix run tests/boot_smoke.exs`.

## Wire contract

Dashboard/events shapes are the genswarms-dashboard backend's golden
contract (`backend/README.md`) — do not invent keys.
