# genswarms-observer — working notes

State: **v0.2.0 published** (`swarmidx:genlayerlabs/genswarms-observer@0.2.0`,
log #54) and **v0.3.0 prepared for release** (durable detector state,
declarative health rules, human alert cards, positive restart detection,
daily ops digests, spend-rate alerts, and isolated bwrap `:diagnostico`
escalation). Everything here runs live against real swarms via the
`genswarms-fleet` MCP.

## Architecture

```
observer swarm (one more genswarms; the swarm that watches swarms)
│
├── :cron        Genswarms.Cron — seed job ticks :scope; fail-closed
│                allowlists (allowed_targets: %{scope: ["tick"]})
├── :scope       lib/genswarms/observer/objects/scope.ex — the ONLY node
│                with network. x-mutable registry of observed swarms
│                (tokens as env var NAMES), pure detectors per tick
│                (detectors.ex: stall / error_burst / budget_block /
│                endpoint_down / pool_saturated), dedupe+cooldown per
│                (swarm, type), alert cards to :sender, escalation task to
│                :diagnostico, allowlisted read actions for agents.
├── :sender      genswarms-telegram Objects.Sender (bot_token_env)
├── :diagnostico bwrap, network: :isolated — Σ_pol to the unhardcoded
│                router, router-driven auto-compaction; asks :scope for
│                data via swarm-msg ask; writes diagnoses.
└── :dashboard   optional (live-only dep), the observer observing itself
```

Non-negotiables: detectors are pure (LLM only diagnoses); only :scope does
HTTP; tokens as env var names; registry/thresholds x-mutable, allowlists NOT.

## Environment

- MCP `genswarms-fleet` registered (user scope); fleet in
  ~/.config/genswarms/fleet.json (target :4994/:4000, observer :4996/:4002).
  Hot-reloads fleet.json; has an agent-debugging tier (list_agents /
  get_agent_history / get_agent_logs).
- `dev/start-target.sh`: engine REST :4000 + test swarm (dashboard :4994).
- Live run: see the env block in run_live.exs (GENSWARMS_PATH,
  SUBZEROCLAW_PATH, GENSWARMS_ALLOWED_ENDPOINTS, UNHARDCODED_CONSUMER_KEY —
  key lives in ~/docs/personal/strategivm/.env).
- Engine: ALWAYS the checkout ~/docs/personal/genswarms (needs #77/#78/#79,
  all merged).
- Publishing: `gsp publish swarmidx.json --version X --source
  github://genlayerlabs/genswarms-observer@vX` from a CLEAN clone of the tag
  (a local .env corrupts the dirhash); token in
  ~/docs/personal/genswarms-email/.env; gsp CLI at
  ~/docs/genlayer/genswarms-packages/cli/gsp.

## Known gotchas (do not rediscover)

- szc endpoint is the FULL URL (…/v1/chat/completions); the bare /v1 base
  dies silently. /v1/compact derives from it.
- No "model" in request_extra against the router — the Σ_pol decides.
  No max_turns either: szc's default bounds the loop; context is the
  router's compaction.
- mix.lock: the repo's lock is generated WITHOUT GENSWARMS_PATH; never
  commit the live (engine-fattened) lock.
- The engine's GenswarmsWeb.Endpoint needs adapter+secret config in THIS
  host app (config/config.exs) when the engine is a path dep.
- Paths with `~` do NOT expand in env vars read by Elixir — absolutes.
- The dashboard wire contract lives in genswarms-dashboard/backend/README.md
  — never invent keys.
- Repo language is English (published genlayerlabs package).
