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

## Custom detectors (boot-only)

`custom_detectors` registers third-party `Genswarms.Observer.Detector`
modules alongside the built-ins — a bare module (global, runs for every
observed swarm) or `%{module: mod, swarms: ["mm"]}` to scope it to specific
swarms. Package detectors are vendored via
`gsp vendor swarmidx:<scope>/<pkg-detector>@<ver>` — digest-pinned by the
notary; referencing a module here is the explicit allowlist. In-process
code shares the observer's trust boundary (spec §5.2) — it runs with the
same privileges as the built-in detectors, so only vendor modules you trust
as much as this codebase itself.

Resolved once at `init/1` and never again: this key is **not** `x-mutable`
(see `swarm-object.json`), and an unresolvable module or one missing
`detect/2` raises at boot rather than being silently skipped — loading
third-party code is a deliberate operator decision, and a typo here should
fail loudly, not quietly drop a detector. Boot also raises if two detector
modules (built-in or custom) declare the same `default_thresholds/0` key.

## Health rules (declarative)

Packages can ship their own alerting logic without the observer knowing
anything about their business — a dashboard extension block just adds a
`"health_rules"` key next to its data, in a small structured (JSON, not a
string DSL) grammar that `Genswarms.Observer.Signals` evaluates generically.
Operators can add their own rules too, via the `signal_rules` config. This
section is the wire contract a package author (or operator) needs to ship
one.

### Grammar (v1)

```
rule    = { "id": <=32 chars [a-z0-9_], "severity": "warn" (default) | "info",
            "card": <=200 chars, template with {field} placeholders,
            "each": optional block-relative path to a LIST,
            "where": optional cond, "when": required cond }

cond    = { "op": "gt" | "gte" | "lt" | "lte" | "eq" | "neq",
            "lhs": operand, "rhs": operand }

operand = number
        | "now"
        | { "lit": <any> }
        | { "path": "dot.path" }    # item-relative under "each", else block-relative
        | { "delta": "dot.path" }   # ALWAYS block-relative, even inside "each"
        | { "add" | "sub" | "mul" | "div": [operand, operand] }
```

- `"each"` runs the rule once per item of the list it resolves to (block-
  relative), evaluating `"where"`/`"when"` with paths resolved against the
  ITEM first; without `"each"`, `"where"`/`"when"` evaluate against the
  block itself (see llm-proxy's `budget_guard_75`: `"where"` guards on
  `ceiling_usd > 0` with no `"each"` at all).
- `"where"` is an optional pre-filter; `"when"` is the firing condition.
  Both are `cond`s.
- `{field}` in `"card"` interpolates `to_string(item[field] or block[field])`
  (item wins), missing ⇒ `"?"`. The rendered summary is sliced to 200
  chars.
- `alert.key = {swarm, :health_rule, block_key, rule_id, item_key}`, where
  `item_key = item["name"] || item["id"] || nil` — this is what dedupe/
  cooldown key on, same as every other detector alert.

### Two sources, different trust

- **Package-shipped** (`block["health_rules"]`, inside the dashboard
  extension the OBSERVED swarm publishes — remote, untrusted data): **fail-
  SOFT**. A malformed block's rules are dropped for that tick and the
  block is named in the `:signals` per-stage health error; every OTHER
  block still evaluates normally.
- **Operator-configured** (`signal_rules` config on `:scope`, each entry
  carrying its own `"block"` key naming the extension key it targets):
  **fail-CLOSED**, same trust class as `custom_detectors`. An invalid rule
  raises at `init/1`, naming it — a typo in your own config should fail
  loudly at boot, not silently vanish.

### Bounds and absence-tolerance

At most 16 rules per block, 32 cond/operand nodes per rule (recursive,
`"where"` + `"when"` combined) — both enforced by
`Signals.validate_rules/1`. Evaluation is absence-tolerant throughout: a
missing `"path"`, a non-numeric arithmetic operand, division by zero, or a
first-sight `"delta"` all make THAT rule no-op silently — never raise,
never false-alarm. `"eq"`/`"neq"` compare any terms (including `nil` via
`{"lit": null}` — see the `poller_deaf` example below); the ordered
operators (`gt`/`gte`/`lt`/`lte`) require both sides numeric. Numeric
strings are never coerced: a block publishing a number as a string is a
producer bug, treated the same as absence.

The observer additionally bounds a block's OWN misbehavior at the Scope
integration layer (not inside `Signals`, which stays a pure evaluator): at
most 10 alerts per `(block, rule_id)` per tick (a block with a huge or
hostile `"each"` list can't mint unbounded cooldown keys — the per-swarm
alert budget downstream would coalesce any real overflow anyway), and a
string `item_key` is sliced to 64 chars before it becomes part of a key.

### The `"../"` host-only escape

A `"path"`/`"delta"` string starting with `"../"` resolves the remainder
against the WHOLE `extensions` map instead of the item/block. Only
HOST-published blocks use this (wingston is the envelope owner — same
trust domain as the extensions map itself); package-shipped rules never
span blocks, only their own.

### Worked example: cron's `missed_tick`

Verbatim from `genswarms-objects`' `packages/cron/cron.ex` (the `"cron"`
machine block, see the plan's Task 1):

```json
{
  "id": "missed_tick",
  "severity": "warn",
  "card": "cron job {name} did not run (overdue past grace)",
  "each": "jobs",
  "where": {"op": "eq", "lhs": {"path": "state"}, "rhs": {"lit": "active"}},
  "when": {"op": "gt", "lhs": "now",
           "rhs": {"add": [{"path": "next_run_at_ms"}, 1800000]}}
}
```

For every job in `cron.jobs` whose `state == "active"` (the `"where"`
filters out paused/running-once jobs), fire once `now > next_run_at_ms +
30 minutes` — one alert per overdue job, `item_key` = the job's `name`.

### `rules_gone`: the sovereign check

`Signals` only evaluates whatever rules are handed to it — it has no
opinion on whether a block that used to publish `"health_rules"` still
does. `:scope` watches that independently: any PACKAGE block seen
publishing `"health_rules"` on a prior tick that is **absent** (not just
malformed — genuinely missing from the envelope) for **2 CONSECUTIVE
dashboard-fetched-ok ticks** fires a `:health_rules_gone` alert naming the
block. The 2-tick debounce exists because "block absent" is not the same
as "component broken": a restarting process (telegram's poller included)
can leave its block briefly missing for a few seconds without that being
page-worthy on the very first miss. A dashboard fetch error/`:unavailable`
tick resets nothing and counts nothing — it is treated as if the tick
never happened for this purpose, same no-verdict discipline as every other
stage. `rules_gone` only tracks package-published blocks; a `signal_rules`
operator entry's target block never "goes away" in this sense — it's
static config, not remote state.

### Caveat: delta rules under a failing `"where"`

`eval_item`'s `"where"` guard short-circuits `"when"` entirely — if a
`"delta"` operand lives inside `"when"` and `"where"` is false this tick,
the delta is never evaluated, so its current sample is NOT recorded (only
a `"delta"` operand that actually gets evaluated updates `samples` — see
`Signals.eval_delta/6`). A delta rule gated by a flaky `"where"` can
therefore see gaps in its own sample history that have nothing to do with
the block being absent. Keep delta rules `"where"`-less (like
`poll_conflict`), or fold the guard INTO the `"when"` cond (e.g. via
`"add"`/nested `cond`-style tricks) if you need one — don't put a delta
operand behind a separate `"where"`.

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
