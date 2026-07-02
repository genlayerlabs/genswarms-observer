# genswarms-observer

A swarm that observes swarms. **Design in progress — v0.2; the v0.1 prototype was discarded.**

Intended shape (a `kind: swarm` swarmidx package, not a handler):

- Reads a target swarm's live story from the [genswarms-dashboard](https://github.com/genlayerlabs/genswarms-dashboard)
  feed (HTTP/WS, consumer token) — sessions, events, KPIs, issues.
- Spots problems (stalls, error bursts, budget exhaustion, dead routes) and helps
  manage the swarm: diagnoses, proposals, and PRs against the swarm's repo.
- Acts THROUGH agents (its own swarm IR: bodies + skills + topology), which is what
  makes it a package rather than an external client — see the package criterion in
  the [gsp design doc §6.1](https://github.com/genlayerlabs/genswarms-packages/blob/main/gsp-design-doc.md).

Nothing to run yet. The design lands here as it firms up.
