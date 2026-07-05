# genswarms-observer — handoff de implementación (v0.2)

Estado: DISEÑO CERRADO, implementación pendiente. Todo el sustrato que el
observer necesita ya existe y está publicado (2026-07-05). Este documento es
el plan para programarlo — con el MCP `genswarms-fleet` conectado en Claude
Code para verificar en vivo contra swarms reales.

## Arquitectura final

```
observer swarm (un genswarms más; el swarm que vigila swarms)
│
├── :cron        Genswarms.Cron (genswarms-objects v0.1.7, ya publicado)
│                seed job cada 5 min → {"action":"tick"} a :scope
│                allowlists fail-closed: allowed_targets: %{scope: ["tick"]}
│
├── :scope       ★ EL OBJETO A ESCRIBIR (lib/genswarms/observer/objects/scope.ex)
│                - registry de swarms observados (config, x-mutable):
│                    %{"wingston" => %{dashboard_url: ..., token_env: "WINGSTON_DASH_TOKEN",
│                                      repo: "genlayerlabs/wingston-rally-bot"}}
│                  tokens como NOMBRES de env vars (contrato x-secret §14.2.1)
│                - en cada tick: GET /dashboard + /events de cada swarm
│                - DETECTORES DETERMINISTAS (funciones puras, sin LLM):
│                    stall (sin eventos en N min con agentes activos),
│                    error_burst (≥K llm_error/min), budget_block
│                    (llm_proxy_global_block visto), endpoint_down (fetch
│                    falla), pool_saturated (leased == size sostenido)
│                - dedupe + cooldown por (swarm, tipo) — patrón roster de
│                  wingston; sin esto hay tormentas de alertas
│                - alerta → :sender como card: swarm, tipo, evidencia,
│                  deep-link al dashboard y PROMPT DE INVESTIGACIÓN listo
│                  ("conecta genswarms-fleet y corre get_events(X, error)")
│                - acciones agent-facing (para la fase 3): snapshot/events/
│                  config de cualquier swarm observado vía swarm-msg ask
│
├── :sender      genswarms-telegram v0.4.0 (publicado, log #52) — Objects.Sender
│                bot_token_env (nunca el token literal)
│
└── fase 3       :diagnostico — agente bwrap network: :isolated que recibe la
                 escalada, pregunta a :scope (nunca abre sockets), redacta
                 diagnóstico; PRs vía Claude Code headless con el MCP + gh
```

Principios no negociables (los del ecosistema):
- detectores = objetos deterministas; el LLM solo diagnostica (escalada)
- el agente jamás ve tokens ni red — :scope es el único que hace HTTP
- config del scope con config_schema: registry x-mutable (añadir un swarm a
  vigilar en caliente desde el configurador), umbrales x-mutable, allowlists NO
- todo reusable como paquete: swarmidx.json kind:swarm cuando esté maduro

## Plan de implementación (orden)

1. `mix new` shape de paquete (copiar patrón de genswarms-email: mix.exs con
   sibling_or_github para genswarms_objects v0.1.7 + genswarms_telegram
   v0.4.0 + jason; sin dep de compilación del engine).
2. `lib/genswarms/observer/detectors.ex` — PURO: `detect(snapshot, events,
   thresholds) :: [alert]`. Tests exhaustivos con fixtures (los shapes del
   wire contract están en el README del backend del dashboard).
3. `lib/genswarms/observer/objects/scope.ex` — ObjectHandler por convención
   (sin @behaviour). Client seam: `Client.Http` + `Client.Fake` (calcar
   fleet-mcp/test/fake-fleet.mjs pero en Elixir, o un Agent con fixtures).
   Acciones: tick (de cron), status, y las de lectura para agentes.
4. `swarm-object.json` con config_schema + test de conformance schema↔init
   (calcar genswarms-email/test/config_schema_test.exs).
5. `observer.swarm.exs` + `run_live.exs` (calcar bitprime-swarm).
6. Boot smoke (calcar bitprime-swarm/tests/boot_smoke.exs) con Telegram
   Client.Fake y un target fake.
7. EN VIVO con el MCP: `dev/start-target.sh` levanta engine+swarm objetivo;
   arrancar el observer apuntándole; usar las tools genswarms-fleet
   (get_dashboard/get_config/get_events) para ver AMBOS swarms; matar el
   target → verificar alerta endpoint_down; provocar error_burst con
   send_task; patch de umbrales por patch_object_config y ver el overlay.
8. Publicar: tag v0.2.0 + `gsp publish` (token en
   ~/docs/personal/genswarms-email/.env; publicar SIEMPRE desde clone limpio
   del tag — un .env local corrompe el dirhash).

## Entorno para la sesión

- MCP `genswarms-fleet` YA registrado en ~/.claude.json (scope user); flota
  en ~/.config/genswarms/fleet.json. Tools visibles con /mcp.
- `dev/start-target.sh` (este repo): engine REST :4000 (tokens
  fleet-full-token / fleet-config-token) + swarm de prueba
  jmlago-genswarms-fleet-mcp (dashboard :4994, objetos require-mode).
- Engine: usar SIEMPRE el checkout ~/docs/personal/genswarms con
  `mix run -e 'Genswarms.Application.start_web_server(port: 4000)'` — el
  escript NO carga NIFs (exqlite) y `genswarms.up` daemoniza sin env.
- PR pendiente de merge: genlayerlabs/genswarms#77 (fixes ref-map handlers;
  el checkout local ya la lleva en la rama fix/schema-ref-handlers).

## Gotchas conocidos (no re-descubrir)

- Paths con `~` NO se expanden en env vars leídas por Elixir — absolutos.
- El backend :local hereda el env del BEAM (PATH/OUTBOX_DIR/ASK_REPLY_DIR
  se preparan en run_live.exs — ver bitprime-swarm).
- `gsp vendor` REESCRIBE vendor-lock.json: pasar TODOS los refs en una
  invocación.
- jq no está instalado en esta máquina (tests de reply.sh fallan por eso).
- El wire del dashboard: campos y shapes pinneados por el golden contract
  test — no inventar keys, leerlos de backend/README.md.
