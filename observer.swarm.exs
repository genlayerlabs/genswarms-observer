# observer — el swarm que vigila swarms (v0.2: cron -> scope -> sender).
#
# Sin agentes: la detección es determinista (Genswarms.Observer.Detectors);
# el LLM solo entra en fase 3 (:diagnostico). El único que abre sockets es
# :scope; los tokens viajan como NOMBRES de env vars.
#
# Env:
#   OBSERVER_REGISTRY_JSON          registry completo como JSON (si falta, el
#                                   target de dev de dev/start-target.sh)
#   OBSERVER_TARGET_DASHBOARD_URL   base del target de dev (default :4994)
#   OBSERVER_TICK_CRON              cadencia del tick (default cada 5 min)
#   OBSERVER_TELEGRAM_BOT_TOKEN     token del bot (via bot_token_env, no literal)
#   OBSERVER_ALERT_CONVERSATION_ID  chat destino de las alert cards
#   OBSERVER_COOLDOWN_MINUTES       anti-tormenta por (swarm, tipo) (default 30)
#   OBSERVER_SMOKE                  "1" -> fakes desde :persistent_term (boot smoke)

registry =
  case System.get_env("OBSERVER_REGISTRY_JSON") do
    json when is_binary(json) and json != "" ->
      Jason.decode!(json)

    _ ->
      %{
        "jmlago-genswarms-fleet-mcp" => %{
          "dashboard_url" =>
            System.get_env("OBSERVER_TARGET_DASHBOARD_URL") || "http://127.0.0.1:4994",
          "token_env" => "OBSERVER_TARGET_DASH_TOKEN"
        }
      }
  end

tick_cron = System.get_env("OBSERVER_TICK_CRON") || "*/5 * * * *"
smoke? = System.get_env("OBSERVER_SMOKE") == "1"

if (System.get_env("OBSERVER_TELEGRAM_BOT_TOKEN") || "") == "" do
  raise "OBSERVER_TELEGRAM_BOT_TOKEN is not set (use a dummy + OBSERVER_SMOKE=1 for smoke runs)"
end

scope_client =
  if smoke?,
    do: [
      client: Genswarms.Observer.Client.Fake,
      client_opts: [fake: :persistent_term.get({:observer_smoke, :scope_fake})]
    ],
    else: [client: Genswarms.Observer.Client.Http, client_opts: []]

sender_client =
  if smoke?,
    do: [
      client: Genswarms.Telegram.Client.Fake,
      client_opts: [fake: :persistent_term.get({:observer_smoke, :telegram_fake})]
    ],
    else: []

%{
  name: "observer",
  # fase 3: :diagnostico pasa de :mock a bwrap network: :isolated. Hoy es el
  # placeholder que el engine exige (un swarm sin agentes no arranca) y el
  # consumidor allowlisted de las lecturas de :scope.
  agents: [
    %{
      name: :diagnostico,
      backend: :mock,
      skills: [Path.join(__DIR__, "skills/diagnostico.md")]
    }
  ],
  objects: [
    %{
      name: :cron,
      handler: Genswarms.Cron,
      config: %{
        swarm_name: "observer",
        # nadie crea jobs en runtime; el seed job es todo el producto
        trusted_sources: [],
        allowed_targets: %{scope: ["tick"]},
        seed_jobs: [
          %{
            name: "observer-tick",
            dedupe_key: "loop:observer-tick",
            schedule: %{"cron" => tick_cron},
            target: "scope",
            message: %{"action" => "tick"}
          }
        ]
      }
    },
    %{
      name: :scope,
      handler: Genswarms.Observer.Objects.Scope,
      config:
        Map.merge(
          %{
            swarm_name: "observer",
            registry: registry,
            thresholds: %{},
            cooldown_minutes:
              String.to_integer(System.get_env("OBSERVER_COOLDOWN_MINUTES") || "30"),
            alert_conversation_id: System.get_env("OBSERVER_ALERT_CONVERSATION_ID"),
            tick_sources: ["cron"],
            read_sources: ["diagnostico"],
            sender: :sender
          },
          Map.new(scope_client)
        )
    },
    %{
      name: :sender,
      handler: Genswarms.Telegram.Objects.Sender,
      config:
        Map.merge(
          %{
            bot_token_env: "OBSERVER_TELEGRAM_BOT_TOKEN",
            send_sources: [:scope],
            binding_authority: :__none__,
            dry_run: System.get_env("OBSERVER_TELEGRAM_DRY_RUN") == "1"
          },
          Map.new(sender_client)
        )
    }
  ] ++
    # el observer también es observable: su propio dashboard, solo si el
    # paquete está cargado (dep live-only; el paquete publicado no lo exige)
    (if Code.ensure_loaded?(GenswarmsDashboard.Objects.Dashboard) do
       [
         %{
           name: :dashboard,
           handler: GenswarmsDashboard.Objects.Dashboard,
           config: %{
             swarm: "observer",
             port: System.get_env("OBSERVER_DASHBOARD_PORT") || "4996",
             dashboard_title: "observer · el swarm que vigila swarms"
           }
         }
       ]
     else
       []
     end),
  topology: [
    {:cron, :scope},
    # la respuesta del tick (contadores) vuelve a cron; sin esta arista el
    # router la descarta con "invalid route"
    {:scope, :cron},
    {:scope, :sender},
    {:diagnostico, :scope},
    {:scope, :diagnostico}
  ]
}
