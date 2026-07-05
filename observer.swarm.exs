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

# :diagnostico (fase 3): body bwrap con red AISLADA — sin red salvo el
# forwarder clavado al router unhardcoded (el modelo lo elige la Σ_pol,
# nunca se nombra aquí; la consumer key entra por env, jamás literal).
# Los datos de los swarms se los pide a :scope por la topología.
# Sin UNHARDCODED_CONSUMER_KEY (o en smoke) degrada a :mock para que el
# boot smoke y los hosts sin sandbox sigan funcionando.
diagnostico =
  if not smoke? and (System.get_env("UNHARDCODED_CONSUMER_KEY") || "") != "" do
    %{
      name: :diagnostico,
      backend: :bwrap,
      # URL COMPLETA: szc postea al endpoint tal cual — con la base /v1 a
      # secas el turno muere en silencio (gotcha de config.example de szc)
      endpoint: "https://router.ygr.ai/v1/chat/completions",
      skills: [Path.join(__DIR__, "skills/diagnostico.md")],
      config: %{
        network: :isolated,
        api_key: System.get_env("UNHARDCODED_CONSUMER_KEY"),
        # Sin max_turns: el default de szc (200) ya acota el loop de tools
        # por tarea, y el contexto lo gestiona la autocompactación del router
        # (compact_extra) — un presupuesto arbitrario aquí solo puede truncar
        # trabajo legítimo. (Si algún día hace falta, requiere engine ≥ #79:
        # antes, CUALQUIER agente bwrap con max_turns moría al arrancar.)
        # sin "model": contra el router la Σ_pol decide; un model literal
        # aquí sería precisamente lo que unhardcoded existe para evitar
        request_extra: %{
          "policy_ir" =>
            Jason.decode!(File.read!(Path.join(__DIR__, "policies/diagnostico.policy.json")))
        },
        # sesión de larga vida (cada escalada se acumula): el sealing lo hace
        # el ROUTER en /v1/compact cuando él decide (x_router.compact) — szc
        # solo transporta keep_recent + la Σ_pol barata del summariser
        compact_extra: %{
          "keep_recent" => 8,
          "policy_ir" =>
            Jason.decode!(
              File.read!(Path.join(__DIR__, "policies/diagnostico.compact.policy.json"))
            )
        }
      }
    }
  else
    %{
      name: :diagnostico,
      backend: :mock,
      skills: [Path.join(__DIR__, "skills/diagnostico.md")]
    }
  end

%{
  name: "observer",
  agents: [diagnostico],
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
            sender: :sender,
            escalate_to: :diagnostico
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
