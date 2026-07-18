# observer — the swarm that watches swarms (v0.3: cron -> scope -> sender + diagnosis).
#
# Detection is deterministic (Genswarms.Observer.Detectors); the LLM only
# enters at fase 3 (:diagnostico). The only node that opens sockets is
# :scope; tokens travel as env var NAMES.
#
# Env:
#   OBSERVER_REGISTRY_JSON          full registry as JSON (unset -> the
#                                   dev target from dev/start-target.sh)
#   OBSERVER_TARGET_DASHBOARD_URL   dev target base (default :4994)
#   OBSERVER_TICK_CRON              tick cadence (default every 5 min)
#   OBSERVER_TELEGRAM_BOT_TOKEN     bot token (via bot_token_env, never literal)
#   OBSERVER_ALERT_CONVERSATION_ID  destination chat for alert cards
#   OBSERVER_COOLDOWN_MINUTES       per (swarm, type) anti-storm (default 30)
#   OBSERVER_TELEGRAM_DRY_RUN       "1" -> sender records but never hits Telegram
#   OBSERVER_OPS_DIGEST_JSON        daily ops digest config as JSON (unset -> off)
#   OBSERVER_SMOKE                  "1" -> fakes from :persistent_term (boot smoke)
#   UNHARDCODED_CONSUMER_KEY        router consumer key; unset -> :diagnostico
#                                   degrades to :mock

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

# Operator-authored health rules (fail-CLOSED at boot — see Scope's
# `signal_rules` config). Same JSON-env pattern as OBSERVER_REGISTRY_JSON:
# a list of rule maps, each carrying its target "block" key.
signal_rules =
  case System.get_env("OBSERVER_SIGNAL_RULES_JSON") do
    json when is_binary(json) and json != "" -> Jason.decode!(json)
    _ -> []
  end

# Daily ops digest (fail-CLOSED at boot — see Scope's `ops_digest` config).
# Same JSON-env pattern: a map with hour_utc + sections; unset -> off.
ops_digest =
  case System.get_env("OBSERVER_OPS_DIGEST_JSON") do
    json when is_binary(json) and json != "" -> Jason.decode!(json)
    _ -> nil
  end

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

# :diagnostico (fase 3): bwrap body with ISOLATED network — no network except
# the forwarder pinned to the unhardcoded router (the model is chosen by the
# Σ_pol, never named here; the consumer key enters via env, never literal).
# It gets swarm data by asking :scope through the topology.
# Without UNHARDCODED_CONSUMER_KEY (or in smoke) it degrades to :mock so the
# boot smoke and sandbox-less hosts keep working.
diagnostico =
  if not smoke? and (System.get_env("UNHARDCODED_CONSUMER_KEY") || "") != "" do
    %{
      name: :diagnostico,
      backend: :bwrap,
      # FULL URL: szc posts to the endpoint as-is — with the bare /v1 base
      # the turn dies silently (szc config.example gotcha); /v1/compact is
      # derived from this URL too.
      endpoint: "https://router.ygr.ai/v1/chat/completions",
      skills: [Path.join(__DIR__, "skills/diagnostico.md")],
      config: %{
        network: :isolated,
        api_key: System.get_env("UNHARDCODED_CONSUMER_KEY"),
        # No max_turns: szc's default (200) already bounds the tool loop per
        # task, and CONTEXT is the router's auto-compaction business
        # (compact_extra) — an arbitrary budget here can only truncate
        # legitimate work. (If ever needed, requires engine >= #79: before
        # it, ANY bwrap agent with max_turns died at launch.)
        request_extra: %{
          "policy_ir" =>
            Jason.decode!(File.read!(Path.join(__DIR__, "policies/diagnostico.policy.json")))
        },
        # long-lived session (escalations accumulate): sealing is done by the
        # ROUTER at /v1/compact when IT decides (x_router.compact) — szc only
        # carries keep_recent + the cheap summariser Σ_pol
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
        # nobody creates jobs at runtime; the seed job is the whole product
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
            # Prod defaults sized by the 2026-07-18 incident: the wingston
            # snapshot is ~800KB/2s at IDLE, so 5s http + 2s detector walls
            # false-alarmed under evening load. Package defaults stay tighter.
            http_timeout_ms:
              String.to_integer(System.get_env("OBSERVER_HTTP_TIMEOUT_MS") || "15000"),
            detector_timeout_ms:
              String.to_integer(System.get_env("OBSERVER_DETECTOR_TIMEOUT_MS") || "5000"),
            gap_alert_minutes:
              String.to_integer(System.get_env("OBSERVER_GAP_ALERT_MINUTES") || "30"),
            alert_conversation_id: System.get_env("OBSERVER_ALERT_CONVERSATION_ID"),
            tick_sources: ["cron"],
            read_sources: ["diagnostico"],
            sender: :sender,
            escalate_to: :diagnostico,
            signal_rules: signal_rules,
            ops_digest: ops_digest
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
    # the observer is observable too: its own dashboard, only when the
    # package is loaded (live-only dep; the published package doesn't require it)
    (if Code.ensure_loaded?(GenswarmsDashboard.Objects.Dashboard) do
       [
         %{
           name: :dashboard,
           handler: GenswarmsDashboard.Objects.Dashboard,
           config: %{
             swarm: "observer",
             port: System.get_env("OBSERVER_DASHBOARD_PORT") || "4996",
             dashboard_title: "observer · the swarm that watches swarms"
           }
         }
       ]
     else
       []
     end),
  topology: [
    {:cron, :scope},
    # the tick reply (counters) goes back to cron; without this edge the
    # router drops it with "invalid route"
    {:scope, :cron},
    {:scope, :sender},
    {:diagnostico, :scope},
    {:scope, :diagnostico}
  ]
}
