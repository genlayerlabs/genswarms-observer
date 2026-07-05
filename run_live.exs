# Boots the observer live inside the engine's BEAM.
#
#   GENSWARMS_PATH=/home/jm/docs/personal/genswarms \
#   SUBZEROCLAW_PATH=/path/to/subzeroclaw/subzeroclaw \
#   GENSWARMS_ALLOWED_ENDPOINTS=router.ygr.ai \
#   UNHARDCODED_CONSUMER_KEY=llmr_... \
#   OBSERVER_TELEGRAM_BOT_TOKEN=... OBSERVER_ALERT_CONVERSATION_ID=tg:...:0 \
#   mix run run_live.exs
#
# - GENSWARMS_PATH enables the engine dep in mix.exs — the published package
#   does not compile against the engine.
# - SUBZEROCLAW_PATH: with the engine as a dep, sibling resolution cannot
#   find the binary (it points into _build/..) — pass it explicitly.
# - GENSWARMS_ALLOWED_ENDPOINTS: the isolated-network forwarder is
#   fail-closed; the router host must be allowlisted or the body won't start.
# - UNHARDCODED_CONSUMER_KEY: the router consumer key (fase 3); without it
#   :diagnostico degrades to :mock.
#
# NOTE: `mix deps.get` with GENSWARMS_PATH fattens mix.lock with the engine's
# hex deps. Do NOT commit that lock: the repo's lock is generated WITHOUT the
# env (rm mix.lock && mix deps.get) so the package stays clean.

{:ok, _} = Application.ensure_all_started(:crypto)
{:ok, _} = Application.ensure_all_started(:inets)
{:ok, _} = Application.ensure_all_started(:ssl)
{:ok, _} = Application.ensure_all_started(:genswarms)

# This BEAM's engine REST (for genswarms-fleet: get_events/get_config/
# patch_object_config on the observer itself). Tokens: GENSWARMS_API_TOKEN /
# GENSWARMS_CONFIG_API_TOKEN.
case System.get_env("OBSERVER_ENGINE_PORT") do
  port when is_binary(port) and port != "" ->
    case Genswarms.Application.start_web_server(port: String.to_integer(port)) do
      {:ok, _pid} -> IO.puts("observer engine REST on :#{port}")
      other -> IO.puts("engine REST FAILED on :#{port} -> #{inspect(other)}")
    end

  _ ->
    :ok
end

config_path = Path.join(__DIR__, System.get_env("OBSERVER_CONFIG") || "observer.swarm.exs")

Genswarms.SwarmManager.stop("observer")
{:ok, swarm} = Genswarms.SwarmManager.start_swarm(config_path)

IO.puts("observer swarm up (#{inspect(swarm)})")

Process.sleep(:infinity)
