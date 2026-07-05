# Arranca el observer en vivo dentro del BEAM del engine.
#
#   GENSWARMS_PATH=/home/jm/docs/personal/genswarms \
#   SUBZEROCLAW_PATH=/path/al/subzeroclaw/subzeroclaw \
#   GENSWARMS_ALLOWED_ENDPOINTS=router.ygr.ai \
#   UNHARDCODED_CONSUMER_KEY=llmr_... \
#   OBSERVER_TELEGRAM_BOT_TOKEN=... OBSERVER_ALERT_CONVERSATION_ID=tg:...:0 \
#   mix run run_live.exs
#
# - SUBZEROCLAW_PATH: con el engine como dep, la resolución por siblings no
#   encuentra el binario (apunta a _build/..) — darlo explícito.
# - GENSWARMS_ALLOWED_ENDPOINTS: el forwarder de red aislada es fail-closed;
#   el host del router debe estar allowlisted o el body no arranca.
# - UNHARDCODED_CONSUMER_KEY: la consumer key del router (fase 3); sin ella
#   :diagnostico degrada a :mock.
#
# (GENSWARMS_PATH activa la dep del engine en mix.exs — el paquete publicado
# no compila contra el engine.)
#
# OJO: `mix deps.get` con GENSWARMS_PATH engorda mix.lock con las deps hex del
# engine. NO commitear ese lock: el del repo se genera SIN el env
# (rm mix.lock && mix deps.get) para que el paquete quede limpio.

{:ok, _} = Application.ensure_all_started(:crypto)
{:ok, _} = Application.ensure_all_started(:inets)
{:ok, _} = Application.ensure_all_started(:ssl)
{:ok, _} = Application.ensure_all_started(:genswarms)

# REST del engine de ESTE BEAM (para genswarms-fleet: get_events/get_config/
# patch_object_config del observer). Tokens: GENSWARMS_API_TOKEN / _CONFIG_.
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
