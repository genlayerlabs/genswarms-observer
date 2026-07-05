# Boot smoke: arranca el swarm observer con fakes (sin red, sin Telegram
# real), fuerza un tick y verifica que la alerta endpoint_down llega al
# sender como send_card.
#
#   GENSWARMS_PATH=/home/jm/docs/personal/genswarms mix run tests/boot_smoke.exs

{:ok, _} = Application.ensure_all_started(:genswarms)

root = Path.expand("..", __DIR__)

# ── fakes, publicados donde observer.swarm.exs (OBSERVER_SMOKE=1) los lee ──
# El scope fake NO conoce ningún swarm: todo fetch responde
# {:error, :not_configured} — exactamente lo que endpoint_down debe ver.
{:ok, scope_fake} = Genswarms.Observer.Client.Fake.start_link(%{})
{:ok, telegram_fake} = Genswarms.Telegram.Client.Fake.start_link()
:persistent_term.put({:observer_smoke, :scope_fake}, scope_fake)
:persistent_term.put({:observer_smoke, :telegram_fake}, telegram_fake)

System.put_env("OBSERVER_SMOKE", "1")
System.put_env("OBSERVER_TELEGRAM_BOT_TOKEN", "0000000000:smoke-dummy")
System.put_env("OBSERVER_ALERT_CONVERSATION_ID", "tg:1:0")
# el seed job no debe disparar solo durante el smoke (29 de febrero)
System.put_env("OBSERVER_TICK_CRON", "0 0 29 2 *")

Genswarms.SwarmManager.stop("observer")
{:ok, _} = Genswarms.SwarmManager.start_swarm(Path.join(root, "observer.swarm.exs"))

Process.sleep(1_500)

status = Genswarms.SwarmManager.status("observer")
IO.puts("swarm status: #{inspect(status, limit: 6, printable_limit: 200)}")

# ── tick forzado (como si fuera cron) → endpoint_down → card al sender ─────
Genswarms.Objects.ObjectServer.deliver_message(
  "observer",
  :scope,
  :cron,
  Jason.encode!(%{action: "tick"})
)

Process.sleep(1_000)

calls = Genswarms.Telegram.Client.Fake.calls(telegram_fake)
IO.puts("telegram fake recorded #{length(calls)} call(s)")

# send_card viaja como :send_rich_message con el card renderizado a HTML
sent_alert? =
  Enum.any?(calls, fn %{payload: payload} ->
    html = get_in(payload, [:rich_message, :html]) || payload[:text] || ""
    String.contains?(to_string(html), "endpoint_down")
  end)

unless sent_alert? do
  IO.inspect(calls, label: "telegram calls", limit: :infinity)
  raise "no send with an endpoint_down card reached the telegram fake"
end

IO.puts("BOOT SMOKE OK")
Genswarms.SwarmManager.stop("observer")
