# Boot smoke: boots the observer swarm with fakes (no network, no real
# Telegram), forces a tick and verifies the endpoint_down alert reaches the
# sender as a send_card.
#
#   GENSWARMS_PATH=/home/jm/docs/personal/genswarms mix run tests/boot_smoke.exs

{:ok, _} = Application.ensure_all_started(:genswarms)

root = Path.expand("..", __DIR__)

# ── fakes, published where observer.swarm.exs (OBSERVER_SMOKE=1) reads them ─
# The scope fake knows NO swarm: every fetch answers
# {:error, :not_configured} — exactly what endpoint_down must see.
{:ok, scope_fake} = Genswarms.Observer.Client.Fake.start_link(%{})
{:ok, telegram_fake} = Genswarms.Telegram.Client.Fake.start_link()
:persistent_term.put({:observer_smoke, :scope_fake}, scope_fake)
:persistent_term.put({:observer_smoke, :telegram_fake}, telegram_fake)

System.put_env("OBSERVER_SMOKE", "1")
# own port: don't collide with a live observer on :4996
System.put_env("OBSERVER_DASHBOARD_PORT", "4997")
System.put_env("OBSERVER_TELEGRAM_BOT_TOKEN", "0000000000:smoke-dummy")
System.put_env("OBSERVER_ALERT_CONVERSATION_ID", "tg:1:0")
# the seed job must not fire on its own during the smoke (February 29th)
System.put_env("OBSERVER_TICK_CRON", "0 0 29 2 *")

Genswarms.SwarmManager.stop("observer")
{:ok, _} = Genswarms.SwarmManager.start_swarm(Path.join(root, "observer.swarm.exs"))

Process.sleep(1_500)

status = Genswarms.SwarmManager.status("observer")
IO.puts("swarm status: #{inspect(status, limit: 6, printable_limit: 200)}")

# ── forced tick (as if from cron) → endpoint_down → card to the sender ─────
Genswarms.Objects.ObjectServer.deliver_message(
  "observer",
  :scope,
  :cron,
  Jason.encode!(%{action: "tick"})
)

Process.sleep(1_000)

calls = Genswarms.Telegram.Client.Fake.calls(telegram_fake)
IO.puts("telegram fake recorded #{length(calls)} call(s)")

# send_card travels as :send_rich_message with the card rendered to HTML
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
