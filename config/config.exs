import Config

# A path dep's config files are NOT loaded by the host: when the engine comes
# in as a dep (GENSWARMS_PATH, live runs), its REST endpoint needs the adapter
# and secret declared HERE or Phoenix falls back to the cowboy adapter
# (undef). Inert when the engine is not in the tree.
config :genswarms, GenswarmsWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [formats: [json: GenswarmsWeb.ErrorJSON], layout: false],
  pubsub_server: Genswarms.PubSub,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  check_origin: false,
  secret_key_base: "observer_dev_secret_key_base_that_is_at_least_64_bytes_long_padding!!!!"

config :phoenix, :json_library, Jason
