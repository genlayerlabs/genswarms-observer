import Config

# La config de una dep por path NO se carga en el host: cuando el engine entra
# como dep (GENSWARMS_PATH, runs en vivo), su endpoint REST necesita adapter y
# secret declarados AQUÍ o Phoenix cae al adapter cowboy (undef). Inerte si el
# engine no está en el árbol.
config :genswarms, GenswarmsWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [formats: [json: GenswarmsWeb.ErrorJSON], layout: false],
  pubsub_server: Genswarms.PubSub,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  check_origin: false,
  secret_key_base: "observer_dev_secret_key_base_that_is_at_least_64_bytes_long_padding!!!!"

config :phoenix, :json_library, Jason
