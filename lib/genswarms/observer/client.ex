defmodule Genswarms.Observer.Client do
  @moduledoc """
  Transport seam for reading an observed swarm's dashboard backend.

  `base_url` is the dashboard base (e.g. `http://127.0.0.1:4994`), `swarm` the
  observed swarm's name; the wire paths are the dashboard backend contract
  (`GET /api/swarms/:name/dashboard`, `GET /api/swarms/:name/events`).

  Implementations: `Client.Http` (real, :httpc) and `Client.Fake` (Agent with
  fixtures, injected via `client_opts: [fake: pid]`).
  """

  @type opts :: keyword()

  @callback get_dashboard(base_url :: String.t(), swarm :: String.t(), token :: String.t() | nil, opts) ::
              {:ok, map()} | {:error, term()}

  @callback get_events(base_url :: String.t(), swarm :: String.t(), token :: String.t() | nil, opts) ::
              {:ok, [map()]} | {:error, term()}
end
