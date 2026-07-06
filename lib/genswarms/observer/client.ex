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

  @doc """
  Fase 3 (O6): one conversation's transcript, for the diagnosis relay.
  `cid` is form-encoded into the path by the implementation — callers pass
  the raw cid. This is the narrowest read the client exposes; `Objects.Scope`
  is what binds it to an alert-derived allowlist, not this seam.
  """
  @callback get_session_history(
              base_url :: String.t(),
              swarm :: String.t(),
              cid :: String.t(),
              token :: String.t() | nil,
              opts
            ) :: {:ok, map()} | {:error, term()}
end
