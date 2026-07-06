defmodule Genswarms.Observer.Detector do
  @moduledoc """
  Behaviour for pluggable health detectors run by `Genswarms.Observer.DetectorRunner`.

  A detector is pure and synchronous: given the same `fetched` data and
  `ctx`, `detect/2` always returns the same `{alerts, state}`. Isolation
  (crash/timeout containment, thresholds overlay, state-commit-on-success
  only) is the runner's job — a detector never needs to guard against its
  own failure.
  """

  @typedoc """
  One tick's fetch of the observed swarm, as assembled by `Objects.Scope`:

  - `:dashboard` — `GET /api/swarms/:name/dashboard` envelope.
  - `:events` — `GET /api/swarms/:name/events`, the engine-raw LogStore
    surface (string keys, ISO8601 `"timestamp"`). Consumed by the legacy
    health detectors (`Genswarms.Observer.Detectors`).
  - `:feed` — `GET /api/swarms/:name/events/feed`, the host's DISPLAY event
    feed (cursor read; Scope threads the per-swarm cursor). This is where
    the `request_open`/`reply_sent`/... vocabulary lives — string keys,
    `"ts"` float unix seconds. `:unavailable` mirrors the wire's
    `source: "unavailable"` (host has no EventsSource — legitimate, not an
    error); detectors consuming the feed must treat `:unavailable` and
    `{:error, _}` as a no-op with prior state.
  """
  @type fetched :: %{
          dashboard: {:ok, map} | {:error, term},
          events: {:ok, [map]} | {:error, term},
          feed: {:ok, [map]} | :unavailable | {:error, term}
        }

  @type ctx :: %{swarm: String.t(), thresholds: map, state: term, now_ms: integer}

  @type alert :: %{
          required(:type) => atom,
          required(:swarm) => String.t(),
          required(:at_ms) => integer,
          required(:summary) => String.t(),
          required(:evidence) => map,
          optional(:key) => term,
          optional(:cids) => [String.t()]
        }

  @doc """
  Runs the detector for one swarm tick. Pure — no HTTP, no clock, no LLM.
  Returns the alerts raised this tick and the state to thread into the
  next tick (only committed by the runner if the return is well-formed).
  """
  @callback detect(fetched, ctx) :: {[alert], state :: term}

  @doc """
  Flat map of namespaced threshold defaults (string keys), overlaid by the
  x-mutable global thresholds map at call time.
  """
  @callback default_thresholds() :: %{optional(String.t()) => term}

  @doc "Initial per-swarm state, before any tick has run for that swarm."
  @callback init() :: term

  @optional_callbacks default_thresholds: 0, init: 0
end
