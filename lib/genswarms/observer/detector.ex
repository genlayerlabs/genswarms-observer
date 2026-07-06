defmodule Genswarms.Observer.Detector do
  @moduledoc """
  Behaviour for pluggable health detectors run by `Genswarms.Observer.DetectorRunner`.

  A detector is pure and synchronous: given the same `fetched` data and
  `ctx`, `detect/2` always returns the same `{alerts, state}`. Isolation
  (crash/timeout containment, thresholds overlay, state-commit-on-success
  only) is the runner's job — a detector never needs to guard against its
  own failure.
  """

  @type fetched :: %{
          dashboard: {:ok, map} | {:error, term},
          events: {:ok, [map]} | {:error, term}
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
