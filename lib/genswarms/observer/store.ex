defmodule Genswarms.Observer.Store do
  @moduledoc """
  Injectable durability seam for `Genswarms.Observer.Objects.Scope`.

  The store persists these things, as plain Elixir terms:

  - `seen_periods` — `%{swarm => MapSet.t(String.t())}`, digest period ids
    already delivered (O4).
  - `last_alert` — `%{key => at_ms}`, cooldown timestamps per alert key.
  - `det` — `%{swarm => %{module => term}}`, opaque per-detector state.
  - `signals` (Task 6) — `%{samples:, rules_seen:, rules_miss:}` for the
    declarative `health_rules` evaluator: `samples` is
    `%{{swarm, block_key, rule_id, path} => number}` (delta bookkeeping),
    `rules_seen` is `%{swarm => MapSet.t(block_key)}` and `rules_miss` is
    `%{swarm => %{block_key => consecutive_miss_count}}` (the sovereign
    `rules_gone` debounce). See `Genswarms.Observer.Objects.Scope`.

  Deliberately Elixir terms, not JSON: `MapSet` does not round-trip through
  JSON, and re-deriving it on every load would be needless ceremony for a
  durability seam that (today) never leaves the BEAM. File/Postgres/other
  host-backed adapters are a later concern for whoever operates the box —
  they can encode however they like internally, as long as `load/0` hands
  Scope back the same shape it saved.

  Nothing content-bearing is ever saved here: period ids, timestamps, and
  detector state (which for `Unanswered` can include message `cids` — ids,
  never message text) — no dashboard/event payloads, no card text.

  Validation of what comes back from `load/0` (future period ids, future
  cooldowns, rollback detection) is Scope's job, not the store's — a store
  adapter just needs to hand back exactly what it was given.
  """

  @type saved :: %{
          optional(:seen_periods) => %{String.t() => MapSet.t()},
          optional(:last_alert) => map,
          optional(:det) => map,
          optional(:signals) => map,
          optional(:save_seq) => non_neg_integer
        }

  @callback load() :: {:ok, saved} | :empty | {:error, term}
  @callback save(saved) :: :ok | {:error, term}

  defmodule InMemory do
    @moduledoc """
    Default `Genswarms.Observer.Store` backend: an `Agent`, started lazily
    on first use under a registered name (no supervision tree exists for
    the observer, so this is intentionally self-starting rather than a
    child spec). `load/0` answers `:empty` until the first `save/1`.

    Process-lifetime only — this is NOT durable across a BEAM restart. It
    exists so the seam has a working default and so tests can exercise the
    real contract without a file/DB adapter.
    """

    @behaviour Genswarms.Observer.Store

    @name __MODULE__

    @impl true
    def load do
      ensure_started()
      Agent.get(@name, & &1)
    end

    @impl true
    def save(saved) when is_map(saved) do
      ensure_started()
      Agent.update(@name, fn _ -> {:ok, saved} end)
      :ok
    end

    defp ensure_started do
      case Agent.start(fn -> :empty end, name: @name) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end
    end
  end
end
