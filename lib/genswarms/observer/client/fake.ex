defmodule Genswarms.Observer.Client.Fake do
  @moduledoc """
  In-memory dashboard-backend reader for tests and boot smoke.

  Fixture shape (per observed swarm name):

      {:ok, pid} = Client.Fake.start_link(%{
        "myswarm" => %{
          dashboard: {:ok, envelope_map},
          events: {:ok, [event_map]},
          events_feed: {:ok, %{events: [display_event_map], seq: 7}},
          session_history: %{"tg:1:0" => {:ok, %{"turns" => [...]}}}
        }
      })

  Inject via `client: Genswarms.Observer.Client.Fake, client_opts: [fake: pid]`.
  Mutate mid-test with `put/3`; inspect what Scope asked for with `calls/1`.
  An unknown swarm answers `{:error, :not_configured}` — indistinguishable
  from a dead endpoint, which is exactly what endpoint_down should see.

  `session_history` is keyed one level deeper, by `cid` (unlike `dashboard`/
  `events`, which take no extra argument) — an unconfigured cid answers the
  same `{:error, :not_configured}` fallback.

  `events_feed` mirrors the real wire's fail-soft envelope: a CONFIGURED
  swarm without the key answers `:unavailable` (a live dashboard whose host
  never wired an EventsSource — plug.ex serves `source: "unavailable"`, not
  an error), while an unknown swarm still answers `{:error, :not_configured}`
  (a dead endpoint fails every route). The fixture value may also be a
  1-arity fun of `since`, for cursor-threading tests; each call records
  `since` in `calls/1`.
  """

  @behaviour Genswarms.Observer.Client

  def start_link(fixture \\ %{}) do
    Agent.start_link(fn -> %{fixture: fixture, calls: []} end)
  end

  def put(pid, swarm, data) do
    Agent.update(pid, fn state ->
      %{state | fixture: Map.put(state.fixture, swarm, data)}
    end)
  end

  def calls(pid), do: Agent.get(pid, fn state -> Enum.reverse(state.calls) end)

  @impl true
  def get_dashboard(_base_url, swarm, token, opts), do: answer(swarm, :dashboard, token, opts)

  @impl true
  def get_events(_base_url, swarm, token, opts), do: answer(swarm, :events, token, opts)

  @impl true
  def get_events_feed(_base_url, swarm, since, token, opts) do
    pid = Keyword.fetch!(opts, :fake)

    Agent.get_and_update(pid, fn state ->
      call = %{swarm: swarm, kind: :events_feed, since: since, token: token}

      reply =
        case Map.fetch(state.fixture, swarm) do
          # unknown swarm = dead endpoint: every route fails
          :error -> {:error, :not_configured}
          {:ok, swarm_fixture} -> feed_reply(swarm_fixture, since)
        end

      {reply, %{state | calls: [call | state.calls]}}
    end)
  end

  defp feed_reply(swarm_fixture, since) do
    case Map.get(swarm_fixture, :events_feed) do
      # configured swarm, no feed fixture = host without an EventsSource
      nil -> :unavailable
      fun when is_function(fun, 1) -> fun.(since)
      result -> result
    end
  end

  @impl true
  def get_session_history(_base_url, swarm, cid, token, opts),
    do: answer(swarm, :session_history, token, opts, cid)

  # `cid` deepens both the fixture path and the recorded call — only
  # `get_session_history` passes one; the plain readers leave it nil.
  defp answer(swarm, kind, token, opts, cid \\ nil) do
    pid = Keyword.fetch!(opts, :fake)

    Agent.get_and_update(pid, fn state ->
      call =
        case cid do
          nil -> %{swarm: swarm, kind: kind, token: token}
          cid -> %{swarm: swarm, kind: kind, cid: cid, token: token}
        end

      path = if cid == nil, do: [swarm, kind], else: [swarm, kind, cid]

      reply =
        case get_in(state.fixture, path) do
          nil -> {:error, :not_configured}
          result -> result
        end

      {reply, %{state | calls: [call | state.calls]}}
    end)
  end
end
