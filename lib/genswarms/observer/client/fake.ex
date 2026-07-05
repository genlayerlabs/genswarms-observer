defmodule Genswarms.Observer.Client.Fake do
  @moduledoc """
  In-memory dashboard-backend reader for tests and boot smoke.

  Fixture shape (per observed swarm name):

      {:ok, pid} = Client.Fake.start_link(%{
        "wingston" => %{
          dashboard: {:ok, envelope_map},
          events: {:ok, [event_map]}
        }
      })

  Inject via `client: Genswarms.Observer.Client.Fake, client_opts: [fake: pid]`.
  Mutate mid-test with `put/3`; inspect what Scope asked for with `calls/1`.
  An unknown swarm answers `{:error, :not_configured}` — indistinguishable
  from a dead endpoint, which is exactly what endpoint_down should see.
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

  defp answer(swarm, kind, token, opts) do
    pid = Keyword.fetch!(opts, :fake)

    Agent.get_and_update(pid, fn state ->
      call = %{swarm: swarm, kind: kind, token: token}

      reply =
        case get_in(state.fixture, [swarm, kind]) do
          nil -> {:error, :not_configured}
          result -> result
        end

      {reply, %{state | calls: [call | state.calls]}}
    end)
  end
end
