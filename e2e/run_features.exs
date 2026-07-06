# Gherkin-driven e2e for the TRIO: engine + dashboard object + observer, all
# live. A target swarm builds from config and stands up a real
# GenswarmsDashboard endpoint; the observer reads it over :httpc (Client.Http)
# and runs its detectors. No fakes.
#
#   set -a; source ~/docs/personal/strategivm/.env; set +a   # (for engine env)
#   GENSWARMS_PATH=~/docs/personal/genswarms mix run e2e/run_features.exs

require Logger
Logger.configure(level: :warning)

defmodule E2E.Gherkin do
  def parse(text) do
    lines =
      text |> String.split("\n") |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == "" or String.starts_with?(&1, "#")))

    Enum.reduce(lines, %{feature: nil, background: [], scenarios: [], _cur: nil}, fn line, acc ->
      cond do
        String.starts_with?(line, "Feature:") -> %{acc | feature: rest(line, "Feature:")}
        String.starts_with?(line, "Background:") -> %{acc | _cur: :background}
        String.starts_with?(line, "Scenario:") ->
          %{acc | scenarios: acc.scenarios ++ [%{name: rest(line, "Scenario:"), steps: []}], _cur: :scenario}
        kw = kw(line) -> add(acc, {kw, rest(line, kw)})
        true -> acc
      end
    end)
  end

  defp add(%{_cur: :background} = a, s), do: %{a | background: a.background ++ [s]}
  defp add(%{_cur: :scenario, scenarios: scs} = a, s) do
    {last, r} = List.pop_at(scs, -1)
    %{a | scenarios: r ++ [%{last | steps: last.steps ++ [s]}]}
  end
  defp add(a, _), do: a

  defp kw(l), do: Enum.find(["Given ", "When ", "Then ", "And ", "But "], &String.starts_with?(l, &1)) |> then(&(&1 && String.trim(&1)))
  defp rest(l, k), do: l |> String.replace_prefix(k, "") |> String.trim()

  def run(parsed, registry, report) do
    Enum.reduce(parsed.scenarios, {0, 0}, fn sc, {p, f} ->
      report.({:scenario, sc.name})
      res = try do
        Enum.reduce(parsed.background ++ sc.steps, %{}, fn {kw, text}, ctx -> step(kw, text, registry, ctx, report) end)
        :pass
      rescue e -> {:fail, Exception.message(e)} end
      case res do
        :pass -> {p + 1, f}
        {:fail, m} -> report.({:fail, m}); {p, f + 1}
      end
    end)
  end

  defp step(kw, text, registry, ctx, report) do
    case Enum.find_value(registry, fn {re, fun} ->
           case Regex.run(re, text, capture: :all_but_first) do nil -> nil; caps -> {fun, caps} end
         end) do
      {fun, caps} -> c = fun.(ctx, caps) || ctx; report.({:ok, kw, text}); c
      nil -> report.({:undef, kw, text}); raise "undefined step: #{text}"
    end
  end
end

{:ok, _} = Application.ensure_all_started(:genswarms)
{:ok, _} = Application.ensure_all_started(:genswarms_dashboard)
Code.ensure_loaded(GenswarmsDashboard.Objects.Dashboard)

{:ok, created} = Agent.start_link(fn -> [] end)
assert! = fn c, m -> unless c, do: raise(m) end

poll = fn fun, secs ->
  Enum.reduce_while(1..div(secs * 1000, 500), false, fn _i, _ ->
    Process.sleep(500); if fun.(), do: {:halt, true}, else: {:cont, false}
  end)
end

# The GenswarmsDashboard endpoint is a singleton process, so only ONE swarm can
# stand up a dashboard per BEAM. Build a single shared target up front; every
# scenario reads it (the endpoint_down scenario, last, stops it).
port = 40000 + rem(System.unique_integer([:positive]), 20000)
shared_swarm = "trio-#{System.unique_integer([:positive])}"
shared_base = "http://127.0.0.1:#{port}"
{:ok, ^shared_swarm} = Genswarms.SwarmManager.start_from_config(%{
  name: shared_swarm,
  agents: [%{name: :worker, backend: :mock}],
  objects: [%{name: :dashboard, handler: GenswarmsDashboard.Objects.Dashboard,
              config: %{swarm: shared_swarm, port: to_string(port), dashboard_title: "e2e trio"}}],
  topology: []
})
Agent.update(created, &[shared_swarm | &1])
_ = poll.(fn ->
  match?({:ok, _}, Genswarms.Observer.Client.Http.get_dashboard(shared_base, shared_swarm, nil, timeout_ms: 1500))
end, 25)

fetch = fn base, swarm ->
  Genswarms.Observer.Client.Http.get_dashboard(base, swarm, nil, timeout_ms: 3000)
end

detect = fn swarm, dash ->
  {alerts, _} = Genswarms.Observer.Detectors.detect(swarm,
    %{dashboard: dash, events: {:ok, []}}, %{}, nil, System.system_time(:millisecond))
  alerts
end

steps = [
  {~r/^a target swarm built from config with a dashboard object on a port$/,
   fn ctx, _ -> Map.merge(ctx, %{swarm: shared_swarm, base: shared_base}) end},

  {~r/^the observer fetches that swarm's dashboard over HTTP$/,
   fn ctx, _ -> Map.put(ctx, :dash, fetch.(ctx.base, ctx.swarm)) end},
  {~r/^it gets a real envelope naming the swarm and its nodes$/,
   fn ctx, _ ->
     assert!.(match?({:ok, _}, ctx.dash), "fetch failed: #{inspect(ctx.dash)}")
     {:ok, env} = ctx.dash
     body = Jason.encode!(env)
     assert!.(String.contains?(body, ctx.swarm), "envelope does not name the swarm")
     assert!.(Map.has_key?(env, "nodes") or Map.has_key?(env, "topology") or Map.has_key?(env, "summary"),
       "no dashboard shape: #{inspect(Map.keys(env))}"); ctx
   end},

  {~r/^the observer fetches the dashboard and runs its detectors$/,
   fn ctx, _ -> Map.put(ctx, :alerts, detect.(ctx.swarm, fetch.(ctx.base, ctx.swarm))) end},
  {~r/^no alert is raised for a healthy swarm$/,
   fn ctx, _ -> assert!.(ctx.alerts == [], "unexpected alerts: #{inspect(Enum.map(ctx.alerts, & &1.type))}"); ctx end},

  {~r/^the swarm is stopped and the observer fetches again$/,
   fn ctx, _ ->
     Genswarms.SwarmManager.stop(ctx.swarm)
     poll.(fn -> match?({:error, _}, fetch.(ctx.base, ctx.swarm)) end, 15)
     Map.put(ctx, :alerts, detect.(ctx.swarm, fetch.(ctx.base, ctx.swarm)))
   end},
  {~r/^the fetch fails and the detectors raise endpoint_down$/,
   fn ctx, _ ->
     types = Enum.map(ctx.alerts, & &1.type)
     assert!.(:endpoint_down in types, "expected endpoint_down, got #{inspect(types)}"); ctx
   end}
]

report = fn
  {:scenario, n} -> IO.puts("\n  Scenario: #{n}")
  {:ok, kw, t} -> IO.puts("    ✓ #{kw} #{t}")
  {:undef, kw, t} -> IO.puts("    ? #{kw} #{t}  (UNDEFINED)")
  {:fail, m} -> IO.puts("    ✗ FAILED: #{m}")
end

files = Path.wildcard(Path.join(__DIR__, "features/*.feature"))
{tp, tf} = Enum.reduce(files, {0, 0}, fn file, {p, f} ->
  parsed = E2E.Gherkin.parse(File.read!(file))
  fname = Path.basename(file, ".feature")
  IO.puts("\nFeature: #{parsed.feature}  [#{fname}]")
  {sp, sf} = E2E.Gherkin.run(parsed, steps, report)
  {p + sp, f + sf}
end)

Enum.each(Agent.get(created, & &1), fn s -> try do Genswarms.SwarmManager.stop(s) rescue _ -> :ok end end)
IO.puts("\n== trio: #{tp} passed, #{tf} failed ==")
if tf > 0, do: System.halt(1), else: IO.puts("E2E TRIO OK")
