defmodule GenswarmsObserver.MixProject do
  use Mix.Project

  def project do
    [
      app: :genswarms_observer,
      version: "0.2.0",
      elixir: "~> 1.14",
      elixirc_paths: ["lib"],
      description: "Observer swarm: deterministic health detectors over other genswarms' dashboards",
      package: package(),
      source_url: "https://github.com/genlayerlabs/genswarms-observer",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger, :inets, :ssl]]
  end

  defp deps do
    [
      sibling_or_github(:genswarms_objects, "genlayerlabs", "genswarms-objects", "v0.1.7"),
      sibling_or_github(:genswarms_telegram, "genlayerlabs", "genswarms-telegram", "v0.4.0"),
      {:jason, "~> 1.4"}
    ] ++ engine_dep()
  end

  # The engine is a RUNTIME-only dependency of the published package (scope.ex
  # calls ObjectServer via guarded apply). It only becomes a mix dep for live
  # runs / boot smoke, and only when explicitly pointed at via GENSWARMS_PATH.
  defp engine_dep do
    case System.get_env("GENSWARMS_PATH") do
      path when is_binary(path) and path != "" ->
        [
          {:genswarms, path: path},
          # live-only too: the observer's own dashboard (dogfooding — the
          # observer must be observable). Declared conditionally in
          # observer.swarm.exs, so the published package needs neither.
          # The dashboard repo hosts its mix project under backend/.
          dashboard_dep()
        ]

      _ ->
        []
    end
  end

  defp dashboard_dep do
    override = System.get_env("GENSWARMS_DASHBOARD_PATH")
    sibling = Path.expand("../genswarms-dashboard/backend", __DIR__)

    cond do
      is_binary(override) and override != "" ->
        {:genswarms_dashboard, path: override}

      File.dir?(sibling) ->
        {:genswarms_dashboard, path: sibling}

      true ->
        {:genswarms_dashboard,
         github: "genlayerlabs/genswarms-dashboard", tag: "v0.3.3", sparse: "backend"}
    end
  end

  # packages: env override > sibling checkout > pinned git tag (wingston pattern)
  defp sibling_or_github(app, org, repo, tag) do
    override = System.get_env(String.upcase("#{app}_PATH"))
    sibling = Path.expand("../#{repo}", __DIR__)

    cond do
      is_binary(override) and override != "" -> {app, path: override}
      File.dir?(sibling) -> {app, path: sibling}
      true -> {app, github: "#{org}/#{repo}", tag: tag}
    end
  end

  defp package do
    [
      licenses: ["MIT"],
      files: ["lib", "mix.exs", "README.md", "CHANGELOG.md", "swarmidx.json", "swarm-object.json"],
      links: %{"GitHub" => "https://github.com/genlayerlabs/genswarms-observer"}
    ]
  end
end
