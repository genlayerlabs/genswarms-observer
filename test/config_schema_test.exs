defmodule Genswarms.Observer.ConfigSchemaTest do
  use ExUnit.Case, async: true

  # every config key Scope.init/1 reads — keep in sync with the handler;
  # the schema<->init conformance below catches drift in either direction
  @init_keys ~w(swarm_name name registry thresholds cooldown_minutes
                alert_conversation_id tick_sources read_sources sender
                client client_opts now_fn deliver_fn)

  defp schema do
    Path.join(__DIR__, "../swarm-object.json")
    |> File.read!()
    |> Jason.decode!()
    |> Map.fetch!("config_schema")
  end

  test "schema is a valid object schema with properties" do
    s = schema()
    assert s["type"] == "object"
    assert is_map(s["properties"]) and map_size(s["properties"]) > 0
  end

  test "module points at the handler and it loads" do
    module =
      Path.join(__DIR__, "../swarm-object.json")
      |> File.read!()
      |> Jason.decode!()
      |> Map.fetch!("module")

    assert module == "Genswarms.Observer.Objects.Scope"
    assert Code.ensure_loaded?(Genswarms.Observer.Objects.Scope)
  end

  test "schema properties and init-read keys match exactly (contract, drift is a bug)" do
    schema_keys = schema()["properties"] |> Map.keys() |> Enum.sort()
    assert schema_keys == Enum.sort(@init_keys)
  end

  test "no secrets in this package's config (tokens are env var NAMES)" do
    props = schema()["properties"]
    assert Enum.all?(props, fn {_k, v} -> v["x-secret"] != true end)

    registry_entry = props["registry"]["additionalProperties"]["properties"]
    assert registry_entry["token_env"]["description"] =~ "NAME"
  end

  test "x-mutable marks the product/tuning surface, not the trust surface" do
    props = schema()["properties"]
    mutable = for {k, v} <- props, v["x-mutable"] == true, do: k

    assert Enum.sort(mutable) ==
             ["alert_conversation_id", "cooldown_minutes", "registry", "thresholds"]

    # allowlists must never be hot-mutable
    refute "tick_sources" in mutable
    refute "read_sources" in mutable
    refute "sender" in mutable
  end

  test "a config drawn from the schema boots the handler" do
    config = %{
      "swarm_name" => "observer",
      "registry" => %{
        "wingston" => %{
          "dashboard_url" => "http://127.0.0.1:4994",
          "token_env" => "WINGSTON_DASH_TOKEN",
          "repo" => "genlayerlabs/wingston-rally-bot"
        }
      },
      "thresholds" => %{"stall_minutes" => 5},
      "cooldown_minutes" => 15,
      "alert_conversation_id" => "tg:1:0",
      "tick_sources" => ["cron"],
      "read_sources" => ["diagnostico"],
      "sender" => "sender",
      "client" => "Genswarms.Observer.Client.Http"
    }

    {:ok, state} = Genswarms.Observer.Objects.Scope.init(config)
    assert state.registry["wingston"]["token_env"] == "WINGSTON_DASH_TOKEN"
    assert state.thresholds == %{"stall_minutes" => 5}
    assert state.client == Genswarms.Observer.Client.Http
    assert MapSet.member?(state.tick_sources, "cron")
  end
end
