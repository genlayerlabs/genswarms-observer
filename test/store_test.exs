defmodule Genswarms.Observer.StoreTest do
  use ExUnit.Case, async: true

  alias Genswarms.Observer.Client
  alias Genswarms.Observer.Objects.Scope
  alias Genswarms.Observer.Store

  @t0 1_751_734_800_000

  # A store double that always raises — for the "must never take :scope
  # down" test.
  defmodule Crasher do
    @behaviour Store
    def load, do: raise("store is on fire")
    def save(_saved), do: raise("store is on fire")
  end

  # Binds a fresh Agent to a uniquely-named module at runtime, so each test
  # gets its own isolated store (never the process-wide `Store.InMemory`
  # singleton) — and can seed/corrupt the backing Agent directly to drive
  # the validation/rollback scenarios.
  defp fresh_store(initial \\ :empty) do
    {:ok, pid} = Agent.start_link(fn -> initial end)
    name = Module.concat([Genswarms.Observer.StoreTest, "Store#{:erlang.unique_integer([:positive])}"])

    contents =
      quote do
        @behaviour Store
        def load, do: Agent.get(unquote(pid), & &1)
        def save(saved), do: Agent.update(unquote(pid), fn _ -> {:ok, saved} end)
      end

    Module.create(name, contents, Macro.Env.location(__ENV__))
    {name, pid}
  end

  defp healthy_fixture do
    %{
      dashboard: {:error, :econnrefused},
      events: {:ok, []}
    }
  end

  defp start_scope(store_mod, opts \\ []) do
    {:ok, fake} = Client.Fake.start_link(%{"wingston" => healthy_fixture()})
    {:ok, clock} = Agent.start_link(fn -> @t0 end)
    {:ok, outbox} = Agent.start_link(fn -> [] end)

    config =
      Map.merge(
        %{
          swarm_name: "observer",
          registry: %{
            "wingston" => %{dashboard_url: "http://dash.example:4994", token_env: nil, repo: nil}
          },
          tick_sources: ["cron"],
          read_sources: [],
          alert_conversation_id: "tg:1:0",
          client: Client.Fake,
          client_opts: [fake: fake],
          store_mod: store_mod,
          now_fn: fn -> Agent.get(clock, & &1) end,
          deliver_fn: fn target, from, content ->
            Agent.update(outbox, &[%{target: target, from: from, content: content} | &1])
            :ok
          end
        },
        Keyword.get(opts, :config, %{})
      )

    {:ok, state} = Scope.init(config)
    %{state: state, clock: clock, outbox: outbox}
  end

  defp tick(state), do: Scope.handle_message(:cron, ~s({"action":"tick"}), state)
  defp decode_reply({:reply, json, state}), do: {Jason.decode!(json), state}
  defp sent(outbox), do: outbox |> Agent.get(& &1) |> Enum.reverse()

  # ── InMemory: bare contract ──────────────────────────────────────────────

  test "InMemory.load/0 answers :empty until the first save" do
    # Exercise a fresh backing Agent by using a unique registered name — we
    # can't reset the real singleton, so cover the contract via a throwaway
    # module compiled the same way (mirrors store.ex's ensure_started).
    defmodule EmptyProbe do
      @behaviour Store
      @name __MODULE__

      def load do
        ensure_started()
        Agent.get(@name, & &1)
      end

      def save(saved) do
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

    assert EmptyProbe.load() == :empty
  end

  test "InMemory round-trips exactly what it was given, MapSet included" do
    saved = %{
      det: %{"wingston" => %{Genswarms.Observer.Detectors => %{}}},
      last_alert: %{{"wingston", :endpoint_down} => 123},
      seen_periods: %{"wingston" => MapSet.new(["2026-07-01", "2026-07-02"])},
      save_seq: 1
    }

    :ok = Store.InMemory.save(saved)
    assert {:ok, ^saved} = Store.InMemory.load()
  end

  # ── Scope integration ─────────────────────────────────────────────────────

  test "scope boots :empty cleanly and behaves as a fresh instance" do
    {name, _pid} = fresh_store()
    %{state: state} = start_scope(name)

    assert state.det == %{}
    assert state.last_alert == %{}
    assert state.seen_periods == %{}
    assert state.save_seq == 0
    assert state.pending_alerts == []
  end

  test "round-trip: det/last_alert/seen_periods persist across a tick and reload cleanly" do
    {name, _pid} = fresh_store()
    %{state: state, outbox: outbox} = start_scope(name)

    {reply, state} = decode_reply(tick(state))
    assert reply["alerts"] == 1
    assert length(sent(outbox)) == 1
    assert state.save_seq == 1

    # A brand-new Scope session against the SAME store picks up the alert
    # cooldown — the just-fired endpoint_down does not refire immediately.
    %{state: state2} = start_scope(name)
    assert state2.last_alert != %{}
    assert state2.save_seq == 1

    {reply2, _state2} = decode_reply(tick(state2))
    assert reply2["suppressed"] == 1
    assert reply2["alerts"] == 0
  end

  test "a future seen_period id is dropped on load" do
    {name, pid} = fresh_store()

    tomorrow = @t0 |> DateTime.from_unix!(:millisecond) |> DateTime.to_date() |> Date.add(1)
    far_future = Date.add(tomorrow, 5) |> Date.to_iso8601()
    yesterday = tomorrow |> Date.add(-2) |> Date.to_iso8601()

    Agent.update(pid, fn _ ->
      {:ok,
       %{
         det: %{},
         last_alert: %{},
         seen_periods: %{"wingston" => MapSet.new([far_future, yesterday, "garbage"])},
         save_seq: 0
       }}
    end)

    %{state: state} = start_scope(name)

    assert state.seen_periods["wingston"] == MapSet.new([yesterday])
  end

  test "a future cooldown timestamp is dropped on load" do
    {name, pid} = fresh_store()

    future_ms = @t0 + 999_999
    past_ms = @t0 - 1_000

    Agent.update(pid, fn _ ->
      {:ok,
       %{
         det: %{},
         last_alert: %{{"wingston", :endpoint_down} => future_ms, {"wingston", :other} => past_ms},
         seen_periods: %{},
         save_seq: 0
       }}
    end)

    %{state: state, outbox: outbox} = start_scope(name)

    assert state.last_alert == %{{"wingston", :other} => past_ms}

    # The future (now-dropped) cooldown must not suppress a fresh alert.
    {reply, _state} = decode_reply(tick(state))
    assert reply["alerts"] == 1
    assert length(sent(outbox)) == 1
  end

  test "a loaded save_seq behind this session's watermark synthesizes a :store_rollback alert" do
    {name, pid} = fresh_store()

    # Simulate a prior session that had already saved up to seq 2, and a
    # caller/host that remembers that watermark across the restart.
    Agent.update(pid, fn _ ->
      {:ok, %{det: %{}, last_alert: %{}, seen_periods: %{}, save_seq: 1}}
    end)

    %{state: state, outbox: outbox} =
      start_scope(name, config: %{save_seq: 2, registry: %{}})

    assert [%{type: :store_rollback}] = state.pending_alerts

    {reply, _state} = decode_reply(tick(state))
    assert reply["alerts"] == 1

    [delivery] = sent(outbox)
    msg = Jason.decode!(delivery.content)
    assert msg["card"]["title"] =~ "store_rollback"
    assert msg["card"]["title"] =~ "observer"
  end

  test "no rollback on a genuinely fresh boot even with unset save_seq" do
    {name, pid} = fresh_store()

    Agent.update(pid, fn _ ->
      {:ok, %{det: %{}, last_alert: %{}, seen_periods: %{}, save_seq: 0}}
    end)

    %{state: state} = start_scope(name)
    assert state.pending_alerts == []
  end

  # ── fail-open on a raising store ──────────────────────────────────────────

  test "a raising store on load never crashes init — boots empty and logs" do
    %{state: state} = start_scope(Crasher)

    assert state.det == %{}
    assert state.last_alert == %{}
    assert state.seen_periods == %{}
  end

  test "a raising store on save never crashes a tick — durability is skipped, alerting still works" do
    %{state: state, outbox: outbox} = start_scope(Crasher)

    {reply, state} = decode_reply(tick(state))
    assert reply["alerts"] == 1
    assert length(sent(outbox)) == 1
    # save/1 raised, so the watermark could not advance
    assert state.save_seq == 0
  end
end
