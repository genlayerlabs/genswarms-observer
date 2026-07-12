defmodule Genswarms.Observer.Ingest do
  @moduledoc """
  The observer's read side for one swarm tick: dashboard + raw events + the
  display feed, with feed pagination drained to head under a page budget.

  Pure with respect to Scope state: the feed cursor comes IN as an argument
  and goes OUT as a proposal (`nil` = do not move). Committing the proposal
  is the caller's decision — Scope ties it to detector success (F1), which
  is exactly why this module must not commit it here.

  Both the first read (cursor `nil` → drain from 0) and steady state
  (integer cursor → drain from it) use the same loop: F5 showed that a
  single-page steady read splits an open/reply pair across ticks whenever
  a backlog exceeds one server page. Draining to head hands detectors the
  whole backlog as one batch, so answered pairs cancel inside one detect/2.

  Any mid-drain failure discards the partial union and reports the error
  with a nil proposal — a partial window would recreate the page-boundary
  false alert the drain exists to prevent. Nothing is lost: the unchanged
  cursor re-drains next tick.
  """

  require Logger

  @doc """
  See moduledoc. Returns `{%{dashboard:, events:, feed:}, proposed_cursor}`.

  `swarm` is the observer-side identity (the registry key — alert titles,
  dedupe, MCP addressing). What goes on the WIRE is the entry's `"name"`
  when set: a fleet can watch two deployments of the same swarm (e.g. a
  local `wingston` and the prod one) under distinct registry keys while
  each backend still only answers to its own wire name. Without the
  override the key doubles as the wire name, as before.
  """
  def fetch(client, client_opts, swarm, entry, cursor, max_pages) do
    token = resolve_token(entry)
    base = entry["dashboard_url"]
    wire = wire_name(entry, swarm)

    {feed, proposed} = drain_feed(client, client_opts, wire, base, token, cursor || 0, [], max_pages)

    data = %{
      dashboard: safe_call(client, :get_dashboard, [base, wire, token, client_opts]),
      events: safe_call(client, :get_events, [base, wire, token, client_opts]),
      feed: feed
    }

    {data, proposed}
  end

  @doc "The name the observed backend answers to: entry `\"name\"` override or the registry key."
  def wire_name(entry, swarm) when is_map(entry) do
    case entry["name"] do
      name when is_binary(name) and name != "" -> name
      _ -> to_string(swarm)
    end
  end

  def wire_name(_entry, swarm), do: to_string(swarm)

  defp drain_feed(client, opts, swarm, base, token, since, acc, pages_left) do
    case safe_call(client, :get_events_feed, [base, swarm, since, token, opts]) do
      {:ok, %{events: events, seq: seq}} when is_list(events) and is_integer(seq) and seq >= 0 ->
        cond do
          seq < since ->
            drain_feed(client, opts, swarm, base, token, 0, [], max(pages_left - 1, 1))

          events == [] ->
            {{:ok, acc}, seq}

          seq <= since ->
            {{:ok, acc ++ events}, seq}

          pages_left <= 1 ->
            {{:ok, acc ++ events}, seq}

          true ->
            drain_feed(client, opts, swarm, base, token, seq, acc ++ events, pages_left - 1)
        end

      :unavailable ->
        {:unavailable, nil}

      {:error, reason} ->
        {{:error, reason}, nil}

      other ->
        {{:error, {:bad_feed_return, other}}, nil}
    end
  end

  # A crashing client must read as an error, never take the object down.
  defp safe_call(client, fun, args) do
    apply(client, fun, args)
  rescue
    e -> {:error, {:client_crash, Exception.message(e)}}
  catch
    :exit, reason -> {:error, {:client_exit, reason}}
  end

  def resolve_token(entry) do
    case entry["token_env"] do
      env when is_binary(env) and env != "" ->
        case System.get_env(env) do
          t when is_binary(t) and t != "" -> t
          _ -> nil
        end

      _ ->
        nil
    end
  end
end
