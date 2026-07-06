defmodule Genswarms.Observer.Client.Http do
  @moduledoc """
  Real dashboard-backend reader over :httpc (OTP inets — no HTTP dep).

  Opts: `timeout_ms` (default 5_000).
  """

  @behaviour Genswarms.Observer.Client

  @impl true
  def get_dashboard(base_url, swarm, token, opts) do
    get_json("#{base_url}/api/swarms/#{swarm}/dashboard", token, opts)
  end

  @impl true
  def get_events(base_url, swarm, token, opts) do
    case get_json("#{base_url}/api/swarms/#{swarm}/events", token, opts) do
      {:ok, %{"events" => events}} when is_list(events) -> {:ok, events}
      {:ok, other} -> {:error, {:bad_events_envelope, other}}
      error -> error
    end
  end

  @impl true
  def get_session_history(base_url, swarm, cid, token, opts) do
    get_json("#{base_url}/api/swarms/#{swarm}/sessions/#{URI.encode_www_form(cid)}/history", token, opts)
  end

  defp get_json(url, token, opts) do
    timeout = Keyword.get(opts, :timeout_ms, 5_000)

    headers =
      case token do
        t when is_binary(t) and t != "" -> [{~c"authorization", String.to_charlist("Bearer " <> t)}]
        _ -> []
      end

    request = {String.to_charlist(url), headers}
    http_opts = [timeout: timeout, connect_timeout: timeout]

    case :httpc.request(:get, request, http_opts, body_format: :binary) do
      {:ok, {{_http, 200, _reason}, _headers, body}} ->
        case Jason.decode(body) do
          {:ok, json} -> {:ok, json}
          {:error, _} -> {:error, {:bad_json, String.slice(body, 0, 200)}}
        end

      {:ok, {{_http, status, _reason}, _headers, body}} ->
        {:error, {:http_status, status, String.slice(to_string(body), 0, 200)}}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
