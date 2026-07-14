defmodule Genswarms.Observer.Client.HttpTest do
  use ExUnit.Case, async: false

  alias Genswarms.Observer.Client.Http

  setup do
    {:ok, _} = Application.ensure_all_started(:inets)
    :ok
  end

  test "reads dashboard and events with bearer auth and returns unauthorized status" do
    base_url = start_server!()

    assert {:ok, %{"status" => "running", "summary" => %{"pool" => %{}}}} =
             Http.get_dashboard(base_url, "micro-markets", "smoke-token", timeout_ms: 1_000)

    assert {:ok, [%{"level" => "info"}]} =
             Http.get_events(base_url, "micro-markets", "smoke-token", timeout_ms: 1_000)

    assert {:error, {:http_status, 401, "unauthorized"}} =
             Http.get_dashboard(base_url, "micro-markets", nil, timeout_ms: 1_000)
  end

  defp start_server! do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])
    {:ok, {_address, port}} = :inet.sockname(listener)
    server = spawn(fn -> serve(listener, 3) end)

    on_exit(fn ->
      :gen_tcp.close(listener)

      if Process.alive?(server) do
        Process.exit(server, :normal)
      end
    end)

    "http://127.0.0.1:#{port}"
  end

  defp serve(listener, remaining) when remaining > 0 do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        request = read_request(socket, "")
        :ok = :gen_tcp.send(socket, response(request))
        :ok = :gen_tcp.close(socket)
        serve(listener, remaining - 1)

      {:error, :closed} ->
        :ok
    end
  end

  defp serve(_listener, 0), do: :ok

  defp read_request(socket, received) do
    case :gen_tcp.recv(socket, 0, 1_000) do
      {:ok, chunk} ->
        request = received <> chunk
        if String.contains?(request, "\r\n\r\n"), do: request, else: read_request(socket, request)

      {:error, reason} ->
        raise "HTTP test server could not read request: #{inspect(reason)}"
    end
  end

  defp response(request) do
    case request do
      "GET /api/swarms/micro-markets/dashboard HTTP/1.1\r\n" <> rest ->
        if authenticated?(rest) do
          ok(%{"status" => "running", "summary" => %{"pool" => %{}}})
        else
          unauthorized()
        end

      "GET /api/swarms/micro-markets/events HTTP/1.1\r\n" <> rest ->
        if authenticated?(rest),
          do: ok(%{"events" => [%{"level" => "info"}]}),
          else: unauthorized()

      _ ->
        "HTTP/1.1 404 Not Found\r\ncontent-length: 9\r\nconnection: close\r\n\r\nnot found"
    end
  end

  defp authenticated?(headers),
    do: String.contains?(headers, "authorization: Bearer smoke-token\r\n")

  defp ok(body) do
    json = Jason.encode!(body)

    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: #{byte_size(json)}\r\nconnection: close\r\n\r\n#{json}"
  end

  defp unauthorized,
    do: "HTTP/1.1 401 Unauthorized\r\ncontent-length: 12\r\nconnection: close\r\n\r\nunauthorized"
end
