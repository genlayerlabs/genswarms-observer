#!/usr/bin/env bash
# Levanta el entorno de observación para desarrollar el observer:
#   1. engine REST en :4000 (tokens de demo: fleet-full-token / fleet-config-token)
#   2. el swarm de prueba jmlago-genswarms-fleet-mcp (dashboard :4994,
#      objetos dashboard+mcp-gateway por require-mode)
# El MCP genswarms-fleet (ya instalado en Claude Code) ve ambos vía
# ~/.config/genswarms/fleet.json.
set -euo pipefail

ENGINE=~/docs/personal/genswarms
SWARM_CFG=/home/jm/docs/personal/strategivm/swarms/jmlago/genswarms-fleet-mcp/swarm.exs

if curl -s -o /dev/null http://127.0.0.1:4000/ --max-time 2; then
  echo "engine ya escucha en :4000"
else
  echo "arrancando engine (log: /tmp/engine-daemon.log)…"
  (cd "$ENGINE" && env \
    GENSWARMS_API_TOKEN=fleet-full-token \
    GENSWARMS_CONFIG_API_TOKEN=fleet-config-token \
    GENSWARMS_SWARM_CONFIG_DIR=/home/jm/docs/personal/strategivm/swarms \
    nohup mix run --no-halt -e 'Genswarms.Application.start_web_server(port: 4000)' \
    > /tmp/engine-daemon.log 2>&1 &)
  until curl -s -o /dev/null http://127.0.0.1:4000/ --max-time 2; do sleep 2; done
  echo "engine arriba"
fi

curl -s -X POST http://127.0.0.1:4000/api/swarms \
  -H "Authorization: Bearer fleet-full-token" \
  -H "Content-Type: application/json" \
  -d "{\"config_path\":\"$SWARM_CFG\"}" --max-time 180
echo
echo "target listo: dashboard http://127.0.0.1:4994/api/swarms/jmlago-genswarms-fleet-mcp/dashboard"
