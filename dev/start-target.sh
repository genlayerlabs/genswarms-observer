#!/usr/bin/env bash
# Brings up the observation environment for observer development:
#   1. engine REST on :4000 (demo tokens: fleet-full-token / fleet-config-token)
#   2. the test swarm jmlago-genswarms-fleet-mcp (dashboard :4994,
#      dashboard+mcp-gateway objects via require-mode)
# The genswarms-fleet MCP (already installed in Claude Code) sees both via
# ~/.config/genswarms/fleet.json.
set -euo pipefail

ENGINE=~/docs/personal/genswarms
SWARM_CFG=/home/jm/docs/personal/strategivm/swarms/jmlago/genswarms-fleet-mcp/swarm.exs

if curl -s -o /dev/null http://127.0.0.1:4000/ --max-time 2; then
  echo "engine already listening on :4000"
else
  echo "starting engine (log: /tmp/engine-daemon.log)…"
  (cd "$ENGINE" && env \
    GENSWARMS_API_TOKEN=fleet-full-token \
    GENSWARMS_CONFIG_API_TOKEN=fleet-config-token \
    GENSWARMS_SWARM_CONFIG_DIR=/home/jm/docs/personal/strategivm/swarms \
    nohup mix run --no-halt -e 'Genswarms.Application.start_web_server(port: 4000)' \
    > /tmp/engine-daemon.log 2>&1 &)
  until curl -s -o /dev/null http://127.0.0.1:4000/ --max-time 2; do sleep 2; done
  echo "engine up"
fi

curl -s -X POST http://127.0.0.1:4000/api/swarms \
  -H "Authorization: Bearer fleet-full-token" \
  -H "Content-Type: application/json" \
  -d "{\"config_path\":\"$SWARM_CFG\"}" --max-time 180
echo
echo "target ready: dashboard http://127.0.0.1:4994/api/swarms/jmlago-genswarms-fleet-mcp/dashboard"
