#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"

function stop_vtadmin() {
  local name="$1"
  local file="$2"

  if [[ -e "$file" ]]; then
    echo "Stopping $name..."
    local pid=$(cat "$file")
    kill $pid
    # Wait for the process to terminate
    while ps -p $pid > /dev/null; do
      sleep 1
    done
  else
    echo "Skipping stopping $name because no pid file."
  fi
}

stop_vtadmin "vtadmin-web" "$VTDATAROOT/tmp/vtadmin-web.pid"
stop_vtadmin "vtadmin-api" "$VTDATAROOT/tmp/vtadmin-api.pid"
