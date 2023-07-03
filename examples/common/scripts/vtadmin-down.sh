#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"

if test -e "$VTDATAROOT/tmp/vtadmin-web.pid"; then
  echo "Stopping vtadmin-web..."
  kill -9 "$(cat "$VTDATAROOT/tmp/vtadmin-web.pid")"
else
  echo "Skipping stopping vtadmin-web because no pid file."
fi

if test -e "$VTDATAROOT/tmp/vtadmin-api.pid"; then
  echo "Stopping vtadmin-api..."
  kill -9 "$(cat "$VTDATAROOT/tmp/vtadmin-api.pid")"
else
  echo "Skipping stopping vtadmin-web because no pid file."
fi
