#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"

echo "Stopping vtadmin-web..."
kill -9 "$(cat "$VTDATAROOT/tmp/vtadmin-web.pid")"

echo "Stopping vtadmin-api..."
kill -9 "$(cat "$VTDATAROOT/tmp/vtadmin-api.pid")"
