#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"

echo "Stopping vtorc..."

stop_process "vttablet" "$VTDATAROOT/tmp/vtorc.pid"

