#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"

echo "Stopping vtorc."
kill -9 "$(cat "$VTDATAROOT/tmp/vtorc.pid")"

