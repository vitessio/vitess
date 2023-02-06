#!/bin/bash

source "$(dirname ${BASH_SOURCE})/../env.sh"

echo "Stopping vtorc."
kill -9 "$(cat "$VTDATAROOT/tmp/vtorc.pid")"

