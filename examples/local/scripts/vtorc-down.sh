#!/bin/bash

source ./env.sh

echo "Stopping vtorc."
kill -9 "$(cat "$VTDATAROOT/tmp/vtorc.pid")"

