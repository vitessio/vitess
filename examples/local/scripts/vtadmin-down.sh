#!/bin/bash

source ./env.sh

echo "Stopping vtadmin..."
kill -9 "$(cat "$VTDATAROOT/tmp/vtadmin.pid")"
