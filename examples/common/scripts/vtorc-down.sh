#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"

stop_process "vtorc" "$VTDATAROOT/tmp/vtorc.pid"

