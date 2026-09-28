#!/bin/bash

script_dir="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${script_dir}/../env.sh"

cell=${CELL:-'test'}
log_dir="${VTDATAROOT}/tmp"
port=16000

echo "Starting vtorc..."
vtorc \
  $TOPOLOGY_FLAGS \
  --logtostderr \
  --alsologtostderr \
  --config-path="${script_dir}/../vtorc/" \
  --config-name="config.yaml" \
  --config-type="yml" \
  --cell $cell \
  --port $port \
  > "${log_dir}/vtorc.out" 2>&1 &

vtorc_pid=$!
echo ${vtorc_pid} > "${log_dir}/vtorc.pid"

echo "\
vtorc is running!
  - UI: http://localhost:${port}
  - Logs: ${log_dir}/vtorc.out
  - PID: ${vtorc_pid}
"
