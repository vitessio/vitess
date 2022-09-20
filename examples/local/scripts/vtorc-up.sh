#!/bin/bash

source ./env.sh

log_dir="${VTDATAROOT}/tmp"
web_dir="../../web/vtorc"
vtorc_web_port=16000

vtorc \
  $TOPOLOGY_FLAGS \
  --orc_web_dir "${web_dir}" \
  --logtostderr \
  --alsologtostderr \
  --config="./vtorc/config.json" \
  > "${log_dir}/vtorc.out" 2>&1 &

vtorc_pid=$!
echo ${vtorc_pid} > "${log_dir}/vtorc.pid"

echo "\
vtorc is running!
  - UI: http://localhost:${vtorc_web_port}
  - Logs: ${log_dir}/vtorc.out
  - PID: ${vtorc_pid}
"
