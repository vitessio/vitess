#!/bin/bash

source ./env.sh

log_dir="${VTDATAROOT}/tmp"
vtadmin_api_port=14200

vtadmin \
  --addr ":${vtadmin_api_port}" \
  --http-origin "http://localhost:3000" \
  --logtostderr \
  --alsologtostderr \
  --cluster "id=local,name=local,discovery=staticfile,discovery-staticfile-path=./vtadmin/discovery.json" \
  > "${log_dir}/vtadmin-api.out" 2>&1 &
vtadmin_pid=$!

function cleanup() {
  kill -9 "${vtadmin_pid}"

  echo
  echo "Shutdown complete!"
}

trap cleanup INT QUIT TERM

echo "vtadmin-api is up! Logs are in ${log_dir}/vtadmin-api.out, and its PID is ${vtadmin_pid}"

(
  cd ../../web/vtadmin &&
  npm install &&
  REACT_APP_VTADMIN_API_ADDRESS="http://127.0.0.1:${vtadmin_api_port}" \
    npm run start
)
