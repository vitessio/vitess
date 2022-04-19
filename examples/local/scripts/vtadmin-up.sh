#!/bin/bash

source ./env.sh

log_dir="${VTDATAROOT}/tmp"
vtadmin_api_port=14200

vtadmin \
  --addr ":${vtadmin_api_port}" \
  --http-origin "http://localhost:3000" \
  --http-tablet-url-tmpl "http://{{ .Tablet.Hostname }}:15{{ .Tablet.Alias.Uid }}" \
  --tracer "opentracing-jaeger" \
  --grpc-tracing \
  --http-tracing \
  --logtostderr \
  --alsologtostderr \
  --cluster "id=local,name=local,discovery=staticfile,discovery-staticfile-path=./vtadmin/discovery.json,tablet-fqdn-tmpl={{ .Tablet.Hostname }}:15{{ .Tablet.Alias.Uid }}" \
  > "${log_dir}/vtadmin-api.out" 2>&1 &
vtadmin_pid=$!

function cleanup() {
  kill -9 "${vtadmin_pid}"

  echo
  echo "Shutdown complete!"
}

trap cleanup INT QUIT TERM

echo "vtadmin-api is running on http://localhost:${vtadmin_api_port}. Logs are in ${log_dir}/vtadmin-api.out, and its PID is ${vtadmin_pid}"

(
  cd ../../web/vtadmin &&
  npm install &&
  REACT_APP_VTADMIN_API_ADDRESS="http://127.0.0.1:${vtadmin_api_port}" \
  REACT_APP_ENABLE_EXPERIMENTAL_TABLET_DEBUG_VARS="true" \
    npm run start
)
