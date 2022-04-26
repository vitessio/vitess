#!/bin/bash

source ./env.sh

log_dir="${VTDATAROOT}/tmp"

vtadmin_api_port=14200

vtadmin \
  --addr ":${vtadmin_api_port}" \
  --http-tablet-url-tmpl "http://{{ .Tablet.Hostname }}:15{{ .Tablet.Alias.Uid }}" \
  --tracer "opentracing-jaeger" \
  --grpc-tracing \
  --http-tracing \
  --logtostderr \
  --alsologtostderr \
  --rbac \
  --rbac-config="./vtadmin/rbac.yaml" \
  --cluster "id=local,name=local,discovery=staticfile,discovery-staticfile-path=./vtadmin/discovery.json,tablet-fqdn-tmpl={{ .Tablet.Hostname }}:15{{ .Tablet.Alias.Uid }}" \
  > "${log_dir}/vtadmin.out" 2>&1 &

vtadmin_pid=$!
echo ${vtadmin_pid} > "${log_dir}/vtadmin.pid"

echo "\
vtadmin is running!
  - Web: http://localhost:${vtadmin_api_port}
  - API: http://localhost:${vtadmin_api_port}/api
  - Logs: ${log_dir}/vtadmin.out
  - PID: ${vtadmin_pid}
"
