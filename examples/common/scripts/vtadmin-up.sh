#!/bin/bash

script_dir="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${script_dir}/../env.sh"

cluster_name="local"
log_dir="${VTDATAROOT}/tmp"
web_dir="${script_dir}/../../../web/vtadmin"

vtadmin_api_port=14200
vtadmin_web_port=14201

vtadmin \
  --addr ":${vtadmin_api_port}" \
  --http-origin "http://localhost:${vtadmin_web_port}" \
  --http-tablet-url-tmpl "http://{{ .Tablet.Hostname }}:15{{ .Tablet.Alias.Uid }}" \
  --tracer "opentracing-jaeger" \
  --grpc-tracing \
  --http-tracing \
  --logtostderr \
  --alsologtostderr \
  --rbac \
  --rbac-config="${script_dir}/../vtadmin/rbac.yaml" \
  --cluster "id=${cluster_name},name=${cluster_name},discovery=staticfile,discovery-staticfile-path=${script_dir}/../vtadmin/discovery.json,tablet-fqdn-tmpl={{ .Tablet.Hostname }}:15{{ .Tablet.Alias.Uid }}" \
  > "${log_dir}/vtadmin-api.out" 2>&1 &

vtadmin_api_pid=$!
echo ${vtadmin_api_pid} > "${log_dir}/vtadmin-api.pid"

echo "\
vtadmin-api is running!
  - API: http://localhost:${vtadmin_api_port}
  - Logs: ${log_dir}/vtadmin-api.out
  - PID: ${vtadmin_api_pid}
"

# Wait for vtadmin to successfully discover the cluster
expected_cluster_result="{\"result\":{\"clusters\":[{\"id\":\"${cluster_name}\",\"name\":\"${cluster_name}\"}]},\"ok\":true}"
for _ in {0..300}; do
  result=$(curl -s "http://localhost:${vtadmin_api_port}/api/clusters")
  if [[ ${result} == "${expected_cluster_result}" ]]; then
    break
  fi
  sleep 0.1
done

# Check one last time
[[ $(curl -s "http://localhost:${vtadmin_api_port}/api/clusters") == "${expected_cluster_result}" ]] || fail "vtadmin failed to discover the running example Vitess cluster."

# As a TODO, it'd be nice to make the assumption that vtadmin-web is already
# installed and built (since we assume that `make` has already been run for
# other Vitess components.)
npm --prefix "$web_dir" --silent install

REACT_APP_VTADMIN_API_ADDRESS="http://localhost:${vtadmin_api_port}" \
  REACT_APP_ENABLE_EXPERIMENTAL_TABLET_DEBUG_VARS="true" \
  npm run --prefix "$web_dir" build

"${web_dir}/node_modules/.bin/serve" --no-clipboard -l $vtadmin_web_port -s "${web_dir}/build" \
  > "${log_dir}/vtadmin-web.out" 2>&1 &

vtadmin_web_pid=$!
echo ${vtadmin_web_pid} > "${log_dir}/vtadmin-web.pid"

echo "\
vtadmin-web is running!
  - Browser: http://localhost:${vtadmin_web_port}
  - Logs: ${log_dir}/vtadmin-web.out
  - PID: ${vtadmin_web_pid}
"
