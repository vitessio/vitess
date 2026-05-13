#!/bin/bash

# Copyright 2026 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function output() {
  echo -e "$@"
}

script_dir="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${script_dir}/../env.sh"

cluster_name="local"
log_dir="${VTDATAROOT}/tmp"
vtadmin2_port=14202

case_insensitive_hostname=$(echo "$hostname" | tr '[:upper:]' '[:lower:]')

output "\n\033[1;32mStarting vtadmin2 on http://${case_insensitive_hostname}:${vtadmin2_port}\033[0m"

vtadmin2 \
  --addr "${case_insensitive_hostname}:${vtadmin2_port}" \
  --http-tablet-url-tmpl "http://{{ .Tablet.Hostname }}:15{{ .Tablet.Alias.Uid }}" \
  --logtostderr \
  --alsologtostderr \
  --rbac \
  --rbac-config="${script_dir}/../vtadmin/rbac.yaml" \
  --cluster "id=${cluster_name},name=${cluster_name},discovery=staticfile,discovery-staticfile-path=${script_dir}/../vtadmin/discovery.json,tablet-fqdn-tmpl=http://{{ .Tablet.Hostname }}:15{{ .Tablet.Alias.Uid }},schema-cache-default-expiration=1m" \
  > "${log_dir}/vtadmin2.out" 2>&1 &

vtadmin2_pid=$!
echo ${vtadmin2_pid} > "${log_dir}/vtadmin2.pid"

for _ in {0..100}; do
  if curl -s "http://${case_insensitive_hostname}:${vtadmin2_port}/clusters" | grep -q "${cluster_name}"; then
    break
  fi
  sleep 0.1
done

curl -s "http://${case_insensitive_hostname}:${vtadmin2_port}/clusters" | grep -q "${cluster_name}" || fail "vtadmin2 failed to discover the running example Vitess cluster."

echo "\
vtadmin2 is running!
  - Browser: http://${case_insensitive_hostname}:${vtadmin2_port}
  - Logs: ${log_dir}/vtadmin2.out
  - PID: ${vtadmin2_pid}
"
