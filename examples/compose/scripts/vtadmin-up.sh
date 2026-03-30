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

set -euo pipefail

cluster_name="vitess-example"

echo "Starting vtadmin..."
exec vtadmin \
  --addr "0.0.0.0:14200" \
  --http-origin "http://localhost:14201" \
  --http-tablet-url-tmpl "http://{{ .Tablet.Hostname }}:15000" \
  --log-format text \
  --rbac \
  --rbac-config=/vt/config/vtadmin/rbac.yaml \
  --cluster "id=${cluster_name},name=${cluster_name},discovery=staticfile,discovery-staticfile-path=/vt/config/vtadmin/discovery.json,tablet-fqdn-tmpl=http://localhost:15{{ .Tablet.Alias.Uid }},schema-cache-default-expiration=1m"
