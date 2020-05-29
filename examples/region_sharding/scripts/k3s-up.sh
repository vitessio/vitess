#!/bin/bash

# Copyright 2019 The Vitess Authors.
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

# This is an example script that creates a Kubernetes api for topo use by running k3s

set -e
cell=${CELL:-'test'}

script_root=$(dirname "${BASH_SOURCE[0]}")

# shellcheck source=./env.sh
# shellcheck disable=SC1091
source ./env.sh

case $(uname) in
  Linux) ;;
  *) echo "WARNING: unsupported platform. K3s only supports running on Linux, the k8s topology is available for local examples."; exit 1;;
esac

case $(uname -m) in
    aarch64) ;;
    x86_64) ;;
    *) echo "ERROR: unsupported architecture, the k8s topology is not available for local examples."; exit 1;;
esac

k3s server --disable-agent --data-dir "${VTDATAROOT}/k3s/" --https-listen-port "${K8S_PORT}" --write-kubeconfig "${K8S_KUBECONFIG}" > "${VTDATAROOT}"/tmp/k3s.out 2>&1 &
PID=$!
echo $PID > "${VTDATAROOT}/tmp/k3s.pid"
disown -a
echo "Waiting for k3s server to start"
sleep 15

# Use k3s built-in kubectl with custom config
KUBECTL="k3s kubectl --kubeconfig=${K8S_KUBECONFIG}"

# Create the CRD for vitesstopologynodes
$KUBECTL create -f ../../go/vt/topo/k8stopo/VitessTopoNodes-crd.yaml

# Add the CellInfo description for the cell
set +e
echo "add $cell CellInfo"
vtctl $TOPOLOGY_FLAGS AddCellInfo \
  -root /vitess/$cell \
  $cell
set -e

echo "k3s start done..."
