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

source "$(dirname "${BASH_SOURCE[0]:-$0}")/lib/utils.sh"

#hostname=$(hostname -f)
hostname="127.0.0.1"
vtctld_web_port=15000
export VTDATAROOT="${VTDATAROOT:-${PWD}/vtdataroot}"

if [[ $EUID -eq 0 ]]; then
  fail "This script refuses to be run as root. Please switch to a regular user."
fi

# mysqld might be in /usr/sbin which will not be in the default PATH
PATH="/usr/sbin:$PATH"
for binary in mysqld etcd etcdctl curl vtctlclient vttablet vtgate vtctld mysqlctl; do
  command -v "$binary" > /dev/null || fail "${binary} is not installed in PATH. See https://vitess.io/docs/get-started/local/ for install instructions."
done;

if [ "${TOPO}" = "zk2" ]; then
    # Each ZooKeeper server needs a list of all servers in the quorum.
    # Since we're running them all locally, we need to give them unique ports.
    # In a real deployment, these should be on different machines, and their
    # respective hostnames should be given.
    zkcfg=(\
        "1@$hostname:28881:38881:21811" \
        "2@$hostname:28882:38882:21812" \
        "3@$hostname:28883:38883:21813" \
        )
    printf -v zkcfg ",%s" "${zkcfg[@]}"
    zkcfg=${zkcfg:1}

    zkids='1 2 3'

    # Set topology environment parameters.
    ZK_SERVER="localhost:21811,localhost:21812,localhost:21813"
    # shellcheck disable=SC2034
    TOPOLOGY_FLAGS="--topo_implementation zk2 --topo_global_server_address ${ZK_SERVER} --topo_global_root /vitess/global"

    mkdir -p "${VTDATAROOT}/tmp"
elif [ "${TOPO}" = "k8s" ]; then
    # Set topology environment parameters.
    K8S_ADDR="localhost"
    K8S_PORT="8443"
    K8S_KUBECONFIG=$VTDATAROOT/tmp/k8s.kubeconfig
    # shellcheck disable=SC2034
    TOPOLOGY_FLAGS="--topo_implementation k8s --topo_k8s_kubeconfig ${K8S_KUBECONFIG} --topo_global_server_address ${K8S_ADDR}:${K8S_PORT} --topo_global_root /vitess/global"
elif [ "${TOPO}" = "consul" ]; then
    # Set up topology environment parameters.
    CONSUL_SERVER=127.0.0.1
    CONSUL_HTTP_PORT=8500
    CONSUL_SERVER_PORT=8300
    TOPOLOGY_FLAGS="--topo_implementation consul --topo_global_server_address ${CONSUL_SERVER}:${CONSUL_HTTP_PORT} --topo_global_root vitess/global/"
    mkdir -p "${VTDATAROOT}/consul"
else
    ETCD_SERVER="localhost:2379"
    TOPOLOGY_FLAGS="--topo_implementation etcd2 --topo_global_server_address $ETCD_SERVER --topo_global_root /vitess/global"

    mkdir -p "${VTDATAROOT}/etcd"
fi

mkdir -p "${VTDATAROOT}/tmp"

# Set aliases to simplify instructions.
# In your own environment you may prefer to use config files,
# such as ~/.my.cnf

alias mysql="command mysql -h 127.0.0.1 -P 15306"
alias vtctlclient="command vtctlclient --server localhost:15999 --log_dir ${VTDATAROOT}/tmp --alsologtostderr"
alias vtctldclient="command vtctldclient --server localhost:15999"

# Make sure aliases are expanded in non-interactive shell
shopt -s expand_aliases

