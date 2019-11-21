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

# This is an example script that creates etcd clusters.
# Vitess requires a global cluster, as well as one for each cell.
#
# For automatic discovery, an etcd cluster can be bootstrapped from an
# existing cluster. In this example, we use an externally-run discovery
# service, but you can use your own. See the etcd docs for more:
# https://github.com/coreos/etcd/blob/v2.0.13/Documentation/clustering.md

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

replicas=${ETCD_REPLICAS:-3}
cells=`echo $CELLS | tr ',' ' '`

# Check the installation for etcd-operator has been done.
# To make sure the EtcdClusters CRD not only exists but is ready to serve,
# we try to list EtcdClusters. This will succeed even if the list is empty.
if ! kubectl get etcdclusters.etcd.database.coreos.com &> /dev/null ; then
  # etcd-operator is a shared resource, and installing it on a secure
  # cluster requires granting RBAC permissions.
  # We therefore require the user to do this before running Vitess.
  echo
  echo "Please install etcd-operator in the same namespace as Vitess"
  echo "before running etcd-up.sh. See the instructions here:"
  echo
  echo "  https://github.com/coreos/etcd-operator/blob/master/doc/user/install_guide.md"
  echo
  echo "If you already installed etcd-operator and still get this prompt,"
  echo "check for any errors in etcd-operator logs:"
  echo
  echo "  kubectl logs -l name=etcd-operator"
  echo
  exit 1
fi

for cell in 'global' $cells; do
  # Create the etcd cluster using etcd-operator
  echo "Creating etcd service for '$cell' cell..."
  sed -e "s/{{cell}}/$cell/g" -e "s/{{replicas}}/$replicas/g" \
    etcd-service-template.yaml | \
    $KUBECTL $KUBECTL_OPTIONS create -f -
done

