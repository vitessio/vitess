#!/bin/bash

# Copyright 2017 Google Inc.
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

# This is an example script that tears down the etcd servers started by
# etcd-up.sh.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

replicas=${ETCD_REPLICAS:-3}
cells=`echo $CELLS | tr ',' ' '`

# Delete etcd clusters
for cell in 'global' $cells; do
  echo "Stopping etcd cluster for $cell cell..."
  sed -e "s/{{cell}}/$cell/g" -e "s/{{replicas}}/$replicas/g" \
    etcd-service-template.yaml | \
    $KUBECTL $KUBECTL_OPTIONS delete -f -
done

# Note we keep etcd-operator installed for future use.
