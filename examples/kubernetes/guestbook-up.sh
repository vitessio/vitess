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

# This is an example script that starts a guestbook replicationcontroller.

set -e

port=${GUESTBOOK_PORT:-8080}
cell=${GUESTBOOK_CELL:-"test"}
keyspace=${GUESTBOOK_KEYSPACE:-"test_keyspace"}
vtgate_port=${VTGATE_PORT:-15991}

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Creating guestbook service..."
$KUBECTL $KUBECTL_OPTIONS create -f guestbook-service.yaml

sed_script=""
for var in port cell keyspace vtgate_port; do
  sed_script+="s,{{$var}},${!var},g;"
done

echo "Creating guestbook replicationcontroller..."
sed -e "$sed_script" guestbook-controller-template.yaml | $KUBECTL $KUBECTL_OPTIONS create -f -
