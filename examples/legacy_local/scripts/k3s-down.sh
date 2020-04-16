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

# This is an example script that stops the k3s server started by k3s-up.sh.

set -e

# shellcheck source=./env.sh
# shellcheck disable=SC1091
source ./env.sh

# Stop K3s server.
echo "Stopping k3s server..."

pid=`cat $VTDATAROOT/tmp/k3s.pid`
echo "Stopping k3s..."
kill -9 $pid
