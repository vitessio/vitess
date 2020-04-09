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

source build.env

function fail() {
  echo "ERROR: $1"
  exit 1
}

# These binaries are required to 'make test'
# mysqld might be in /usr/sbin which will not be in the default PATH
PATH="/usr/sbin:$PATH"
for binary in k3s mysqld consul etcd etcdctl zksrv.sh javadoc mvn ant curl wget zip unzip; do
  command -v "$binary" > /dev/null || fail "${binary} is not installed in PATH. See https://vitess.io/contributing/build-from-source for install instructions."
done;
