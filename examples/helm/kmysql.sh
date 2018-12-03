#!/bin/bash

# Copyright 2018 The Vitess Authors.
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

# This is a convenience script to run vtctlclient against the local example.

host=$(minikube service vtgate-zone1 --format "{{.IP}}" | tail -n 1)
port=$(minikube service vtgate-zone1 --format "{{.Port}}" | tail -n 1)

mysql -h "$host" -P "$port" $*
