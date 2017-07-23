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

# This is a wrapper script that installs and runs the example
# client for the low-level Java interface.

set -e

script_root=`dirname "${BASH_SOURCE}"`

# We have to install the "example" module first because Maven cannot resolve
# them when we run "exec:java". See also: http://stackoverflow.com/questions/11091311/maven-execjava-goal-on-a-multi-module-project
# Install only "example". See also: http://stackoverflow.com/questions/1114026/maven-modules-building-a-single-specific-module
mvn -f $script_root/../../java/pom.xml -pl example -am install -DskipTests
mvn -f $script_root/../../java/example/pom.xml exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="io.vitess.example.VitessClientExample" -Dexec.args="localhost:15991"
