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
# this script brings down the tablets for customer/0 keyspace

source ./init.d/utils.sh

vtctldclient --server=localhost:15999 Reshard --workflow cust2cust --target-keyspace customer complete

# The init.d script supports an optional argument which is the tablet
# to [start|stop|status] on. So we can target the 200 tablets to stop them.
# This does not delete the data, and a [start] again *would* bring them back.

for i in 200 201 202; do
  ./init.d/mysqlctl-vttablet stop $i
done

# Once the tablets are down, we can remove them from our configuration file.
cp ../common/config/vitess.yaml.306-example ~/.vitess.yaml