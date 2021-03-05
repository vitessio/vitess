#!/bin/bash

# Copyright 2021 The Vitess Authors.
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

# getLength gets the number of elements in a comma separated string
function getLength() {
  COUNT=0
  for _ in ${1//,/ }
  do
    ((COUNT++))
  done
  echo "$COUNT"
}

# The first argument to the file is a comma separated list of keyspaces and the second argument is the comma separated list of number of shards.

KEYSPACES="$1"
NUM_SHARDS="$2"

COUNT_KEYSPACES=$(getLength "$KEYSPACES")
COUNT_NUM_SHARDS=$(getLength "$NUM_SHARDS")

# Incase the number of keyspaces and num_shards do not match, throw an error
if [ "$COUNT_KEYSPACES" != "$COUNT_NUM_SHARDS" ]; then
    echo "Incompatible list of keyspaces and number of shards"
    exit 1
fi

# Convert the strings to lists
read -ra KEYSPACES_LIST <<<"${KEYSPACES//,/ }"
read -ra NUM_SHARDS_LIST <<<"${NUM_SHARDS//,/ }"

# create the main schema directory
mkdir /vt/schema/

i=0;
for keyspace in "${KEYSPACES_LIST[@]}";
do
  # create a directory for each keyspace
  mkdir "/vt/schema/$keyspace"
  num_shard=${NUM_SHARDS_LIST[$i]}
  # Create a vschema.json file only if the number of shards are more than 1
  if [[ $num_shard -gt "1" ]]; then
    printf "{\n\t\"sharded\": true\n}" > "/vt/schema/$keyspace/vschema.json"
  fi
  ((i++))
done
