#!/bin/bash

# Copyright 2026 The Vitess Authors.
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

# Runs SQLancer (built from a checkout with vitess-mode.patch applied) against vtgate and summarizes the failures.
#
# usage: run.sh <out-dir> <seconds> <keyspaces> [sqlancer mysql options...]
#   run.sh out/unsharded 300 u0,u1 --oracle TLP_WHERE
#   run.sh out/sharded   300 s0,s1 --vitess-sharded --oracle TLP_WHERE
#
# One SQLancer thread runs per keyspace. Errors matching known_limitations.txt are treated as expected.

set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
: "${SQLANCER_JAR:?set SQLANCER_JAR to the sqlancer-*.jar built with vitess-mode.patch}"
VTGATE_PORT=${VTGATE_PORT:-33573}
out=$1 secs=$2 keyspaces=$3
shift 3
threads=$(tr ',' '\n' <<<"$keyspaces" | wc -l)

mkdir -p "$out"
cd "$out"
java -jar "$SQLANCER_JAR" \
  --host 127.0.0.1 --port "$VTGATE_PORT" --username root --password "" \
  --use-connection-test false --database-prefix try \
  --num-threads "$threads" --num-tries 100 --timeout-seconds "$secs" --print-progress-summary true \
  mysql --vitess --vitess-keyspaces "$keyspaces" --vitess-ignore-errors-file "$here/known_limitations.txt" "$@" \
  >sqlancer.out 2>&1 || true
python3 "$here/summarize.py" logs/mysql
