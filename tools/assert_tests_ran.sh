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

# gotestsum --post-run-command hook: a test run that executed zero tests is
# never a pass. go test exits 0 when flag ordering or a -run regex leaves it
# with nothing to run, so without this guard such a shard stays silently green.
# TESTS_TOTAL is set by gotestsum and counts skipped tests too, so a shard
# whose tests all t.Skip does not trip this.
if [[ "${TESTS_TOTAL:-0}" -eq 0 ]]; then
	echo "ERROR: gotestsum ran 0 tests for $PACKAGES: flag ordering or -run filtering may have broken package selection"
	exit 1
fi
