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

# We should not assume that any of the steps have been executed.
# This makes it possible for a user to cleanup at any point.

source ./env.sh

./scripts/vtgate-down.sh

for tablet in 100 200 300 400; do
	if vtctlclient -action_timeout 1s -server localhost:15999 GetTablet zone1-$tablet >/dev/null 2>&1; then
		# The zero tablet is up. Try to shutdown 0-2 tablet + mysqlctl
		for i in 0 1 2; do
			uid=$(($tablet + $i))
			echo "Shutting down tablet zone1-$uid"
			CELL=zone1 TABLET_UID=$uid ./scripts/vttablet-down.sh
			echo "Shutting down mysql zone1-$uid"
			CELL=zone1 TABLET_UID=$uid ./scripts/mysqlctl-down.sh
		done
	fi
done

./scripts/vtctld-down.sh

if [ "${TOPO}" = "zk2" ]; then
	CELL=zone1 ./scripts/zk-down.sh
elif [ "${TOPO}" = "k8s" ]; then
	CELL=zone1 ./scripts/k3s-down.sh
else
	CELL=zone1 ./scripts/etcd-down.sh
fi

# pedantic check: grep for any remaining processes

if [ ! -z "$VTDATAROOT" ]; then

	if pgrep -f -l "$VTDATAROOT" >/dev/null; then
		echo "ERROR: Stale processes detected! It is recommended to manuallly kill them:"
		pgrep -f -l "$VTDATAROOT"
	else
		echo "All good! It looks like every process has shut down"
	fi

	# shellcheck disable=SC2086
	rm -r ${VTDATAROOT:?}/*

fi

disown -a
