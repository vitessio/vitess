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

source ../common/env.sh

../common/scripts/vtadmin-down.sh

../common/scripts/vtorc-down.sh

../common/scripts/vtgate-down.sh

for tablet in 100 200 300 400; do
	if vtctlclient --action_timeout 1s --server localhost:15999 GetTablet zone1-$tablet >/dev/null 2>&1; then
		# The zero tablet is up. Try to shutdown 0-2 tablet + mysqlctl
		for i in 0 1 2; do
			uid=$((tablet + i))
			printf -v alias '%s-%010d' 'zone1' $uid
			echo "Shutting down tablet $alias"
			CELL=zone1 TABLET_UID=$uid ../common/scripts/vttablet-down.sh
			CELL=zone1 TABLET_UID=$uid ../common/scripts/mysqlctl-down.sh
		done
	fi
done

../common/scripts/vtctld-down.sh

if [ "${TOPO}" = "zk2" ]; then
	CELL=zone1 ../common/scripts/zk-down.sh
elif [ "${TOPO}" = "k8s" ]; then
	CELL=zone1 ../common/scripts/k3s-down.sh
elif [ "${TOPO}" = "consul" ]; then
	CELL=zone1 ../common/scripts/consul-down.sh
else
	CELL=zone1 ../common/scripts/etcd-down.sh
fi

# pedantic check: grep for any remaining processes

if [ -n "$VTDATAROOT" ]; then
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
