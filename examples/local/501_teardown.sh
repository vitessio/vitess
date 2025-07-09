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
	if vtctldclient --action_timeout 1s --server localhost:15999 GetTablet zone1-$tablet >/dev/null 2>&1; then
		# The zero tablet is up. Try to shutdown 0-2 tablet + mysqlctl
		for i in 0 1 2; do
			uid=$((tablet + i))
			printf -v alias '%s-%010d' 'zone1' $uid
			echo "Shutting down tablet $alias"
			CELL=zone1 TABLET_UID=$uid ../common/scripts/vttablet-down.sh
   			# because MySQL takes time to stop, we do this in parallel
			CELL=zone1 TABLET_UID=$uid ../common/scripts/mysqlctl-down.sh &
		done

  		# without a sleep below, we can have the echo happen before the echo of mysqlctl-down.sh
                sleep 2
                echo "Waiting mysqlctl to stop..."
                wait
                echo "mysqlctls are stopped!"
	fi
done

../common/scripts/vtctld-down.sh

if [ "${TOPO}" = "zk2" ]; then
	CELL=zone1 ../common/scripts/zk-down.sh
elif [ "${TOPO}" = "consul" ]; then
	CELL=zone1 ../common/scripts/consul-down.sh
elif [ "${TOPO}" = "mysql" ]; then
	# MySQL doesn't have a down script, since we externally manage it.
	# But just below in the cleanup we will empty the TOPO schema
	# so that it is ready for the next run.
	echo "The MySQL topo does not have a down script"
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

	# For the MySQL topo specifically, we do not store data in VTDATAROOT,
	# it is externally managed instead. If we don't wipe it, the next run
	# will fail because the MySQL topo will not be empty, and already know about
	# various tables causing an error:
	# ERROR: Cannot determine primary tablet for keyspace/shard commerce/0
	if [ "${TOPO}" = "mysql" ]; then
		echo "Cleaning up MySQL topology database..."
		# Extract user, password, host and port from MYSQL_TOPO_ADDR (format: user:password@host:port/database)
		MYSQL_USER_PASS=$(echo "$MYSQL_TOPO_ADDR" | sed 's/@.*//')
		MYSQL_USER=$(echo "$MYSQL_USER_PASS" | cut -d: -f1)
		MYSQL_PASS=$(echo "$MYSQL_USER_PASS" | cut -d: -f2)
		MYSQL_HOST_PORT=$(echo "$MYSQL_TOPO_ADDR" | sed 's/.*@\([^/]*\).*/\1/')
		MYSQL_HOST=$(echo "$MYSQL_HOST_PORT" | cut -d: -f1)
		MYSQL_PORT=$(echo "$MYSQL_HOST_PORT" | cut -d: -f2)
		mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" -e "DROP DATABASE IF EXISTS topo"
	fi
fi

disown -a
