/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package messaging

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance      *cluster.LocalProcessCluster
	shard0Master         *cluster.Vttablet
	shard0Replica        *cluster.Vttablet
	shard1Master         *cluster.Vttablet
	lookupMaster         *cluster.Vttablet
	hostname             = "localhost"
	keyspaceName         = "test_keyspace"
	testingID            = 1
	tableName            = "vt_prepare_stmt_test"
	cell                 = "zone1"
	userKeyspace         = "user"
	lookupKeyspace       = "lookup"
	createShardedMessage = `create table sharded_message(
		time_scheduled bigint,
		id bigint,
		time_next bigint,
		epoch bigint,
		time_created bigint,
		time_acked bigint,
		message varchar(128),
		primary key(time_scheduled, id),
		unique index id_idx(id),
		index next_idx(time_next, epoch)
		) comment 'vitess_message,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=2,vt_cache_size=10,vt_poller_interval=1'`
	createUnshardedMessage = `create table unsharded_message(
			time_scheduled bigint,
			id bigint,
			time_next bigint,
			epoch bigint,
			time_created bigint,
			time_acked bigint,
			message varchar(128),
			primary key(time_scheduled, id),
			unique index id_idx(id),
			index next_idx(time_next, epoch)
			) comment 'vitess_message,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=2,vt_cache_size=10,vt_poller_interval=1'`
	userVschema = `{
				  "sharded": true,
				  "vindexes": {
					"hash_index": {
					  "type": "hash"
					}
				  },
				  "tables": {
					"sharded_message": {
					  "column_vindexes": [
						{
						  "column": "id",
						  "name": "hash_index"
						}
					  ]
					}
				  }
				}`
	lookupVschema = `{
				  "sharded": false,
				  "tables": {
					"unsharded_message": {
					  "type": "sequence"
					}
				  }
				}`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = &cluster.LocalProcessCluster{Cell: cell, Hostname: hostname}
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start unsharded keyspace
		keyspace := &cluster.Keyspace{
			Name:      lookupKeyspace,
			SchemaSQL: createUnshardedMessage,
			VSchema:   lookupVschema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
			return 1, err
		}

		// Start sharded keyspace
		keyspace = &cluster.Keyspace{
			Name:      userKeyspace,
			SchemaSQL: createShardedMessage,
			VSchema:   userVschema,
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, false); err != nil {
			return 1, err
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1, err
		}

		shard0Master = clusterInstance.Keyspaces[1].Shards[0].MasterTablet()
		shard1Master = clusterInstance.Keyspaces[1].Shards[1].MasterTablet()
		lookupMaster = clusterInstance.Keyspaces[0].Shards[0].MasterTablet()
		shard0Replica = clusterInstance.Keyspaces[1].Shards[0].Replica()

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}

}
