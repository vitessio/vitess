/*
Copyright 2020 The Vitess Authors.

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
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

var (
	clusterInstance      *cluster.LocalProcessCluster
	shard0Primary        *cluster.Vttablet
	shard0Replica        *cluster.Vttablet
	shard1Primary        *cluster.Vttablet
	lookupPrimary        *cluster.Vttablet
	hostname             = "localhost"
	cell                 = "zone1"
	userKeyspace         = "user"
	lookupKeyspace       = "lookup"
	createShardedMessage = `create table sharded_message(
		# required columns
		id bigint NOT NULL COMMENT 'often an event id, can also be auto-increment or a sequence',
		priority tinyint NOT NULL DEFAULT '50' COMMENT 'lower number priorities process first',
		epoch bigint NOT NULL DEFAULT '0' COMMENT 'Vitess increments this each time it sends a message, and is used for incremental backoff doubling',
		time_next bigint DEFAULT 0 COMMENT 'the earliest time the message will be sent in epoch nanoseconds. Must be null if time_acked is set',
		time_acked bigint DEFAULT NULL COMMENT 'the time the message was acked in epoch nanoseconds. Must be null if time_next is set',

		# add as many custom fields here as required
		# optional - these are suggestions
		tenant_id bigint,
		message json,

		# required indexes
		primary key(id),
		index poller_idx(time_acked, priority, time_next desc)

		# add any secondary indexes or foreign keys - no restrictions
	) comment 'vitess_message,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=2,vt_cache_size=10,vt_poller_interval=1'`
	createUnshardedMessage = `create table unsharded_message(
		# required columns
		id bigint NOT NULL COMMENT 'often an event id, can also be auto-increment or a sequence',
		priority tinyint NOT NULL DEFAULT '50' COMMENT 'lower number priorities process first',
		epoch bigint NOT NULL DEFAULT '0' COMMENT 'Vitess increments this each time it sends a message, and is used for incremental backoff doubling',
		time_next bigint DEFAULT 0 COMMENT 'the earliest time the message will be sent in epoch nanoseconds. Must be null if time_acked is set',
		time_acked bigint DEFAULT NULL COMMENT 'the time the message was acked in epoch nanoseconds. Must be null if time_next is set',

		# add as many custom fields here as required
		# optional - these are suggestions
		tenant_id bigint,
		message json,

		# required indexes
		primary key(id),
		index poller_idx(time_acked, priority, time_next desc)

		# add any secondary indexes or foreign keys - no restrictions
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
			"unsharded_message": {},
			"vitess_message": {},
			"vitess_message3": {},
			"vitess_message4": {}
	  }
	}`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start unsharded keyspace
		keyspace := cluster.Keyspace{
			Name:      lookupKeyspace,
			SchemaSQL: createUnshardedMessage,
			VSchema:   lookupVschema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(keyspace, 1, false); err != nil {
			return 1, err
		}

		// Start sharded keyspace
		keyspace = cluster.Keyspace{
			Name:      userKeyspace,
			SchemaSQL: createShardedMessage,
			VSchema:   userVschema,
		}
		if err := clusterInstance.StartKeyspace(keyspace, []string{"-80", "80-"}, 1, false); err != nil {
			return 1, err
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1, err
		}

		shard0Primary = clusterInstance.Keyspaces[1].Shards[0].PrimaryTablet()
		shard1Primary = clusterInstance.Keyspaces[1].Shards[1].PrimaryTablet()
		lookupPrimary = clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
		shard0Replica = clusterInstance.Keyspaces[1].Shards[0].Vttablets[1]

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}

}
