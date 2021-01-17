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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance      *cluster.LocalProcessCluster
	shard0Master         *cluster.Vttablet
	shard0Replica        *cluster.Vttablet
	shard1Master         *cluster.Vttablet
	lookupMaster         *cluster.Vttablet
	hostname             = "localhost"
	cell                 = "zone1"
	userKeyspace         = "user"
	lookupKeyspace       = "lookup"
	createShardedMessage = `create table sharded_message(
		id bigint,
		priority bigint default 0,
		time_next bigint default 0,
		epoch bigint,
		time_acked bigint,
		message varchar(128),
		primary key(id),
		index next_idx(priority, time_next desc),
		index ack_idx(time_acked)
		) comment 'vitess_message,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=2,vt_cache_size=10,vt_poller_interval=1'`
	createUnshardedMessage = `create table unsharded_message(
		id bigint,
		priority bigint default 0,
		time_next bigint default 0,
		epoch bigint,
		time_acked bigint,
		message varchar(128),
		primary key(id),
		index next_idx(priority, time_next desc),
		index ack_idx(time_acked)
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
			"vitess_message3": {}
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

		shard0Master = clusterInstance.Keyspaces[1].Shards[0].MasterTablet()
		shard1Master = clusterInstance.Keyspaces[1].Shards[1].MasterTablet()
		lookupMaster = clusterInstance.Keyspaces[0].Shards[0].MasterTablet()
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

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
}
