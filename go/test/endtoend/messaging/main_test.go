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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
)

var (
	clusterInstance      *vitesst.Cluster
	shard0Primary        *vitesst.Tablet
	shard0Replica        *vitesst.Tablet
	shard1Primary        *vitesst.Tablet
	lookupPrimary        *vitesst.Tablet
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

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(lookupKeyspace).
			WithReplicas(1).
			WithSchema(createUnshardedMessage).
			WithVSchema(lookupVschema),
		vitesst.WithKeyspace(userKeyspace).
			WithShardNames("-80", "80-").
			WithReplicas(1).
			WithSchema(createShardedMessage).
			WithVSchema(userVschema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	clusterInstance = cluster
	shard0Primary = cluster.Keyspace(userKeyspace).Shard("-80").Primary()
	shard1Primary = cluster.Keyspace(userKeyspace).Shard("80-").Primary()
	lookupPrimary = cluster.Keyspace(lookupKeyspace).Shard("-").Primary()
	shard0Replica = cluster.Keyspace(userKeyspace).Shard("-80").Replicas()[0]
}
