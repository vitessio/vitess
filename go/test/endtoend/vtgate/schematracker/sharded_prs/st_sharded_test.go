/*
Copyright 2021 The Vitess Authors.

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

package shardedprs

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	KeyspaceName  = "ks"
	sidecarDBName = "_vt_schema_tracker_metadata" // custom sidecar database name for testing
	SchemaSQL     = `
create table t2(
	id3 bigint,
	id4 bigint,
	primary key(id3)
) Engine=InnoDB;

create table t2_id4_idx(
	id bigint not null auto_increment,
	id4 bigint,
	id3 bigint,
	primary key(id),
	key idx_id4(id4)
) Engine=InnoDB;

create table t8(
	id8 bigint,
	testId bigint,
	primary key(id8)
) Engine=InnoDB;
`

	VSchema = `
{
  "sharded": true,
  "vindexes": {
    "unicode_loose_xxhash" : {
	  "type": "unicode_loose_xxhash"
    },
    "unicode_loose_md5" : {
	  "type": "unicode_loose_md5"
    },
    "hash": {
      "type": "hash"
    },
    "xxhash": {
      "type": "xxhash"
    },
    "t2_id4_idx": {
      "type": "lookup_hash",
      "params": {
        "table": "t2_id4_idx",
        "from": "id4",
        "to": "id3",
        "autocommit": "true"
      },
      "owner": "t2"
    }
  },
  "tables": {
    "t2": {
      "column_vindexes": [
        {
          "column": "id3",
          "name": "hash"
        },
        {
          "column": "id4",
          "name": "t2_id4_idx"
        }
      ]
    },
    "t2_id4_idx": {
      "column_vindexes": [
        {
          "column": "id4",
          "name": "hash"
        }
      ]
    },
    "t8": {
      "column_vindexes": [
        {
          "column": "id8",
          "name": "hash"
        }
      ]
    }
  }
}`
)

func setup(t *testing.T) (*vitesst.Cluster, mysql.ConnParams) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace(KeyspaceName).
			WithShards(2).
			WithReplicas(2).
			WithSchema(SchemaSQL).
			WithVSchema(VSchema).
			WithSidecarDBName(sidecarDBName),
		vitesst.WithVTGateArgs("--schema-change-signal"),
		vitesst.WithVTTabletArgs("--queryserver-config-schema-change-signal"),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	// PRS on the second VTTablet of each shard.
	// This is supposed to change the primary tablet in the shards, meaning that a different tablet
	// will be responsible for sending schema tracking updates.
	for _, shard := range cluster.Keyspace(KeyspaceName).Shards() {
		newPrimary := shard.Replicas()[0]
		require.NoError(t, cluster.Vtctld().ExecuteCommand(
			ctx,
			"PlannedReparentShard",
			shard.Ref(),
			"--wait-replicas-timeout", "31s",
			"--new-primary", newPrimary.Alias(),
		))
		require.NoError(t, cluster.WaitForHealthyShard(ctx, KeyspaceName, shard.Name, 5*time.Minute))
	}

	_, err = cluster.AddVTOrc(t, ctx, "")
	require.NoError(t, err)
	require.NoError(t, waitForVTGateHealthy(ctx, cluster, 5*time.Minute))

	return cluster, cluster.VTParams(ctx, "")
}

// waitForVTGateHealthy blocks until VTGate's healthcheck reports one serving
// primary and the expected number of serving replicas for every shard.
func waitForVTGateHealthy(ctx context.Context, c *vitesst.Cluster, timeout time.Duration) error {
	shards := c.Keyspace(KeyspaceName).Shards()
	_, _, err := c.VTGate().MakeAPICallRetry(ctx, "/debug/vars", timeout, func(status int, body string) bool {
		if status != 200 {
			return false
		}
		var vars struct {
			HealthcheckConnections map[string]float64 `json:"HealthcheckConnections"`
		}
		if err := json.Unmarshal([]byte(body), &vars); err != nil {
			return false
		}
		for _, shard := range shards {
			if vars.HealthcheckConnections[KeyspaceName+"."+shard.Name+".primary"] != 1 {
				return false
			}
			if vars.HealthcheckConnections[KeyspaceName+"."+shard.Name+".replica"] != float64(len(shard.Tablets())-1) {
				return false
			}
		}
		return true
	})
	return err
}

func TestAddColumn(t *testing.T) {
	ctx := t.Context()
	_, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	_ = vitesst.Exec(t, conn, `alter table t2 add column aaa int`)
	vitesst.AssertMatchesWithTimeout(t, conn,
		"select aaa from t2", `[]`,
		100*time.Millisecond,
		30*time.Second,
		"t2 did not have the expected aaa column")
}
