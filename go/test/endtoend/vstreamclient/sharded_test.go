/*
Copyright 2025 The Vitess Authors.

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

package vstreamclient

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/vstreamclient"
)

var (
	startShardedKeyspaceOnce sync.Once
	startShardedKeyspaceErr  error
)

const shardedVSchema = `{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    }
  },
  "tables": {
    "customer": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "hash"
        }
      ]
    },
    "purchases": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "hash"
        }
      ]
    }
  }
}`

func ensureShardedCustomerKeyspace(t *testing.T) {
	t.Helper()

	startShardedKeyspaceOnce.Do(func() {
		startShardedKeyspaceErr = clusterInstance.StartKeyspace(cluster.Keyspace{
			Name:      "sharded_customer",
			SchemaSQL: sqlSchema,
			VSchema:   shardedVSchema,
		}, []string{"-80", "80-"}, 1, true, cell)
		if startShardedKeyspaceErr != nil {
			return
		}

		startShardedKeyspaceErr = clusterInstance.WaitForTabletsToHealthyInVtgate()
	})
	require.NoError(t, startShardedKeyspaceErr)
}

func TestVStreamClientStreamsAndResumesFromShardedSource(t *testing.T) {
	ensureShardedCustomerKeyspace(t)
	te := newTestEnv(t)
	streamName := t.Name()

	newClient := func(flushFn vstreamclient.FlushFunc) *vstreamclient.VStreamClient {
		return te.newDefaultClient(t, streamName, []vstreamclient.TableConfig{{
			Keyspace:        "sharded_customer",
			Table:           "customer",
			Query:           "select * from customer where id between 4100 and 4199",
			MaxRowsPerFlush: 1,
			DataType:        &Customer{},
			FlushFn:         flushFn,
		}})
	}

	firstBatch := []*Customer{{ID: 4101, Email: "sharded-a@domain.com"}, {ID: 4102, Email: "sharded-b@domain.com"}}
	for _, customer := range firstBatch {
		te.exec(t, "insert into sharded_customer.customer(id, email) values(:id, :email)", customerBindVars(customer.ID, customer.Email))
	}

	var firstRun []*Customer
	te.runUntilTimeout(t, newClient(func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
		for _, row := range rows {
			firstRun = append(firstRun, row.Data.(*Customer))
		}
		return nil
	}), 3*time.Second)
	assert.ElementsMatch(t, firstBatch, firstRun)

	vgtid := queryLatestVGtid(t, te.ctx, te.session, streamName)
	require.Len(t, vgtid.ShardGtids, 2)
	assert.ElementsMatch(t, []string{"-80", "80-"}, []string{vgtid.ShardGtids[0].Shard, vgtid.ShardGtids[1].Shard})

	secondBatch := []*Customer{{ID: 4103, Email: "sharded-c@domain.com"}}
	for _, customer := range secondBatch {
		te.exec(t, "insert into sharded_customer.customer(id, email) values(:id, :email)", customerBindVars(customer.ID, customer.Email))
	}

	var secondRun []*Customer
	te.runUntilTimeout(t, newClient(func(_ context.Context, rows []vstreamclient.Row, _ vstreamclient.FlushMeta) error {
		for _, row := range rows {
			secondRun = append(secondRun, row.Data.(*Customer))
		}
		return nil
	}), 3*time.Second)
	assert.ElementsMatch(t, secondBatch, secondRun)
}
