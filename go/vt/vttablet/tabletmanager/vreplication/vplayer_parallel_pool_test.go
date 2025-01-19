/*
Copyright 2024 The Vitess Authors.

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

package vreplication

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

func newTestVPlayer(t *testing.T, ctx context.Context) *vplayer {

	tablet := addTablet(100)
	defer deleteTablet(tablet)
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "t1",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
	}
	id := int32(1)
	vsclient := newTabletConnector(tablet)
	stats := binlogplayer.NewStats()
	defer stats.Stop()
	dbClient := playerEngine.dbClientFactoryFiltered()
	err := dbClient.Connect()
	require.NoError(t, err)
	defer dbClient.Close()
	dbName := dbClient.DBName()
	// Ensure there's a dummy vreplication workflow record
	_, err = dbClient.ExecuteFetch(fmt.Sprintf("insert into _vt.vreplication (id, workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name, options) values (%d, 'test_workflow', '', '', 99999, 99999, 0, 0, 'Running', '%s', '{}') on duplicate key update workflow='test', source='', pos='', max_tps=99999, max_replication_lag=99999, time_updated=0, transaction_timestamp=0, state='Running', db_name='%s'",
		id, dbName, dbName), 1)
	require.NoError(t, err)
	defer func() {
		_, err = dbClient.ExecuteFetch(fmt.Sprintf("delete from _vt.vreplication where id = %d", id), 1)
		require.NoError(t, err)
	}()
	vr := newVReplicator(id, bls, vsclient, stats, dbClient, nil, env.Mysqld, playerEngine, vttablet.DefaultVReplicationConfig)
	settings, _, err := vr.loadSettings(ctx, newVDBClient(dbClient, stats, vttablet.DefaultVReplicationConfig.RelayLogMaxItems))
	require.NoError(t, err)

	throttlerAppName := vr.throttlerAppName()
	assert.Contains(t, throttlerAppName, "test_workflow")
	assert.Contains(t, throttlerAppName, "vreplication")
	assert.NotContains(t, throttlerAppName, "vcopier")
	assert.NotContains(t, throttlerAppName, "vplayer")

	vp := newVPlayer(vr, settings, nil, replication.Position{}, "")

	return vp
}

func newTestParallelWorkersPool(size int) *parallelWorkersPool {
	p := &parallelWorkersPool{
		workers:      make([]*parallelWorker, size),
		pool:         make(chan *parallelWorker, size),
		workerErrors: make(chan error, size*2),
	}
	p.wakeup.L = &p.mu
	for i := range size {
		w := &parallelWorker{
			index: i,
			pool:  p,
		}
		p.workers[i] = w
		p.pool <- w
	}
	p.maxBatchedCommitsPerWorker = 10
	return p
}

func TestIsApplicable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := newTestParallelWorkersPool(8)
	require.NotNil(t, p)

	workers := make([]*parallelWorker, 4)
	for i := range 4 {
		w, err := p.availableWorker(ctx, 5, int64(i*100), i == 0)
		require.NoError(t, err)
		require.NotNil(t, w)
		workers[i] = w
	}
}
