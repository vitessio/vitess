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

package repltracker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func TestWriteHeartbeat(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	now := time.Now()
	tw := newTestWriter(db, &now)
	upsert := fmt.Sprintf("INSERT INTO %s.heartbeat (ts, tabletUid, keyspaceShard) VALUES (%d, %d, '%s') ON DUPLICATE KEY UPDATE ts=VALUES(ts), tabletUid=VALUES(tabletUid)",
		"_vt", now.UnixNano(), tw.tabletAlias.Uid, tw.keyspaceShard)
	db.AddQuery(upsert, &sqltypes.Result{})

	writes.Reset()
	writeErrors.Reset()

	tw.writeHeartbeat()
	assert.Equal(t, int64(1), writes.Get())
	assert.Equal(t, int64(0), writeErrors.Get())
}

func TestWriteHeartbeatError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	now := time.Now()
	tw := newTestWriter(db, &now)

	writes.Reset()
	writeErrors.Reset()

	tw.writeHeartbeat()
	assert.Equal(t, int64(0), writes.Get())
	assert.Equal(t, int64(1), writeErrors.Get())
}

// TestCloseWhileStuckWriting tests that Close shouldn't get stuck even if the heartbeat writer is stuck waiting for a semi-sync ACK.
func TestCloseWhileStuckWriting(t *testing.T) {
	db := fakesqldb.New(t)
	tw := newTestWriter(db, nil)
	tw.isOpen = true

	killWg := sync.WaitGroup{}
	killWg.Add(1)
	startedWaitWg := sync.WaitGroup{}
	startedWaitWg.Add(1)

	// Insert a query pattern that causes the upsert to block indefinitely until it has been killed.
	// This simulates a stuck primary write due to a semi-sync ACK requirement.
	db.AddQueryPatternWithCallback(`INSERT INTO .*heartbeat \(ts, tabletUid, keyspaceShard\).*`, &sqltypes.Result{}, func(s string) {
		startedWaitWg.Done()
		killWg.Wait()
	})

	// When we receive a kill query, we want to finish running the wait group to unblock the upsert query.
	db.AddQueryPatternWithCallback("kill.*", &sqltypes.Result{}, func(s string) {
		killWg.Done()
	})

	// Now we enable writes, but the first write will get blocked.
	tw.enableWrites(true)
	// We wait until the write has blocked to ensure we only call Close after we are stuck writing.
	startedWaitWg.Wait()
	// Even if the write is blocked, we should be able to disable writes without waiting indefinitely.
	// This is what we call, when we try to Close the heartbeat writer.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		tw.enableWrites(false)
		cancel()
	}()
	select {
	case <-ctx.Done():
		db.Close()
	case <-time.After(1000 * time.Second):
		t.Fatalf("Timed out waiting for heartbeat writer to close")
	}
}

func newTestWriter(db *fakesqldb.DB, frozenTime *time.Time) *heartbeatWriter {
	cfg := tabletenv.NewDefaultConfig()
	cfg.ReplicationTracker.Mode = tabletenv.Heartbeat
	cfg.ReplicationTracker.HeartbeatInterval = time.Second

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "")

	tw := newHeartbeatWriter(tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "WriterTest"), &topodatapb.TabletAlias{Cell: "test", Uid: 1111})
	tw.keyspaceShard = "test:0"

	if frozenTime != nil {
		tw.now = func() time.Time {
			return *frozenTime
		}
	}

	tw.appPool.Open(dbc.AppWithDB())
	tw.allPrivsPool.Open(dbc.AllPrivsWithDB())

	return tw
}
