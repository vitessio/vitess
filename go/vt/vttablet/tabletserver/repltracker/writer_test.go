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

const (
	testKeyspaceShard = "test:0"
)

func TestWriteHeartbeat(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	now := time.Now()
	tw := newSimpleTestWriter(db, &now)

	upsert := fmt.Sprintf("INSERT INTO %s.heartbeat (ts, tabletUid, keyspaceShard) VALUES (%d, %d, '%s') ON DUPLICATE KEY UPDATE ts=VALUES(ts), tabletUid=VALUES(tabletUid)",
		"_vt", now.UnixNano(), tw.tabletAlias.Uid, tw.keyspaceShard)
	db.AddQuery(upsert, &sqltypes.Result{})

	writes.Reset()
	writeErrors.Reset()

	tw.writeHeartbeat()
	assert.EqualValues(t, 1, writes.Get())
	assert.EqualValues(t, 0, writeErrors.Get())
}

// TestWriteHeartbeatOpen tests that the heartbeat writer writes heartbeats when the writer is open.
func TestWriteHeartbeatOpen(t *testing.T) {
	defaultOnDemandDuration = 3 * time.Second

	db := fakesqldb.New(t)
	defer db.Close()

	tw := newSimpleTestWriter(db, nil)

	assert.Zero(t, tw.onDemandDuration)

	db.AddQueryPattern("^INSERT INTO.*", &sqltypes.Result{})

	writes.Reset()
	writeErrors.Reset()

	tw.writeHeartbeat()
	lastWrites := writes.Get()
	assert.EqualValues(t, 1, lastWrites)
	assert.EqualValues(t, 0, writeErrors.Get())

	t.Run("closed, no heartbeats", func(t *testing.T) {
		<-time.After(3 * time.Second)
		assert.EqualValues(t, 1, writes.Get())
	})

	{
		rateLimiter := tw.onDemandRequestsRateLimiter.Load()
		assert.Nil(t, rateLimiter)
	}
	tw.Open()
	defer tw.Close()
	{
		rateLimiter := tw.onDemandRequestsRateLimiter.Load()
		assert.Nil(t, rateLimiter)
	}
	t.Run("open, heartbeats", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				assert.EqualValues(t, 0, writeErrors.Get())
				currentWrites := writes.Get()
				assert.Greater(t, currentWrites, lastWrites)
				lastWrites = currentWrites
			}
		}
	})
}

// TestWriteHeartbeatDisabled tests that the heartbeat writer doesn't write heartbeats when the writer is disabled,
// but that it does respond to RequestHeartbeats(), and generates heartbeats on a lease.
func TestWriteHeartbeatDisabled(t *testing.T) {
	defaultOnDemandDuration = 3 * time.Second

	db := fakesqldb.New(t)
	defer db.Close()

	tw := newTestWriter(db, nil, tabletenv.Disable, 0)

	// Even though disabled, the writer will have an on-demand duration set.
	assert.Equal(t, defaultOnDemandDuration, tw.onDemandDuration)

	db.AddQueryPattern("^INSERT INTO.*", &sqltypes.Result{})

	writes.Reset()
	writeErrors.Reset()

	tw.writeHeartbeat()
	lastWrites := writes.Get()
	assert.EqualValues(t, 1, lastWrites)
	assert.EqualValues(t, 0, writeErrors.Get())

	t.Run("closed, no heartbeats", func(t *testing.T) {
		<-time.After(3 * time.Second)
		assert.EqualValues(t, 1, writes.Get())
	})
	{
		rateLimiter := tw.onDemandRequestsRateLimiter.Load()
		assert.Nil(t, rateLimiter)
	}
	tw.Open()
	defer tw.Close()
	{
		rateLimiter := tw.onDemandRequestsRateLimiter.Load()
		assert.NotNil(t, rateLimiter)
	}
	t.Run("open, no heartbeats", func(t *testing.T) {
		<-time.After(3 * time.Second)
		assert.EqualValues(t, 1, writes.Get())
	})
	t.Run("request heartbeats, heartbeats", func(t *testing.T) {
		tw.RequestHeartbeats()
		ctx, cancel := context.WithTimeout(context.Background(), tw.onDemandDuration-time.Second)
		defer cancel()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				assert.EqualValues(t, 0, writeErrors.Get())
				currentWrites := writes.Get()
				assert.Greater(t, currentWrites, lastWrites)
				lastWrites = currentWrites
			}
		}
	})
	t.Run("exhaust lease, no heartbeats", func(t *testing.T) {
		<-time.After(tw.onDemandDuration)
		currentWrites := writes.Get()
		<-time.After(3 * time.Second)
		assert.EqualValues(t, currentWrites, writes.Get())
	})
	tw.Close()
	{
		rateLimiter := tw.onDemandRequestsRateLimiter.Load()
		assert.Nil(t, rateLimiter)
	}
}

// TestWriteHeartbeatOnDemand tests that the heartbeat writer initiates leased heartbeats once opened,
// and then upon RequestHeartbeats().
func TestWriteHeartbeatOnDemand(t *testing.T) {
	defaultOnDemandDuration = 7 * time.Second
	onDemandDuration := minimalOnDemandDuration

	db := fakesqldb.New(t)
	defer db.Close()

	tw := newTestWriter(db, nil, tabletenv.Heartbeat, onDemandDuration)

	assert.Equal(t, onDemandDuration, tw.onDemandDuration)

	db.AddQueryPattern("^INSERT INTO.*", &sqltypes.Result{})

	writes.Reset()
	writeErrors.Reset()

	tw.writeHeartbeat()
	lastWrites := writes.Get()
	assert.EqualValues(t, 1, lastWrites)
	assert.EqualValues(t, 0, writeErrors.Get())

	t.Run("closed, no heartbeats", func(t *testing.T) {
		<-time.After(3 * time.Second)
		assert.EqualValues(t, 1, writes.Get())
	})
	{
		rateLimiter := tw.onDemandRequestsRateLimiter.Load()
		assert.Nil(t, rateLimiter)
	}
	tw.Open()
	defer tw.Close()
	{
		rateLimiter := tw.onDemandRequestsRateLimiter.Load()
		assert.NotNil(t, rateLimiter)
	}
	t.Run("open, initial heartbeats", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), tw.onDemandDuration-time.Second)
		defer cancel()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				assert.EqualValues(t, 0, writeErrors.Get())
				currentWrites := writes.Get()
				assert.Greater(t, currentWrites, lastWrites)
				lastWrites = currentWrites
			}
		}
	})
	t.Run("exhaust lease, no heartbeats", func(t *testing.T) {
		<-time.After(tw.onDemandDuration)
		currentWrites := writes.Get()
		<-time.After(3 * time.Second)
		assert.EqualValues(t, currentWrites, writes.Get())
	})
	t.Run("request heartbeats, heartbeats", func(t *testing.T) {
		tw.RequestHeartbeats()
		lastWrites := writes.Get()
		<-time.After(tw.onDemandDuration)
		assert.Greater(t, writes.Get(), lastWrites)
	})
	t.Run("exhaust lease, no heartbeats", func(t *testing.T) {
		<-time.After(tw.onDemandDuration)
		currentWrites := writes.Get()
		<-time.After(3 * time.Second)
		assert.EqualValues(t, currentWrites, writes.Get())
	})
	tw.Close()
	{
		rateLimiter := tw.onDemandRequestsRateLimiter.Load()
		assert.Nil(t, rateLimiter)
	}
}

func TestWriteHeartbeatError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	now := time.Now()
	tw := newSimpleTestWriter(db, &now)

	writes.Reset()
	writeErrors.Reset()

	tw.writeHeartbeat()
	assert.Equal(t, int64(0), writes.Get())
	assert.Equal(t, int64(1), writeErrors.Get())
}

// TestCloseWhileStuckWriting tests that Close shouldn't get stuck even if the heartbeat writer is stuck waiting for a semi-sync ACK.
func TestCloseWhileStuckWriting(t *testing.T) {
	db := fakesqldb.New(t)
	tw := newSimpleTestWriter(db, nil)

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
	tw.enableWrites()
	// We wait until the write has blocked to ensure we only call Close after we are stuck writing.
	startedWaitWg.Wait()
	// Even if the write is blocked, we should be able to disable writes without waiting indefinitely.
	// This is what we call, when we try to Close the heartbeat writer.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		tw.disableWrites()
		cancel()
	}()
	select {
	case <-ctx.Done():
		db.Close()
	case <-time.After(1000 * time.Second):
		t.Fatalf("Timed out waiting for heartbeat writer to close")
	}
}

func newSimpleTestWriter(db *fakesqldb.DB, frozenTime *time.Time) *heartbeatWriter {
	return newTestWriter(db, frozenTime, tabletenv.Heartbeat, 0)
}

func newTestWriter(db *fakesqldb.DB, frozenTime *time.Time, replTrackerMode string, onDemandInterval time.Duration) *heartbeatWriter {
	cfg := tabletenv.NewDefaultConfig()
	cfg.ReplicationTracker.Mode = replTrackerMode
	cfg.ReplicationTracker.HeartbeatOnDemand = onDemandInterval
	cfg.ReplicationTracker.HeartbeatInterval = 250 * time.Millisecond // oversampling our 1*time.Second unit test interval in various functions

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "")

	tw := newHeartbeatWriter(tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "WriterTest"), &topodatapb.TabletAlias{Cell: "test", Uid: 1111})
	tw.keyspaceShard = testKeyspaceShard

	if frozenTime != nil {
		tw.now = func() time.Time {
			return *frozenTime
		}
	}

	tw.appPool.Open(dbc.AppWithDB())
	tw.allPrivsPool.Open(dbc.AllPrivsWithDB())

	return tw
}
