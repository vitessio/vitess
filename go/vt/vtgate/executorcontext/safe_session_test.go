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

package executorcontext

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

type fakeInfo struct {
	transactionID int64
	alias         *topodatapb.TabletAlias
}

func (s *fakeInfo) TransactionID() int64 {
	return s.transactionID
}

func (s *fakeInfo) ReservedID() int64 {
	return 0
}

func (s *fakeInfo) RowsAffected() bool {
	return false
}

func (s *fakeInfo) Alias() *topodatapb.TabletAlias {
	return s.alias
}

func info(txId, uid int) ShardActionInfo {
	return &fakeInfo{transactionID: int64(txId), alias: &topodatapb.TabletAlias{Cell: "cell", Uid: uint32(uid)}}
}

// TestFailToMultiShardWhenSetToSingleDb tests that single db transactions fails on going multi shard.
func TestFailToMultiShardWhenSetToSingleDb(t *testing.T) {
	session := NewSafeSession(&vtgatepb.Session{
		InTransaction: true, TransactionMode: vtgatepb.TransactionMode_SINGLE,
	})

	err := session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "0"},
		info(1, 0),
		nil,
		vtgatepb.TransactionMode_SINGLE)
	require.NoError(t, err)
	err = session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "1"},
		info(1, 1),
		nil,
		vtgatepb.TransactionMode_SINGLE)
	require.Error(t, err)
}

// TestSingleDbUpdateToMultiShard tests that a single db transaction cannot be updated to multi shard.
func TestSingleDbUpdateToMultiShard(t *testing.T) {
	session := NewSafeSession(&vtgatepb.Session{
		InTransaction: true, TransactionMode: vtgatepb.TransactionMode_SINGLE,
	})

	// shard session s0 due to a vindex query
	session.execReadQuery = true
	err := session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "0"},
		info(1, 0),
		nil,
		vtgatepb.TransactionMode_SINGLE)
	require.NoError(t, err)
	session.execReadQuery = false

	// shard session s1
	err = session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "1"},
		info(1, 1),
		nil,
		vtgatepb.TransactionMode_SINGLE)
	require.NoError(t, err)

	// shard session s0 with normal query
	err = session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "0"},
		info(1, 1),
		session.ShardSessions[0],
		vtgatepb.TransactionMode_SINGLE)
	require.Error(t, err)
}

// TestSingleDbPreFailOnFind tests that finding a shard session fails
// if already shard session exists on another shard and the query is not from vindex.
func TestSingleDbPreFailOnFind(t *testing.T) {
	session := NewSafeSession(&vtgatepb.Session{
		InTransaction: true, TransactionMode: vtgatepb.TransactionMode_SINGLE,
	})

	// shard session s0 due to a vindex query
	session.execReadQuery = true
	err := session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "0"},
		info(1, 0),
		nil,
		vtgatepb.TransactionMode_SINGLE)
	require.NoError(t, err)
	session.execReadQuery = false

	// shard session s1
	err = session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "1"},
		info(1, 1),
		nil,
		vtgatepb.TransactionMode_SINGLE)
	require.NoError(t, err)

	// shard session s1 for normal query again - should not fail as already part of the session.
	ss, err := session.FindAndChangeSessionIfInSingleTxMode(
		"keyspace",
		"1",
		topodatapb.TabletType_UNKNOWN,
		vtgatepb.TransactionMode_SINGLE)
	require.NoError(t, err)
	require.NotNil(t, ss)
	require.False(t, ss.ReadOnly)
	require.EqualValues(t, 1, ss.TabletAlias.Uid)

	// shard session s0 for normal query
	_, err = session.FindAndChangeSessionIfInSingleTxMode(
		"keyspace",
		"0",
		topodatapb.TabletType_UNKNOWN,
		vtgatepb.TransactionMode_SINGLE)
	require.Error(t, err)
}

func TestPrequeries(t *testing.T) {
	session := NewSafeSession(&vtgatepb.Session{
		SystemVariables: map[string]string{
			"s1": "'apa'",
			"s2": "42",
		},
	})

	want := []string{"set s1 = 'apa', s2 = 42"}
	preQueries := session.SetPreQueries()

	assert.Equalf(t, want, preQueries, "got %v but wanted %v", preQueries, want)
}

func TestTimeZone(t *testing.T) {
	testCases := []struct {
		tz   string
		want string
	}{
		{
			tz:   "",
			want: time.Local.String(),
		},
		{
			tz:   "'Europe/Amsterdam'",
			want: "Europe/Amsterdam",
		},
		{
			tz:   "'+02:00'",
			want: "UTC+02:00",
		},
		{
			tz:   "foo",
			want: time.Local.String(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.tz, func(t *testing.T) {
			sysvars := map[string]string{}
			if tc.tz != "" {
				sysvars["time_zone"] = tc.tz
			}
			session := NewSafeSession(&vtgatepb.Session{
				SystemVariables: sysvars,
			})

			assert.Equal(t, tc.want, session.TimeZone().String())
		})
	}
}

// TestTargetTabletAlias tests the SetTargetTabletAlias and GetTargetTabletAlias methods.
func TestTargetTabletAlias(t *testing.T) {
	session := NewSafeSession(&vtgatepb.Session{})

	// Test: initially nil
	assert.Nil(t, session.GetTargetTabletAlias())

	// Test: Set and get
	alias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
	session.SetTargetTabletAlias(alias)
	got := session.GetTargetTabletAlias()
	assert.Equal(t, alias, got)

	// Test: Clear (set to nil)
	session.SetTargetTabletAlias(nil)
	assert.Nil(t, session.GetTargetTabletAlias())
}

// The reserved-connection keepalive is a request-level ExecuteRequest field
// (see queryservice.ContextWithReservedConnKeepAlive), not an ExecuteOptions
// field, precisely so that a client-supplied session cannot carry it — through
// this vtgate or through one predating the feature, which would relay unknown
// option fields to the tablet verbatim. The tablet-side regression test for
// that relay lives with the TabletServer keepalive tests.

func TestClearPrepareData(t *testing.T) {
	session := NewSafeSession(&vtgatepb.Session{})

	// Clearing a name that was never stored is a no-op, including before
	// any statement has been stored at all (nil map).
	session.ClearPrepareData("absent")

	session.StorePrepareData("stmt", &vtgatepb.PrepareData{PrepareStatement: "select 1"})
	require.NotNil(t, session.GetPrepareData("stmt"))

	session.ClearPrepareData("stmt")
	require.Nil(t, session.GetPrepareData("stmt"))
}

func TestPrepareDataConcurrentAccess(t *testing.T) {
	// A session can be accessed from multiple goroutines, which is why
	// StorePrepareData and GetPrepareData hold the session mutex.
	// ClearPrepareData must do the same: an unsynchronized delete on the
	// PrepareStatement map alongside the locked accessors is a map race
	// that crashes the process. This test fails under the race detector
	// if any of the three accessors skips the lock.
	session := NewSafeSession(&vtgatepb.Session{})

	var wg sync.WaitGroup
	for i := range 100 {
		name := fmt.Sprintf("stmt_%d", i%4)
		wg.Add(3)
		go func() {
			defer wg.Done()
			session.StorePrepareData(name, &vtgatepb.PrepareData{PrepareStatement: "select 1"})
		}()
		go func() {
			defer wg.Done()
			session.GetPrepareData(name)
		}()
		go func() {
			defer wg.Done()
			session.ClearPrepareData(name)
		}()
	}
	wg.Wait()
}

// TestShardSessionSnapshots verifies the snapshot accessor: it covers pre,
// normal, and post shard sessions in order, and the returned snapshots are
// copies — a later in-place update of the live proto (as AppendOrUpdate
// performs during query execution) must not show through.
func TestShardSessionSnapshots(t *testing.T) {
	alias := &topodatapb.TabletAlias{Cell: "cell", Uid: 1}
	shardSession := func(shard string, reservedID, transactionID int64) *vtgatepb.Session_ShardSession {
		return &vtgatepb.Session_ShardSession{
			Target:        &querypb.Target{Keyspace: "keyspace", Shard: shard},
			TabletAlias:   alias,
			ReservedId:    reservedID,
			TransactionId: transactionID,
		}
	}
	session := NewSafeSession(&vtgatepb.Session{
		InReservedConn: true,
		PreSessions:    []*vtgatepb.Session_ShardSession{shardSession("pre", 1, 0)},
		ShardSessions:  []*vtgatepb.Session_ShardSession{shardSession("main", 2, 20)},
		PostSessions:   []*vtgatepb.Session_ShardSession{shardSession("post", 3, 0)},
	})

	snapshots := session.ShardSessionSnapshots()
	require.Len(t, snapshots, 3)
	assert.Equal(t, "pre", snapshots[0].Target.Shard)
	assert.Equal(t, "main", snapshots[1].Target.Shard)
	assert.Equal(t, "post", snapshots[2].Target.Shard)
	assert.EqualValues(t, 2, snapshots[1].ReservedID)
	assert.EqualValues(t, 20, snapshots[1].TransactionID)
	assert.Equal(t, alias, snapshots[1].TabletAlias)

	session.ShardSessions[0].TransactionId = 999
	assert.EqualValues(t, 20, snapshots[1].TransactionID,
		"a snapshot must not observe later in-place updates of the live shard session")
}
