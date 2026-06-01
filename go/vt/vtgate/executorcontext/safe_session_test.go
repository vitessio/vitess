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
	"reflect"
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

// TestAllowCrossShardDirectiveBypassesSingleDbCheck tests that the ALLOW_CROSS_SHARD
// directive allows a query to span multiple shards in SINGLE transaction mode.
func TestAllowCrossShardDirectiveBypassesSingleDbCheck(t *testing.T) {
	session := NewSafeSession(&vtgatepb.Session{
		InTransaction: true, TransactionMode: vtgatepb.TransactionMode_SINGLE,
	})

	// First shard session — always succeeds
	err := session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "0"},
		info(1, 0),
		nil,
		vtgatepb.TransactionMode_SINGLE)
	require.NoError(t, err)

	// Second shard WITHOUT directive — should fail
	err = session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "1"},
		info(1, 1),
		nil,
		vtgatepb.TransactionMode_SINGLE)
	require.Error(t, err)
	require.Contains(t, err.Error(), "multi-db transaction attempted")

	// Reset: start fresh session
	session = NewSafeSession(&vtgatepb.Session{
		InTransaction: true, TransactionMode: vtgatepb.TransactionMode_SINGLE,
	})

	// First shard session
	err = session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "0"},
		info(1, 0),
		nil,
		vtgatepb.TransactionMode_SINGLE)
	require.NoError(t, err)

	// Second shard WITH allowCrossShard directive — should succeed
	session.SetAllowCrossShard(true)
	err = session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "1"},
		info(1, 1),
		nil,
		vtgatepb.TransactionMode_SINGLE)
	require.NoError(t, err)

	// Verify directive is per-query: reset flag, third shard should fail
	session.SetAllowCrossShard(false)
	err = session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "2"},
		info(1, 2),
		nil,
		vtgatepb.TransactionMode_SINGLE)
	require.Error(t, err)
	require.Contains(t, err.Error(), "multi-db transaction attempted")
}

// TestAllowCrossShardDirectiveOnFindAndChange tests the directive with FindAndChangeSessionIfInSingleTxMode.
func TestAllowCrossShardDirectiveOnFindAndChange(t *testing.T) {
	session := NewSafeSession(&vtgatepb.Session{
		InTransaction: true, TransactionMode: vtgatepb.TransactionMode_SINGLE,
	})

	// Create read-only shard session on shard 0 (simulating a vindex query)
	session.execReadQuery = true
	err := session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "0"},
		info(1, 0),
		nil,
		vtgatepb.TransactionMode_SINGLE)
	require.NoError(t, err)
	session.execReadQuery = false

	// Create shard session on shard 1
	err = session.AppendOrUpdate(
		&querypb.Target{Keyspace: "keyspace", Shard: "1"},
		info(1, 1),
		nil,
		vtgatepb.TransactionMode_SINGLE)
	require.NoError(t, err)

	// Find shard 0 for a write query WITHOUT directive — should fail
	// because shard 0 is ReadOnly and a write would make it non-ReadOnly,
	// exceeding the cross-shard limit
	_, err = session.FindAndChangeSessionIfInSingleTxMode(
		"keyspace", "0", topodatapb.TabletType_UNKNOWN, vtgatepb.TransactionMode_SINGLE)
	require.Error(t, err)

	// Same query WITH allowCrossShard — should succeed
	session.SetAllowCrossShard(true)
	ss, err := session.FindAndChangeSessionIfInSingleTxMode(
		"keyspace", "0", topodatapb.TabletType_UNKNOWN, vtgatepb.TransactionMode_SINGLE)
	require.NoError(t, err)
	require.NotNil(t, ss)
	session.SetAllowCrossShard(false)
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

	if !reflect.DeepEqual(want, preQueries) {
		t.Errorf("got %v but wanted %v", preQueries, want)
	}
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
