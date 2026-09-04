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
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

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

// TestSQLModeStripping verifies that the sql_mode value applied to backend
// connections has NO_BACKSLASH_ESCAPES removed — the mode a backend must not
// lex the vtgate's serialized SQL under — while every other mode is forwarded
// and the session keeps the full value for parsing and @@sql_mode reads.
func TestSQLModeStripping(t *testing.T) {
	session := NewSafeSession(&vtgatepb.Session{SystemVariables: map[string]string{
		"sql_mode":         "'NO_BACKSLASH_ESCAPES,PIPES_AS_CONCAT,STRICT_TRANS_TABLES'",
		"sql_safe_updates": "1",
	}})

	forwarded := map[string]string{}
	session.GetSystemVariables(func(k, v string) { forwarded[k] = v })
	assert.Equal(t, "'PIPES_AS_CONCAT,STRICT_TRANS_TABLES'", forwarded["sql_mode"])
	assert.Equal(t, "1", forwarded["sql_safe_updates"])

	// the session holds the canonical form, the mode kept from the backend included
	mode, ok := session.SQLMode()
	require.True(t, ok)
	assert.Equal(t, "PIPES_AS_CONCAT,NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES", mode)

	// a value that only contains the unforwardable mode forwards as the empty
	// mode: the user replaced the whole value, so the backend must drop its
	// execution modes too
	session.SetSystemVariable("sql_mode", "'NO_BACKSLASH_ESCAPES'")
	session.GetSystemVariables(func(k, v string) { forwarded[k] = v })
	assert.Equal(t, "''", forwarded["sql_mode"])

	// HIGH_NOT_PRECEDENCE is forwarded: the serialized SQL parenthesizes NOT
	// operands that would bind differently under it
	session.SetSystemVariable("sql_mode", "'HIGH_NOT_PRECEDENCE,STRICT_TRANS_TABLES'")
	session.GetSystemVariables(func(k, v string) { forwarded[k] = v })
	assert.Equal(t, "'HIGH_NOT_PRECEDENCE,STRICT_TRANS_TABLES'", forwarded["sql_mode"])

	// non-literal values are forwarded unchanged
	expr := "CONCAT(@@sql_mode, ',PIPES_AS_CONCAT')"
	session.SetSystemVariable("sql_mode", expr)
	session.GetSystemVariables(func(k, v string) { forwarded[k] = v })
	assert.Equal(t, expr, forwarded["sql_mode"])
}

// A session proto can carry sql_mode as the number MySQL accepts for the
// variable — an older vtgate stored numeric assignments as written, and direct
// gRPC clients may send one. Every reader expects mode names, so the value is
// decoded into MySQL's canonical form when the session is taken in.
func TestSQLModeSessionValueCanonicalized(t *testing.T) {
	for _, tc := range []struct {
		stored    string
		mode      string
		forwarded string
	}{
		{stored: "4", mode: "ANSI_QUOTES", forwarded: "'ANSI_QUOTES'"},
		{stored: "1048576", mode: "NO_BACKSLASH_ESCAPES", forwarded: "''"},
		{stored: "4194304", mode: "STRICT_ALL_TABLES", forwarded: "'STRICT_ALL_TABLES'"},
		{stored: "0", mode: "", forwarded: "''"},
		{stored: "'STRICT_TRANS_TABLES'", mode: "STRICT_TRANS_TABLES", forwarded: "'STRICT_TRANS_TABLES'"},
		// name lists in another spelling — an older vtgate stored them as the client
		// wrote them — take the canonical form: uppercased, expanded, in canonical order
		{stored: "'no_zero_date'", mode: "NO_ZERO_DATE", forwarded: "'NO_ZERO_DATE'"},
		{stored: "'STRICT_TRANS_TABLES,ANSI_QUOTES'", mode: "ANSI_QUOTES,STRICT_TRANS_TABLES", forwarded: "'ANSI_QUOTES,STRICT_TRANS_TABLES'"},
		{stored: "'ansi'", mode: "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI", forwarded: "'REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI'"},
		{stored: "pipes_as_concat", mode: "PIPES_AS_CONCAT", forwarded: "'PIPES_AS_CONCAT'"},
		// values that are not a valid mode are left as they are, for the backend to judge
		{stored: "'4'", mode: "4", forwarded: "'4'"},
		{stored: "'MARIADB_ONLY_MODE'", mode: "MARIADB_ONLY_MODE", forwarded: "'MARIADB_ONLY_MODE'"},
		{stored: "99999999999999999", mode: "99999999999999999", forwarded: "99999999999999999"},
		// expressions are not lists; they are evaluated when the session is used
		{stored: "REPLACE(@@sql_mode, 'ANSI_QUOTES', '')", mode: "REPLACE(@@sql_mode, 'ANSI_QUOTES', '')", forwarded: "REPLACE(@@sql_mode, 'ANSI_QUOTES', '')"},
	} {
		t.Run(tc.stored, func(t *testing.T) {
			session := NewSafeSession(&vtgatepb.Session{SystemVariables: map[string]string{"sql_mode": tc.stored}})
			mode, ok := session.SQLMode()
			require.True(t, ok)
			assert.Equal(t, tc.mode, mode)
			forwarded := map[string]string{}
			session.GetSystemVariables(func(k, v string) { forwarded[k] = v })
			assert.Equal(t, tc.forwarded, forwarded["sql_mode"])
			// the evaluation-relevant modes are matched by their canonical spelling
			assert.Equal(t, !strings.Contains(strings.ToUpper(tc.stored), "NO_ZERO_DATE"), evalengine.ParseSQLMode(mode).AllowZeroDate())
		})
	}
}

// TestSQLModeUnset verifies the accessor reports absence when the session
// never set sql_mode.
func TestSQLModeUnset(t *testing.T) {
	session := NewSafeSession(&vtgatepb.Session{})
	_, ok := session.SQLMode()
	assert.False(t, ok)
}
