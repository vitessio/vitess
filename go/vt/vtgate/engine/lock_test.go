/*
Copyright 2026 The Vitess Authors.

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

package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// lockResult is a scripted result for one lock function call in one keyspace.
type lockResult struct {
	value sqltypes.Value
	err   error
}

// majorityLockVCursor runs lock functions against scripted per-keyspace
// results, and tracks the session's advisory locks.
type majorityLockVCursor struct {
	noopVCursor

	resolveErrs map[string]error
	results     map[string][]lockResult
	locks       map[string]int
	log         []string
}

func (vc *majorityLockVCursor) ResolveDestinations(ctx context.Context, keyspace string, ids []*querypb.Value, destinations []key.ShardDestination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	if err := vc.resolveErrs[keyspace]; err != nil {
		return nil, nil, err
	}
	return []*srvtopo.ResolvedShard{{Target: &querypb.Target{Keyspace: keyspace, Shard: "-"}}}, nil, nil
}

func (vc *majorityLockVCursor) ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, lockFuncType sqlparser.LockingFuncType, multiKeyspace bool) (*sqltypes.Result, error) {
	ks := rs.Target.Keyspace
	vc.log = append(vc.log, fmt.Sprintf("%s: %s", ks, query.Sql))
	if len(vc.results[ks]) == 0 {
		return nil, fmt.Errorf("no scripted result for keyspace %s", ks)
	}
	res := vc.results[ks][0]
	vc.results[ks] = vc.results[ks][1:]
	if res.err != nil {
		return nil, res.err
	}
	return &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("lock", "int64"),
		Rows:   []sqltypes.Row{{res.value}},
	}, nil
}

func (vc *majorityLockVCursor) Session() SessionActions {
	return vc
}

func (vc *majorityLockVCursor) AddAdvisoryLock(name string) {
	vc.locks[name]++
}

func (vc *majorityLockVCursor) RemoveAdvisoryLock(name string) {
	vc.locks[name]--
	if vc.locks[name] <= 0 {
		delete(vc.locks, name)
	}
}

func (vc *majorityLockVCursor) AnyAdvisoryLockTaken() bool {
	return len(vc.locks) != 0
}

func (vc *majorityLockVCursor) ReleaseLock(context.Context) error {
	vc.log = append(vc.log, "ReleaseLock")
	vc.locks = map[string]int{}
	return nil
}

// newMajorityLock returns a Lock primitive that runs one lock function for the
// lock named "x" across the given keyspaces.
func newMajorityLock(typ sqlparser.LockingFuncType, keyspaces ...string) *Lock {
	lockFunc := &sqlparser.LockingFunc{Type: typ, Name: sqlparser.NewStrLiteral("x")}
	if typ == sqlparser.GetLock {
		lockFunc.Timeout = sqlparser.NewIntLiteral("10")
	}
	lock := &Lock{
		TargetDestination: key.DestinationKeyspaceID{0},
		LockFunctions: []*LockFunc{{
			Typ:  lockFunc,
			Name: evalengine.NewLiteralString([]byte("x"), collations.SystemCollation),
		}},
	}
	for _, ks := range keyspaces {
		lock.Keyspaces = append(lock.Keyspaces, &vindexes.Keyspace{Name: ks})
	}
	lock.Keyspace = lock.Keyspaces[0]
	return lock
}

func newMajorityLockVCursor(results map[string][]lockResult) *majorityLockVCursor {
	return &majorityLockVCursor{
		resolveErrs: map[string]error{},
		results:     results,
		locks:       map[string]int{},
	}
}

func one() lockResult  { return lockResult{value: sqltypes.NewInt64(1)} }
func zero() lockResult { return lockResult{value: sqltypes.NewInt64(0)} }
func null() lockResult { return lockResult{value: sqltypes.NULL} }

// TestMajorityGetLockSucceedsWithMajority tests that GET_LOCK returns 1 when it
// acquires the lock in a majority of the designated keyspaces.
func TestMajorityGetLockSucceedsWithMajority(t *testing.T) {
	vc := newMajorityLockVCursor(map[string][]lockResult{
		"a": {one()},
		"b": {zero()},
		"c": {one()},
	})

	qr, err := newMajorityLock(sqlparser.GetLock, "a", "b", "c").TryExecute(t.Context(), vc, nil, false)
	require.NoError(t, err)
	assert.Equal(t, "1", qr.Rows[0][0].ToString())
	assert.Equal(t, map[string]int{"x": 1}, vc.locks)
	assert.Equal(t, []string{
		"a: select get_lock('x', 10) from dual",
		"b: select get_lock('x', 10) from dual",
		"c: select get_lock('x', 10) from dual",
	}, vc.log)
}

// TestMajorityGetLockToleratesUnreachableKeyspace tests that GET_LOCK still
// succeeds when one of three designated keyspaces cannot be resolved.
func TestMajorityGetLockToleratesUnreachableKeyspace(t *testing.T) {
	vc := newMajorityLockVCursor(map[string][]lockResult{
		"a": {one()},
		"c": {one()},
	})
	vc.resolveErrs["b"] = errors.New("no healthy tablet")

	qr, err := newMajorityLock(sqlparser.GetLock, "a", "b", "c").TryExecute(t.Context(), vc, nil, false)
	require.NoError(t, err)
	assert.Equal(t, "1", qr.Rows[0][0].ToString())
	assert.Equal(t, map[string]int{"x": 1}, vc.locks)
}

// TestMajorityGetLockReleasesMinority tests that GET_LOCK releases the copies
// it acquired and returns 0 when it acquires the lock in only a minority.
func TestMajorityGetLockReleasesMinority(t *testing.T) {
	vc := newMajorityLockVCursor(map[string][]lockResult{
		"a": {one(), one()},
		"b": {zero()},
		"c": {zero()},
	})

	qr, err := newMajorityLock(sqlparser.GetLock, "a", "b", "c").TryExecute(t.Context(), vc, nil, false)
	require.NoError(t, err)
	assert.Equal(t, "0", qr.Rows[0][0].ToString())
	assert.Empty(t, vc.locks)
	assert.Equal(t, []string{
		"a: select get_lock('x', 10) from dual",
		"b: select get_lock('x', 10) from dual",
		"c: select get_lock('x', 10) from dual",
		"a: select release_lock('x') from dual",
	}, vc.log)
}

// TestMajorityGetLockReturnsErrorWithoutMajority tests that GET_LOCK releases
// the copies it acquired and returns the error when a keyspace fails and the
// lock is not held in a majority.
func TestMajorityGetLockReturnsErrorWithoutMajority(t *testing.T) {
	vc := newMajorityLockVCursor(map[string][]lockResult{
		"a": {one(), one()},
		"b": {{err: errors.New("connection refused")}},
		"c": {zero()},
	})

	_, err := newMajorityLock(sqlparser.GetLock, "a", "b", "c").TryExecute(t.Context(), vc, nil, false)
	require.ErrorContains(t, err, "connection refused")
	assert.Empty(t, vc.locks)
	assert.Contains(t, vc.log, "a: select release_lock('x') from dual")
	assert.NotContains(t, vc.log, "ReleaseLock")
}

// TestMajorityGetLockReleasesAllWhenCleanupFails tests that GET_LOCK releases
// all of the session's locks when it cannot release a partially acquired lock.
func TestMajorityGetLockReleasesAllWhenCleanupFails(t *testing.T) {
	vc := newMajorityLockVCursor(map[string][]lockResult{
		"a": {one(), {err: errors.New("connection reset")}},
		"b": {zero()},
		"c": {zero()},
	})
	vc.locks["held"] = 1

	_, err := newMajorityLock(sqlparser.GetLock, "a", "b", "c").TryExecute(t.Context(), vc, nil, false)
	require.ErrorContains(t, err, "failed to release a partially acquired advisory lock, held locks are released")
	assert.Empty(t, vc.locks)
	assert.Equal(t, "ReleaseLock", vc.log[len(vc.log)-1])
}

// TestMajorityReleaseLockCombinesResults tests that RELEASE_LOCK returns 1 when
// any keyspace released the lock. A lock taken before a keyspace was
// designated is not held there, which returns NULL.
func TestMajorityReleaseLockCombinesResults(t *testing.T) {
	vc := newMajorityLockVCursor(map[string][]lockResult{
		"a": {one()},
		"b": {null()},
	})
	vc.locks["x"] = 1

	qr, err := newMajorityLock(sqlparser.ReleaseLock, "a", "b").TryExecute(t.Context(), vc, nil, false)
	require.NoError(t, err)
	assert.Equal(t, "1", qr.Rows[0][0].ToString())
	assert.Empty(t, vc.locks)
	assert.Equal(t, []string{
		"a: select release_lock('x') from dual",
		"b: select release_lock('x') from dual",
		"ReleaseLock",
	}, vc.log)
}

// TestMajorityReleaseLockReleasesAllOnError tests that RELEASE_LOCK releases all
// of the session's locks and returns an error when any keyspace fails.
func TestMajorityReleaseLockReleasesAllOnError(t *testing.T) {
	vc := newMajorityLockVCursor(map[string][]lockResult{
		"a": {one()},
		"b": {{err: errors.New("connection reset")}},
	})
	vc.locks["x"] = 1
	vc.locks["other"] = 1

	_, err := newMajorityLock(sqlparser.ReleaseLock, "a", "b").TryExecute(t.Context(), vc, nil, false)
	require.ErrorContains(t, err, "failed to release advisory lock, held locks are released")
	assert.Empty(t, vc.locks)
	assert.Equal(t, "ReleaseLock", vc.log[len(vc.log)-1])
}

// TestMajorityIsFreeLockTreatsAnyHolderAsUsed tests that IS_FREE_LOCK reports a
// lock as used when any designated keyspace holds it.
func TestMajorityIsFreeLockTreatsAnyHolderAsUsed(t *testing.T) {
	vc := newMajorityLockVCursor(map[string][]lockResult{
		"a": {one()},
		"b": {zero()},
		"c": {one()},
	})

	qr, err := newMajorityLock(sqlparser.IsFreeLock, "a", "b", "c").TryExecute(t.Context(), vc, nil, false)
	require.NoError(t, err)
	assert.Equal(t, "0", qr.Rows[0][0].ToString())
}
