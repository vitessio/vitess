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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TestLockFailsClosedWhenKeyspaceIsUnavailable verifies that an advisory-lock
// plan fails when its deterministic keyspace cannot be resolved rather than
// choosing a different lock namespace.
func TestLockFailsClosedWhenKeyspaceIsUnavailable(t *testing.T) {
	vcursor := &loggingVCursor{shardErr: errors.New("no local SrvKeyspace")}
	lock := &Lock{
		Keyspace:          &vindexes.Keyspace{Name: "lock_keyspace"},
		TargetDestination: key.DestinationKeyspaceID{0},
	}

	_, err := lock.TryExecute(t.Context(), vcursor, nil, false)
	require.ErrorContains(t, err, "failed to resolve advisory-lock keyspace lock_keyspace")
}

// TestLockUsesAdvisoryLockSessionTarget verifies that a session already holding
// advisory locks executes lock functions on its reserved connection's target,
// even when the plan was cached for a different keyspace.
func TestLockUsesAdvisoryLockSessionTarget(t *testing.T) {
	vcursor := &loggingVCursor{
		advisoryLockTarget: &querypb.Target{Keyspace: "held_keyspace", Shard: "-"},
		results:            []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("is_free_lock('a')", "int64"), "1")},
	}
	lock := &Lock{
		Keyspace:          &vindexes.Keyspace{Name: "plan_keyspace"},
		TargetDestination: key.DestinationKeyspaceID{0},
		LockFunctions: []*LockFunc{{
			Typ:  &sqlparser.LockingFunc{Type: sqlparser.IsFreeLock, Name: sqlparser.NewStrLiteral("a")},
			Name: evalengine.NewLiteralString([]byte("a"), collations.SystemCollation),
		}},
	}

	_, err := lock.TryExecute(t.Context(), vcursor, nil, false)
	require.NoError(t, err)
	vcursor.ExpectLog(t, []string{
		"ExecuteLock held_keyspace.-: select is_free_lock('a') from dual",
	})
}
