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

package executorcontext

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
)

type (
	resultRunsExecutor struct {
		fakeExecutor

		result     *sqltypes.Result
		runs       []*sqltypes.Result
		executeErr error
	}
)

func (executor resultRunsExecutor) Execute(
	context.Context,
	vtgateservice.MySQLConnection,
	string,
	*SafeSession,
	string,
	map[string]*querypb.BindVariable,
	bool,
) (*sqltypes.Result, error) {
	return nil, executor.executeErr
}

func (executor resultRunsExecutor) ExecuteMultiShardWithResultRuns(
	context.Context,
	engine.Primitive,
	[]*srvtopo.ResolvedShard,
	[]*querypb.BoundQuery,
	*SafeSession,
	bool,
	bool,
	ResultsObserver,
	bool,
) (*sqltypes.Result, []*sqltypes.Result, []error) {
	return executor.result, executor.runs, nil
}

// TestVCursorExecuteMultiShardWithResultRuns proves the VCursor preserves the
// executor's flat result and aligned shard runs.
func TestVCursorExecuteMultiShardWithResultRuns(t *testing.T) {
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "2")
	runs := []*sqltypes.Result{result}
	vc := newResultRunsTestVCursor(t, resultRunsExecutor{result: result, runs: runs}, NewSafeSession(nil))

	got, gotRuns, errs := vc.ExecuteMultiShardWithResultRuns(t.Context(), nil, nil, nil, false, false, false)
	require.Empty(t, errs)
	require.Same(t, result, got)
	require.Len(t, gotRuns, 1)
	require.Same(t, runs[0], gotRuns[0])
}

// TestVCursorExecuteMultiShardWithResultRunsSavepointFailure proves the result-run
// path keeps the existing savepoint error contract.
func TestVCursorExecuteMultiShardWithResultRunsSavepointFailure(t *testing.T) {
	wantErr := errors.New("savepoint failed")
	session := NewSafeSession(nil)
	session.SetSavepointState(true)
	vc := newResultRunsTestVCursor(t, resultRunsExecutor{executeErr: wantErr}, session)
	rss := []*srvtopo.ResolvedShard{{}, {}}

	got, runs, errs := vc.ExecuteMultiShardWithResultRuns(t.Context(), nil, rss, nil, true, false, false)
	require.Nil(t, got)
	require.Nil(t, runs)
	require.Len(t, errs, 1)
	require.ErrorIs(t, errs[0], wantErr)
}

func newResultRunsTestVCursor(t *testing.T, executor iExecute, session *SafeSession) *VCursorImpl {
	t.Helper()
	logStats := logstats.NewLogStats(t.Context(), t.Name(), "select id from user", "", nil, streamlog.NewQueryLogConfigForTest())
	vc, err := NewVCursorImpl(
		session,
		sqlparser.MarginComments{},
		executor,
		logStats,
		nil,
		&vindexes.VSchema{},
		nil,
		nil,
		fakeObserver{},
		VCursorConfig{},
		nil,
	)
	require.NoError(t, err)
	return vc
}
