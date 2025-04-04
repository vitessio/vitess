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

package transaction

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	twopcutil "vitess.io/vitess/go/test/endtoend/transaction/twopc/utils"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// TestTransactionModes tests transactions using twopc mode
func TestTransactionModeMetrics(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	tcases := []struct {
		name  string
		stmts []string
		want  commitMetric
	}{{
		name:  "nothing to commit: so no change on vars",
		stmts: []string{"commit"},
	}, {
		name:  "begin commit - no dml: so no change on vars",
		stmts: []string{"begin", "commit"},
	}, {
		name: "single shard",
		stmts: []string{
			"begin",
			"insert into twopc_user(id) values (1)",
			"commit",
		},
		want: commitMetric{TotalCount: 1, SingleCount: 1},
	}, {
		name: "multi shard insert",
		stmts: []string{
			"begin",
			"insert into twopc_user(id) values (7),(8)",
			"commit",
		},
		want: commitMetric{TotalCount: 1, MultiCount: 1, TwoPCCount: 1},
	}, {
		name: "multi shard delete",
		stmts: []string{
			"begin",
			"delete from twopc_user",
			"commit",
		},
		want: commitMetric{TotalCount: 1, MultiCount: 1, TwoPCCount: 1},
	}}

	initial := getCommitMetric(t)
	utils.Exec(t, conn, "set transaction-mode = multi")
	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			for _, stmt := range tc.stmts {
				utils.Exec(t, conn, stmt)
			}
			updatedMetric := getCommitMetric(t)
			assert.EqualValues(t, tc.want.TotalCount, updatedMetric.TotalCount-initial.TotalCount, "TotalCount")
			assert.EqualValues(t, tc.want.SingleCount, updatedMetric.SingleCount-initial.SingleCount, "SingleCount")
			assert.EqualValues(t, tc.want.MultiCount, updatedMetric.MultiCount-initial.MultiCount, "MultiCount")
			assert.Zero(t, updatedMetric.TwoPCCount-initial.TwoPCCount, "TwoPCCount")
			initial = updatedMetric
		})
	}

	utils.Exec(t, conn, "set transaction-mode = twopc")
	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			for _, stmt := range tc.stmts {
				utils.Exec(t, conn, stmt)
			}
			updatedMetric := getCommitMetric(t)
			assert.EqualValues(t, tc.want.TotalCount, updatedMetric.TotalCount-initial.TotalCount, "TotalCount")
			assert.EqualValues(t, tc.want.SingleCount, updatedMetric.SingleCount-initial.SingleCount, "SingleCount")
			assert.Zero(t, updatedMetric.MultiCount-initial.MultiCount, "MultiCount")
			assert.EqualValues(t, tc.want.TwoPCCount, updatedMetric.TwoPCCount-initial.TwoPCCount, "TwoPCCount")
			initial = updatedMetric
		})
	}
}

// TestVTGate2PCCommitMetricOnFailure tests unresolved commit metrics on VTGate.
func TestVTGate2PCCommitMetricOnFailure(t *testing.T) {
	defer cleanup(t)

	initialCount := getVarValue[float64](t, "CommitUnresolved", clusterInstance.VtgateProcess.GetVars)

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	conn := vtgateConn.Session("", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err = conn.Execute(ctx, "begin", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "insert into twopc_user(id, name) values(7,'foo'), (8,'bar')", nil, false)
	require.NoError(t, err)

	// fail after mm commit.
	newCtx := callerid.NewContext(ctx, callerid.NewEffectiveCallerID("MMCommitted_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil, false)
	require.ErrorContains(t, err, "Fail After MM commit")

	updatedCount := getVarValue[float64](t, "CommitUnresolved", clusterInstance.VtgateProcess.GetVars)
	assert.EqualValues(t, 1, updatedCount-initialCount, "CommitUnresolved")

	waitForResolve(ctx, t, conn, 5*time.Second)

	_, err = conn.Execute(ctx, "begin", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "insert into twopc_user(id, name) values(9,'foo')", nil, false)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "insert into twopc_user(id, name) values(10,'apa')", nil, false)
	require.NoError(t, err)

	// fail during rm commit.
	newCtx = callerid.NewContext(ctx, callerid.NewEffectiveCallerID("RMCommit_-40_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil, false)
	require.ErrorContains(t, err, "Fail During RM commit")

	updatedCount = getVarValue[float64](t, "CommitUnresolved", clusterInstance.VtgateProcess.GetVars)
	assert.EqualValues(t, 2, updatedCount-initialCount, "CommitUnresolved")

	waitForResolve(ctx, t, conn, 5*time.Second)
}

// TestVTTablet2PCMetrics tests 2pc metrics on VTTablet.
func TestVTTablet2PCMetrics(t *testing.T) {
	defer cleanup(t)

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	conn := vtgateConn.Session("", nil)
	ctx, cancel := context.WithCancel(context.Background())
	ctx = callerid.NewContext(ctx, callerid.NewEffectiveCallerID("MMCommitted_FailNow", "", ""), nil)
	defer cancel()

	for i := 1; i <= 20; i++ {
		_, err = conn.Execute(ctx, "begin", nil, false)
		require.NoError(t, err)
		query := fmt.Sprintf("insert into twopc_user(id, name) values(%d,'foo'), (%d,'bar'), (%d,'baz')", i, i*101, i+53)
		_, err = conn.Execute(ctx, query, nil, false)
		require.NoError(t, err)

		multi := len(conn.SessionPb().ShardSessions) > 1

		// fail after mm commit.
		_, err = conn.Execute(ctx, "commit", nil, false)
		if multi {
			assert.ErrorContains(t, err, "Fail After MM commit")
		} else {
			assert.NoError(t, err)
		}
	}

	waitForResolve(ctx, t, conn, 5*time.Second)

	// at least 1 unresolved transaction should be seen by the gauge.
	unresolvedCount := getUnresolvedTxCount(t)
	assert.Greater(t, unresolvedCount, 1.0)

	// after next ticker should be become zero.
	timeout := time.After(3 * time.Second)
	for {
		select {
		case <-timeout:
			t.Errorf("unresolved transaction not reduced to zero within the time limit")
			return
		case <-time.After(500 * time.Millisecond):
			unresolvedCount = getUnresolvedTxCount(t)
			if unresolvedCount == 0 {
				return
			}
			fmt.Printf("unresolved tx count: %f\n", unresolvedCount)
		}
	}
}

// TestVTTablet2PCMetricsFailCommitPrepared tests 2pc metrics on VTTablet on commit prepared failure..';;/
func TestVTTablet2PCMetricsFailCommitPrepared(t *testing.T) {
	defer cleanup(t)

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	conn := vtgateConn.Session("", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	newCtx := callerid.NewContext(ctx, callerid.NewEffectiveCallerID("CP_80-_R", "", ""), nil)
	execute(t, newCtx, conn, "begin")
	execute(t, newCtx, conn, "insert into twopc_t1(id, col) values (4, 1)")
	execute(t, newCtx, conn, "insert into twopc_t1(id, col) values (6, 2)")
	execute(t, newCtx, conn, "insert into twopc_t1(id, col) values (9, 3)")
	_, err = conn.Execute(newCtx, "commit", nil, false)
	require.ErrorContains(t, err, "commit prepared: retryable error")
	dtidRetryable := getDTIDFromWarnings(ctx, t, conn)
	require.NotEmpty(t, dtidRetryable)

	cpFail := getVarValue[map[string]any](t, "CommitPreparedFail", clusterInstance.Keyspaces[0].Shards[2].FindPrimaryTablet().VttabletProcess.GetVars)
	require.EqualValues(t, 1, cpFail["Retryable"])
	require.Nil(t, cpFail["NonRetryable"])

	newCtx = callerid.NewContext(ctx, callerid.NewEffectiveCallerID("CP_80-_NR", "", ""), nil)
	execute(t, newCtx, conn, "begin")
	execute(t, newCtx, conn, "insert into twopc_t1(id, col) values (20, 11)")
	execute(t, newCtx, conn, "insert into twopc_t1(id, col) values (22, 21)")
	execute(t, newCtx, conn, "insert into twopc_t1(id, col) values (25, 31)")
	_, err = conn.Execute(newCtx, "commit", nil, false)
	require.ErrorContains(t, err, "commit prepared: non retryable error")
	dtidNonRetryable := getDTIDFromWarnings(ctx, t, conn)
	require.NotEmpty(t, dtidNonRetryable)

	cpFail = getVarValue[map[string]any](t, "CommitPreparedFail", clusterInstance.Keyspaces[0].Shards[2].FindPrimaryTablet().VttabletProcess.GetVars)
	require.EqualValues(t, 1, cpFail["Retryable"]) // old counter value
	require.EqualValues(t, 1, cpFail["NonRetryable"])

	// restart to trigger unresolved transactions
	err = clusterInstance.Keyspaces[0].Shards[2].FindPrimaryTablet().RestartOnlyTablet()
	require.NoError(t, err)

	// dtid with retryable error should be resolved.
	waitForDTIDResolve(ctx, t, conn, dtidRetryable, 5*time.Second)

	// dtid with non retryable error should remain unresolved.
	qr, err := conn.Execute(ctx, fmt.Sprintf(`show transaction status for '%s'`, dtidNonRetryable), nil, false)
	require.NoError(t, err)
	require.NotZero(t, qr.Rows, "should remain unresolved")

	// running conclude transaction for it.
	out, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(
		"DistributedTransaction", "conclude", "--dtid", dtidNonRetryable)
	require.NoError(t, err)
	require.Contains(t, out, "Successfully concluded the distributed transaction")
	// now verifying
	qr, err = conn.Execute(ctx, fmt.Sprintf(`show transaction status for '%s'`, dtidNonRetryable), nil, false)
	require.NoError(t, err)
	require.Empty(t, qr.Rows)
}

func execute(t *testing.T, ctx context.Context, conn *vtgateconn.VTGateSession, query string) {
	t.Helper()

	_, err := conn.Execute(ctx, query, nil, false)
	require.NoError(t, err)
}

func getUnresolvedTxCount(t *testing.T) float64 {
	unresolvedCount := 0.0
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		unresolvedTx := getVarValue[map[string]any](t, "UnresolvedTransaction", shard.FindPrimaryTablet().VttabletProcess.GetVars)
		if mmCount, exists := unresolvedTx["MetadataManager"]; exists {
			unresolvedCount += mmCount.(float64)
		}
		if rmCount, exists := unresolvedTx["ResourceManager"]; exists {
			unresolvedCount += rmCount.(float64)
		}
	}
	return unresolvedCount
}

type commitMetric struct {
	TotalCount  float64
	SingleCount float64
	MultiCount  float64
	TwoPCCount  float64
}

func getCommitMetric(t *testing.T) commitMetric {
	t.Helper()

	vars := clusterInstance.VtgateProcess.GetVars()
	require.NotNil(t, vars)

	cm := commitMetric{}
	commitVars, exists := vars["CommitModeTimings"]
	if !exists {
		return cm
	}

	commitMap, ok := commitVars.(map[string]any)
	require.True(t, ok, "commit vars is not a map")

	cm.TotalCount = commitMap["TotalCount"].(float64)

	histogram, ok := commitMap["Histograms"].(map[string]any)
	require.True(t, ok, "commit histogram is not a map")

	if single, ok := histogram["Single"]; ok {
		singleMap, ok := single.(map[string]any)
		require.True(t, ok, "single histogram is not a map")
		cm.SingleCount = singleMap["Count"].(float64)
	}

	if multi, ok := histogram["Multi"]; ok {
		multiMap, ok := multi.(map[string]any)
		require.True(t, ok, "multi histogram is not a map")
		cm.MultiCount = multiMap["Count"].(float64)
	}

	if twopc, ok := histogram["TwoPC"]; ok {
		twopcMap, ok := twopc.(map[string]any)
		require.True(t, ok, "twopc histogram is not a map")
		cm.TwoPCCount = twopcMap["Count"].(float64)
	}

	return cm
}

func getVarValue[T any](t *testing.T, key string, varFunc func() map[string]any) T {
	t.Helper()

	vars := varFunc()
	require.NotNil(t, vars)

	value, exists := vars[key]
	if !exists {
		return *new(T)
	}
	castValue, ok := value.(T)
	if !ok {
		t.Errorf("unexpected type, want: %T, got %T", new(T), value)
	}
	return castValue
}

func waitForResolve(ctx context.Context, t *testing.T, conn *vtgateconn.VTGateSession, waitTime time.Duration) {
	t.Helper()

	dtid := getDTIDFromWarnings(ctx, t, conn)
	waitForDTIDResolve(ctx, t, conn, dtid, waitTime)
}

func getDTIDFromWarnings(ctx context.Context, t *testing.T, conn *vtgateconn.VTGateSession) string {
	qr, err := conn.Execute(ctx, "show warnings", nil, false)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)

	// validate warning output
	w := twopcutil.ToWarn(qr.Rows[0])
	assert.Equal(t, "Warning", w.Level)
	assert.EqualValues(t, 302, w.Code)

	// extract transaction ID
	indx := strings.Index(w.Msg, " ")
	require.Greater(t, indx, 0)
	return w.Msg[:indx]
}

func waitForDTIDResolve(ctx context.Context, t *testing.T, conn *vtgateconn.VTGateSession, dtid string, waitTime time.Duration) {
	unresolved := true
	totalTime := time.After(waitTime)
	for unresolved {
		select {
		case <-totalTime:
			t.Errorf("transaction resolution exceeded wait time of %v", waitTime)
			unresolved = false // break the loop.
		case <-time.After(100 * time.Millisecond):
			qr, err := conn.Execute(ctx, fmt.Sprintf(`show transaction status for '%s'`, dtid), nil, false)
			require.NoError(t, err)
			unresolved = len(qr.Rows) != 0
		}
	}
}
