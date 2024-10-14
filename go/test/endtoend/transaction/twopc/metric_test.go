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
	utils.Exec(t, conn, "set transaction_mode = multi")
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

	utils.Exec(t, conn, "set transaction_mode = twopc")
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

// TestMetricOnFailure tests unresolved commit metrics.
func TestMetricOnFailure(t *testing.T) {
	defer cleanup(t)

	initialCount := GetVarValue[float64](t, "CommitUnresolved")

	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "dt_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	conn := vtgateConn.Session("", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err = conn.Execute(ctx, "begin", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "insert into twopc_user(id, name) values(7,'foo'), (8,'bar')", nil)
	require.NoError(t, err)

	// fail after mm commit.
	newCtx := callerid.NewContext(ctx, callerid.NewEffectiveCallerID("MMCommitted_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil)
	require.ErrorContains(t, err, "Fail After MM commit")

	updatedCount := GetVarValue[float64](t, "CommitUnresolved")
	assert.EqualValues(t, 1, updatedCount-initialCount, "CommitUnresolved")

	waitForResolve(ctx, t, conn, 5*time.Second)

	_, err = conn.Execute(ctx, "begin", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "insert into twopc_user(id, name) values(9,'foo')", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "insert into twopc_user(id, name) values(10,'apa')", nil)
	require.NoError(t, err)

	// fail during rm commit.
	newCtx = callerid.NewContext(ctx, callerid.NewEffectiveCallerID("RMCommit_-40_FailNow", "", ""), nil)
	_, err = conn.Execute(newCtx, "commit", nil)
	require.ErrorContains(t, err, "Fail During RM commit")

	updatedCount = GetVarValue[float64](t, "CommitUnresolved")
	assert.EqualValues(t, 2, updatedCount-initialCount, "CommitUnresolved")

	waitForResolve(ctx, t, conn, 5*time.Second)
}

type commitMetric struct {
	TotalCount  float64
	SingleCount float64
	MultiCount  float64
	TwoPCCount  float64
}

func getCommitMetric(t *testing.T) commitMetric {
	t.Helper()

	vars, err := clusterInstance.VtgateProcess.GetVars()
	require.NoError(t, err)

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

func GetVarValue[T any](t *testing.T, key string) T {
	vars, err := clusterInstance.VtgateProcess.GetVars()
	require.NoError(t, err)

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

	qr, err := conn.Execute(ctx, "show warnings", nil)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)

	// validate warning output
	w := toWarn(qr.Rows[0])
	assert.Equal(t, "Warning", w.level)
	assert.EqualValues(t, 302, w.code)

	// extract transaction ID
	indx := strings.Index(w.msg, " ")
	require.Greater(t, indx, 0)
	dtid := w.msg[:indx]

	unresolved := true
	totalTime := time.After(waitTime)
	for unresolved {
		select {
		case <-totalTime:
			t.Errorf("transaction resolution exceeded wait time of %v", waitTime)
			unresolved = false // break the loop.
		case <-time.After(100 * time.Millisecond):
			qr, err = conn.Execute(ctx, fmt.Sprintf(`show transaction status for '%v'`, dtid), nil)
			require.NoError(t, err)
			unresolved = len(qr.Rows) != 0
		}
	}
}
