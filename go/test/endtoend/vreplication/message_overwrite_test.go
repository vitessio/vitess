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

package vreplication

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

const (
	msgOverwriteSchema = `
CREATE TABLE t1 (
  id bigint NOT NULL,
  val varchar(64) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`
	msgOverwriteVSchema = `
{
  "sharded": false,
  "tables": {
    "t1": {}
  }
}
`
	// Distinct text so it is easy to spot in _vt.vreplication.message
	// and _vt.vreplication_log. SIGNAL 45000 is not in isUnrecoverableError,
	// so the controller retries after --vreplication-retry-delay.
	msgOverwriteTriggerError = "retryable apply failure from trigger"
	msgOverwriteTrigger      = `
CREATE TRIGGER t1_fail_insert BEFORE INSERT ON t1
FOR EACH ROW
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '` + msgOverwriteTriggerError + `';
`
	msgOverwritePickedPrefix = "Picked source tablet"
)

// TestVReplicationMessageOverwrite covers the user-visible overwrite of a
// stream's last message by a later lower-tier write (tablet pick, or a
// setState(Running, "") retry).
//
// Subtest retryable_error_then_tablet_pick covers the retry path: a
// retryable apply error leaves state=Running, the controller waits
// RetryDelay, then runBlp picks a tablet and writes "Picked source tablet"
// and replicate() calls setState(Running, ""). On a retry, neither write may
// replace the apply error in _vt.vreplication.message. Once the trigger is
// dropped, the stream must recover and show Running with an empty message.
//
// Subtest on_ddl_stop parks the stream via on_ddl=STOP. The controller
// exits after setState(Stopped, "Stopped at DDL ...") and is not restarted
// for a Stopped row, so the message should survive even on main.
func TestVReplicationMessageOverwrite(t *testing.T) {
	workflow := "msg_ow"
	ksWorkflow := fmt.Sprintf("%s.%s", defaultTargetKs, workflow)
	shard := "0"
	cell := defaultCellName

	savedReplicas := defaultReplicas
	savedRdonly := defaultRdonly
	savedArgs := extraVTTabletArgs
	defaultReplicas = 0
	defaultRdonly = 0
	// 1s retry so the tablet-pick overwrite is observable without a long wait.
	extraVTTabletArgs = []string{"--vreplication-retry-delay=1s"}
	t.Cleanup(func() {
		defaultReplicas = savedReplicas
		defaultRdonly = savedRdonly
		extraVTTabletArgs = savedArgs
	})

	vc = NewVitessCluster(t, nil)
	t.Cleanup(vc.TearDown)
	defaultCell := vc.Cells[cell]

	_, err := vc.AddKeyspace(t, []*Cell{defaultCell}, defaultSourceKs, shard, msgOverwriteVSchema, msgOverwriteSchema, 0, 0, 100, nil)
	require.NoError(t, err)
	_, err = vc.AddKeyspace(t, []*Cell{defaultCell}, defaultTargetKs, shard, msgOverwriteVSchema, "", 0, 0, 200, nil)
	require.NoError(t, err)
	require.NotNil(t, defaultCell.Vtgates[0])
	verifyClusterHealth(t, vc)

	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	t.Cleanup(func() { vtgateConn.Close() })
	targetTab := vc.getPrimaryTablet(t, defaultTargetKs, shard)

	_, err = execVtgateQuery(vtgateConn, defaultSourceKs, "insert into t1(id, val) values (1, 'seed')")
	require.NoError(t, err)

	t.Run("retryable_error_then_tablet_pick", func(t *testing.T) {
		moveTablesAction(t, "Create", cell, workflow, defaultSourceKs, defaultTargetKs, "t1")
		catchup(t, targetTab, workflow, "MoveTables")
		require.NoError(t, waitForWorkflowState(vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String()))

		_, err := targetTab.QueryTablet(msgOverwriteTrigger, defaultTargetKs, true)
		require.NoError(t, err)

		_, err = execVtgateQuery(vtgateConn, defaultSourceKs, "insert into t1(id, val) values (2, 'will fail on target')")
		require.NoError(t, err)

		samples := collectStreamSamples(t, targetTab, workflow, 8*time.Second)
		t.Logf("retryable path _vt.vreplication samples:\n%s", formatStreamSamples(samples))
		logs := readVReplicationLog(t, targetTab)
		t.Logf("retryable path _vt.vreplication_log:\n%s", formatVReplicationLog(logs))

		require.True(t, samplesSaw(samples, msgOverwriteTriggerError) || logsSaw(logs, msgOverwriteTriggerError),
			"never observed the retryable apply error in _vt.vreplication.message or _vt.vreplication_log; samples:\n%s\nlogs:\n%s",
			formatStreamSamples(samples), formatVReplicationLog(logs))

		// After the error is first visible, later samples must still show it
		// as the current message. A tablet-pick or setState(Running, "")
		// overwrite is the failure this test exists to catch.
		afterError := samplesAfterFirstMatch(samples, msgOverwriteTriggerError)
		require.NotEmpty(t, afterError, "no samples after the apply error was first written")
		var overwrites []streamSample
		for _, s := range afterError {
			if strings.Contains(s.message, msgOverwriteTriggerError) {
				continue
			}
			overwrites = append(overwrites, s)
		}
		assert.Empty(t, overwrites,
			"after the retryable apply error was written, later _vt.vreplication.message values lost it (tablet pick / Running-clear). later samples:\n%s\nfull samples:\n%s\nlogs:\n%s",
			formatStreamSamples(overwrites), formatStreamSamples(samples), formatVReplicationLog(logs))

		// Once the cause is gone, the next retry applies the event and its
		// position save clears the kept error, so Workflow show reports a
		// healthy stream again.
		_, err = targetTab.QueryTablet("drop trigger t1_fail_insert", defaultTargetKs, true)
		require.NoError(t, err)
		require.NoError(t, waitForWorkflowState(vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String(), "message=="))
		recovered := readStreamRow(t, targetTab, workflow)
		assert.Equal(t, binlogdatapb.VReplicationWorkflowState_Running.String(), recovered.state)
		assert.Empty(t, recovered.message, "the kept error was not cleared after the stream recovered")

		moveTablesAction(t, "Cancel", cell, workflow, defaultSourceKs, defaultTargetKs, "t1")
	})

	t.Run("on_ddl_stop", func(t *testing.T) {
		// Target still has t1 from the previous workflow; recreate it empty
		// so copy phase is clean.
		_, err := targetTab.QueryTablet("drop table if exists t1", defaultTargetKs, true)
		require.NoError(t, err)

		ddlWorkflow := "msg_ow_ddl"
		ddlKsWorkflow := fmt.Sprintf("%s.%s", defaultTargetKs, ddlWorkflow)
		moveTablesAction(t, "Create", cell, ddlWorkflow, defaultSourceKs, defaultTargetKs, "t1",
			"--on-ddl", binlogdatapb.OnDDLAction_STOP.String())
		catchup(t, targetTab, ddlWorkflow, "MoveTables")
		require.NoError(t, waitForWorkflowState(vc, ddlKsWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String()))

		addColDDL := "alter table t1 add column extra varchar(16)"
		_, err = execVtgateQuery(vtgateConn, defaultSourceKs, addColDDL)
		require.NoError(t, err)
		wantMsg := "Stopped at DDL " + addColDDL
		require.NoError(t, waitForWorkflowState(vc, ddlKsWorkflow,
			binlogdatapb.VReplicationWorkflowState_Stopped.String(), "message=="+wantMsg))

		// Give the controller time to do anything it still might (it should
		// have exited). The stop reason must remain the last message.
		samples := collectStreamSamples(t, targetTab, ddlWorkflow, 4*time.Second)
		t.Logf("on_ddl_stop _vt.vreplication samples:\n%s", formatStreamSamples(samples))
		logs := readVReplicationLog(t, targetTab)
		t.Logf("on_ddl_stop _vt.vreplication_log:\n%s", formatVReplicationLog(logs))

		for _, s := range samples {
			assert.Equal(t, binlogdatapb.VReplicationWorkflowState_Stopped.String(), s.state)
			assert.Equal(t, wantMsg, s.message,
				"Stopped stream message was overwritten; sample=%+v logs:\n%s", s, formatVReplicationLog(logs))
			assert.NotContains(t, s.message, msgOverwritePickedPrefix)
		}

		// Engine reload on tablet restart must not start a picker for a
		// Stopped stream (newController returns early). Confirm the reason
		// is still there after a bounce.
		require.NoError(t, targetTab.TearDown())
		require.NoError(t, targetTab.Setup())
		require.NoError(t, targetTab.WaitForTabletStatusesForTimeout([]string{"SERVING"}, maxWait))
		assert.Eventually(t, func() bool {
			s := readStreamRow(t, targetTab, ddlWorkflow)
			return s.state == binlogdatapb.VReplicationWorkflowState_Stopped.String() && s.message == wantMsg
		}, 30*time.Second, 200*time.Millisecond,
			"after target tablet restart the Stopped DDL reason was lost")

		moveTablesAction(t, "Cancel", cell, ddlWorkflow, defaultSourceKs, defaultTargetKs, "t1")
	})
}

type streamSample struct {
	at      time.Time
	id      string
	state   string
	message string
}

func collectStreamSamples(t *testing.T, tab *cluster.VttabletProcess, workflow string, d time.Duration) []streamSample {
	t.Helper()
	deadline := time.Now().Add(d)
	var samples []streamSample
	var last string
	for time.Now().Before(deadline) {
		s := readStreamRow(t, tab, workflow)
		key := s.state + "|" + s.message
		if key != last {
			samples = append(samples, s)
			last = key
		}
		time.Sleep(50 * time.Millisecond)
	}
	return samples
}

func readStreamRow(t *testing.T, tab *cluster.VttabletProcess, workflow string) streamSample {
	t.Helper()
	query := sqlparser.BuildParsedQuery(
		"select id, state, message from %s.vreplication where workflow=%s",
		sidecarDBIdentifier, encodeString(workflow),
	).Query
	qr, err := tab.QueryTablet(query, "", false)
	require.NoError(t, err)
	require.NotEmpty(t, qr.Rows, "no _vt.vreplication row for workflow %s", workflow)
	row := qr.Named().Rows[0]
	return streamSample{
		at:      time.Now(),
		id:      row.AsString("id", ""),
		state:   row.AsString("state", ""),
		message: row.AsString("message", ""),
	}
}

type logRow struct {
	id      string
	typ     string
	state   string
	message string
	count   string
}

func readVReplicationLog(t *testing.T, tab *cluster.VttabletProcess) []logRow {
	t.Helper()
	query := sqlparser.BuildParsedQuery(
		"select id, type, state, message, count from %s.vreplication_log order by id",
		sidecarDBIdentifier,
	).Query
	qr, err := tab.QueryTablet(query, "", false)
	require.NoError(t, err)
	out := make([]logRow, 0, len(qr.Rows))
	for _, row := range qr.Named().Rows {
		out = append(out, logRow{
			id:      row.AsString("id", ""),
			typ:     row.AsString("type", ""),
			state:   row.AsString("state", ""),
			message: row.AsString("message", ""),
			count:   row.AsString("count", ""),
		})
	}
	return out
}

func samplesSaw(samples []streamSample, substr string) bool {
	for _, s := range samples {
		if strings.Contains(s.message, substr) {
			return true
		}
	}
	return false
}

func logsSaw(logs []logRow, substr string) bool {
	for _, l := range logs {
		if strings.Contains(l.message, substr) {
			return true
		}
	}
	return false
}

func samplesAfterFirstMatch(samples []streamSample, substr string) []streamSample {
	for i, s := range samples {
		if strings.Contains(s.message, substr) {
			return samples[i:]
		}
	}
	return nil
}

func formatStreamSamples(samples []streamSample) string {
	var b strings.Builder
	for _, s := range samples {
		fmt.Fprintf(&b, "  %s id=%s state=%q message=%q\n", s.at.Format(time.RFC3339Nano), s.id, s.state, s.message)
	}
	return b.String()
}

func formatVReplicationLog(logs []logRow) string {
	var b strings.Builder
	for _, l := range logs {
		fmt.Fprintf(&b, "  id=%s type=%s state=%q count=%s message=%q\n", l.id, l.typ, l.state, l.count, l.message)
	}
	return b.String()
}
