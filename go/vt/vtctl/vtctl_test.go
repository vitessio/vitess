/*
Copyright 2023 The Vitess Authors.

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

package vtctl

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/wrangler"
)

// TestMoveTables tests the the MoveTables client command
// via the commandVRWorkflow() cmd handler.
// This currently only tests the Progress action (which is
// a parent of the Show action) but it can be used to test
// other actions as well.
func TestMoveTables(t *testing.T) {
	vrID := 1
	shard := "0"
	sourceKs := "sourceks"
	targetKs := "targetks"
	table := "customer"
	wf := "testwf"
	ksWf := fmt.Sprintf("%s.%s", targetKs, wf)
	minTableSize := 16384 // a single 16KiB InnoDB page
	ctx := context.Background()
	env := newTestVTCtlEnv()
	defer env.close()
	source := env.addTablet(100, sourceKs, shard, &topodatapb.KeyRange{}, topodatapb.TabletType_PRIMARY)
	target := env.addTablet(200, targetKs, shard, &topodatapb.KeyRange{}, topodatapb.TabletType_PRIMARY)
	sourceCol := fmt.Sprintf(`keyspace:"%s" shard:"%s" filter:{rules:{match:"%s" filter:"select * from %s"}}`,
		sourceKs, shard, table, table)
	bls := &binlogdatapb.BinlogSource{
		Keyspace: sourceKs,
		Shard:    shard,
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  table,
				Filter: fmt.Sprintf("select * from %s", table),
			}},
		},
	}
	now := time.Now().UTC().Unix()
	expectGlobalResults := func() {
		env.tmc.setVRResults(
			target.tablet,
			fmt.Sprintf("select id, source, message, cell, tablet_types, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where workflow='%s' and db_name='vt_%s'",
				wf, targetKs),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
				"int64|varchar|varchar|varchar|varchar|int64|int64|int64"),
				fmt.Sprintf("%d|%s||%s|primary|%d|%d",
					vrID, sourceCol, env.cell, binlogdatapb.VReplicationWorkflowType_MoveTables, binlogdatapb.VReplicationWorkflowSubType_None),
			),
		)
	}

	tests := []struct {
		name          string
		workflowType  wrangler.VReplicationWorkflowType
		args          []string
		expectResults func()
		want          string
	}{
		{
			name:         "NotStarted",
			workflowType: wrangler.MoveTablesWorkflow,
			args:         []string{"Progress", ksWf},
			expectResults: func() {
				env.tmc.setVRResults(
					target.tablet,
					fmt.Sprintf("select table_name, lastpk from _vt.copy_state where vrepl_id = %d and id in (select max(id) from _vt.copy_state where vrepl_id = %d group by vrepl_id, table_name)",
						vrID, vrID),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name|lastpk",
						"varchar|varbinary"),
						fmt.Sprintf("%s|", table),
					),
				)
				env.tmc.setDBAResults(
					target.tablet,
					fmt.Sprintf("select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = %d",
						vrID),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name",
						"varchar"),
						table,
					),
				)
				env.tmc.setVRResults(
					target.tablet,
					fmt.Sprintf("select id, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, transaction_timestamp, time_heartbeat, time_throttled, component_throttled, message, tags, workflow_type, workflow_sub_type, defer_secondary_keys, rows_copied from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						targetKs, wf),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"id|source|pos|stop_pos|max_replication_lag|state|db_name|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|message|tags|workflow_type|workflow_sub_type|defer_secondary_keys|rows_copied",
						"int64|varchar|varchar|varchar|int64|varchar|varchar|int64|int64|int64|int64|int64|varchar|varchar|varchar|int64|int64|int64|int64"),
						fmt.Sprintf("%d|%s|||0|Running|vt_%s|0|0|0|0||||%d|%d|0",
							vrID, bls, sourceKs, binlogdatapb.VReplicationWorkflowType_MoveTables, binlogdatapb.VReplicationWorkflowSubType_None),
					),
				)
				env.tmc.setDBAResults(
					target.tablet,
					fmt.Sprintf("select table_name, table_rows, data_length from information_schema.tables where table_schema = 'vt_%s' and table_name in ('%s')",
						targetKs, table),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name|table_rows|data_length",
						"varchar|int64|int64"),
						fmt.Sprintf("%s|0|%d", table, minTableSize),
					),
				)
				env.tmc.setDBAResults(
					source.tablet,
					fmt.Sprintf("select table_name, table_rows, data_length from information_schema.tables where table_schema = 'vt_%s' and table_name in ('%s')",
						sourceKs, table),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name|table_rows|data_length",
						"varchar|int64|int64"),
						fmt.Sprintf("%s|10|%d", table, minTableSize),
					),
				)
			},
			want: fmt.Sprintf("\nCopy Progress (approx):\n\n\ncustomer: rows copied 0/10 (0%%), size copied 16384/16384 (100%%)\n\n\n\nThe following vreplication streams exist for workflow %s:\n\nid=%d on %s/%s-0000000%d: Status: Copying. VStream has not started.\n\n\n",
				ksWf, vrID, shard, env.cell, target.tablet.Alias.Uid),
		},
		{
			name:         "Error",
			workflowType: wrangler.MoveTablesWorkflow,
			args:         []string{"Progress", ksWf},
			expectResults: func() {
				env.tmc.setVRResults(
					target.tablet,
					fmt.Sprintf("select table_name, lastpk from _vt.copy_state where vrepl_id = %d and id in (select max(id) from _vt.copy_state where vrepl_id = %d group by vrepl_id, table_name)",
						vrID, vrID),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name|lastpk",
						"varchar|varbinary"),
						fmt.Sprintf("%s|", table),
					),
				)
				env.tmc.setDBAResults(
					target.tablet,
					fmt.Sprintf("select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = %d",
						vrID),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name",
						"varchar"),
						table,
					),
				)
				env.tmc.setVRResults(
					target.tablet,
					fmt.Sprintf("select id, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, transaction_timestamp, time_heartbeat, time_throttled, component_throttled, message, tags, workflow_type, workflow_sub_type, defer_secondary_keys, rows_copied from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						targetKs, wf),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"id|source|pos|stop_pos|max_replication_lag|state|db_name|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|message|tags|workflow_type|workflow_sub_type|defer_secondary_keys|rows_copied",
						"int64|varchar|varchar|varchar|int64|varchar|varchar|int64|int64|int64|int64|int64|varchar|varchar|varchar|int64|int64|int64|int64"),
						fmt.Sprintf("%d|%s|||0|Error|vt_%s|0|0|0|0||Duplicate entry '6' for key 'customer.PRIMARY' (errno 1062) (sqlstate 23000) during query: insert into customer(customer_id,email) values (6,'mlord@planetscale.com')||%d|%d|0",
							vrID, bls, sourceKs, binlogdatapb.VReplicationWorkflowType_MoveTables, binlogdatapb.VReplicationWorkflowSubType_None),
					),
				)
				env.tmc.setDBAResults(
					target.tablet,
					fmt.Sprintf("select table_name, table_rows, data_length from information_schema.tables where table_schema = 'vt_%s' and table_name in ('%s')",
						targetKs, table),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name|table_rows|data_length",
						"varchar|int64|int64"),
						fmt.Sprintf("%s|5|%d", table, minTableSize),
					),
				)
				env.tmc.setDBAResults(
					source.tablet,
					fmt.Sprintf("select table_name, table_rows, data_length from information_schema.tables where table_schema = 'vt_%s' and table_name in ('%s')",
						sourceKs, table),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name|table_rows|data_length",
						"varchar|int64|int64"),
						fmt.Sprintf("%s|10|%d", table, minTableSize),
					),
				)
			},
			want: fmt.Sprintf("\nCopy Progress (approx):\n\n\ncustomer: rows copied 5/10 (50%%), size copied 16384/16384 (100%%)\n\n\n\nThe following vreplication streams exist for workflow %s:\n\nid=%d on %s/%s-0000000%d: Status: Error: Duplicate entry '6' for key 'customer.PRIMARY' (errno 1062) (sqlstate 23000) during query: insert into customer(customer_id,email) values (6,'mlord@planetscale.com').\n\n\n",
				ksWf, vrID, shard, env.cell, target.tablet.Alias.Uid),
		},
		{
			name:         "Running",
			workflowType: wrangler.MoveTablesWorkflow,
			args:         []string{"Progress", ksWf},
			expectResults: func() {
				env.tmc.setVRResults(
					target.tablet,
					fmt.Sprintf("select table_name, lastpk from _vt.copy_state where vrepl_id = %d and id in (select max(id) from _vt.copy_state where vrepl_id = %d group by vrepl_id, table_name)",
						vrID, vrID),
					&sqltypes.Result{},
				)
				env.tmc.setDBAResults(
					target.tablet,
					fmt.Sprintf("select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = %d",
						vrID),
					&sqltypes.Result{},
				)
				env.tmc.setVRResults(
					target.tablet,
					fmt.Sprintf("select id, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, transaction_timestamp, time_heartbeat, time_throttled, component_throttled, message, tags, workflow_type, workflow_sub_type, defer_secondary_keys, rows_copied from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						targetKs, wf),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"id|source|pos|stop_pos|max_replication_lag|state|db_name|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|message|tags|workflow_type|workflow_sub_type|defer_secondary_keys|rows_copied",
						"int64|varchar|varchar|varchar|int64|varchar|varchar|int64|int64|int64|int64|int64|varchar|varchar|varchar|int64|int64|int64|int64"),
						fmt.Sprintf("%d|%s|MySQL56/4ec30b1e-8ee2-11ed-a1eb-0242ac120002:1-15||0|Running|vt_%s|%d|%d|%d|0||||%d|%d|0",
							vrID, bls, sourceKs, now, now, now, binlogdatapb.VReplicationWorkflowType_MoveTables, binlogdatapb.VReplicationWorkflowSubType_None),
					),
				)
			},
			want: fmt.Sprintf("/\nThe following vreplication streams exist for workflow %s:\n\nid=%d on %s/%s-0000000%d: Status: Running. VStream Lag: .* Tx time: .*",
				ksWf, vrID, shard, env.cell, target.tablet.Alias.Uid),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subFlags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			expectGlobalResults()
			tt.expectResults()
			err := commandVRWorkflow(ctx, env.wr, subFlags, tt.args, tt.workflowType)
			require.NoError(t, err)
			if strings.HasPrefix(tt.want, "/") {
				require.Regexp(t, tt.want[1:], env.cmdlog.String())
			} else {
				require.Equal(t, tt.want, env.cmdlog.String())
			}
			env.cmdlog.Clear()
			env.tmc.clearResults()
		})
	}
}
