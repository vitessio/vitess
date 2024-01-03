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
	_ "embed"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/wrangler"
)

var (
	//go:embed testdata/unknown-params-logged-vschema.json
	unknownParamsLoggedVSchema string

	//go:embed testdata/unknown-params-logged-dry-run-vschema.json
	unknownParamsLoggedDryRunVSchema string
)

// TestApplyVSchema tests the the MoveTables client command
// via the commandVRApplyVSchema() cmd handler.
func TestApplyVSchema(t *testing.T) {
	shard := "0"
	ks := "ks"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := newTestVTCtlEnv(ctx)
	defer env.close()
	_ = env.addTablet(100, ks, shard, &topodatapb.KeyRange{}, topodatapb.TabletType_PRIMARY)

	tests := []struct {
		name          string
		args          []string
		expectResults func()
		want          string
	}{
		{
			name: "EmptyVSchema",
			args: []string{"--vschema", "{}", ks},
			want: "New VSchema object:\n{}\nIf this is not what you expected, check the input data (as JSON parsing will skip unexpected fields).\n\n",
		},
		{
			name: "UnknownParamsLogged",
			args: []string{"--vschema", unknownParamsLoggedVSchema, ks},
			want: `/New VSchema object:
{
  "sharded": true,
  "vindexes": {
    "binary_vdx": {
      "type": "binary",
      "params": {
        "hello": "world"
      }
    },
    "hash_vdx": {
      "type": "hash",
      "params": {
        "foo": "bar",
        "hello": "world"
      }
    }
  }
}
If this is not what you expected, check the input data \(as JSON parsing will skip unexpected fields\)\.

.*W.* .* vtctl.go:.* Unknown parameter in vindex binary_vdx: hello
W.* .* vtctl.go:.* Unknown parameter in vindex hash_vdx: foo
W.* .* vtctl.go:.* Unknown parameter in vindex hash_vdx: hello`,
		},
		{
			name: "UnknownParamsLoggedWithDryRun",
			args: []string{"--vschema", unknownParamsLoggedDryRunVSchema, "--dry-run", ks},
			want: `/New VSchema object:
{
  "sharded": true,
  "vindexes": {
    "binary_vdx": {
      "type": "binary",
      "params": {
        "hello": "world"
      }
    },
    "hash_vdx": {
      "type": "hash",
      "params": {
        "foo": "bar",
        "hello": "world"
      }
    }
  }
}
If this is not what you expected, check the input data \(as JSON parsing will skip unexpected fields\)\.

.*W.* .* vtctl.go:.* Unknown parameter in vindex binary_vdx: hello
W.* .* vtctl.go:.* Unknown parameter in vindex hash_vdx: foo
W.* .* vtctl.go:.* Unknown parameter in vindex hash_vdx: hello
Dry run: Skipping update of VSchema`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subFlags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			err := commandApplyVSchema(ctx, env.wr, subFlags, tt.args)
			require.NoError(t, err)
			if strings.HasPrefix(tt.want, "/") {
				require.Regexp(t, regexp.MustCompile(tt.want[1:]), env.cmdlog.String())
			} else {
				require.Equal(t, tt.want, env.cmdlog.String())
			}
			env.cmdlog.Clear()
			env.tmc.clearResults()
		})
	}
}

// TestMoveTables tests the the MoveTables client command
// via the commandVReplicationWorkflow() cmd handler.
// This currently only tests the Progress action (which is
// a parent of the Show action) but it can be used to test
// other actions as well.
func TestMoveTables(t *testing.T) {
	vrID := 1
	shard := "0"
	sourceKs := "sourceks"
	targetKs := "targetks"
	table1 := "customer"
	table2 := "customer_order"
	wf := "testwf"
	ksWf := fmt.Sprintf("%s.%s", targetKs, wf)
	minTableSize := 16384 // a single 16KiB InnoDB page
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := newTestVTCtlEnv(ctx)
	defer env.close()
	source := env.addTablet(100, sourceKs, shard, &topodatapb.KeyRange{}, topodatapb.TabletType_PRIMARY)
	target := env.addTablet(200, targetKs, shard, &topodatapb.KeyRange{}, topodatapb.TabletType_PRIMARY)
	sourceCol := fmt.Sprintf(`keyspace:"%s" shard:"%s" filter:{rules:{match:"%s" filter:"select * from %s"} rules:{match:"%s" filter:"select * from %s"}}`,
		sourceKs, shard, table1, table1, table2, table2)
	bls := &binlogdatapb.BinlogSource{
		Keyspace: sourceKs,
		Shard:    shard,
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{
				{
					Match:  table1,
					Filter: fmt.Sprintf("select * from %s", table1),
				},
				{
					Match:  table2,
					Filter: fmt.Sprintf("select * from %s", table2),
				},
			},
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
					fmt.Sprintf("select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (%d) and id in (select max(id) from _vt.copy_state where vrepl_id in (%d) group by vrepl_id, table_name)",
						vrID, vrID),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"vrepl_id|table_name|lastpk",
						"int64|varchar|varbinary"),
						fmt.Sprintf("%d|%s|", vrID, table1),
						fmt.Sprintf("%d|%s|", vrID, table2),
					),
				)
				env.tmc.setDBAResults(
					target.tablet,
					fmt.Sprintf("select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = %d",
						vrID),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name",
						"varchar"),
						table1,
						table2,
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
					fmt.Sprintf("select table_name, table_rows, data_length from information_schema.tables where table_schema = 'vt_%s' and table_name in ('%s','%s')",
						targetKs, table1, table2),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name|table_rows|data_length",
						"varchar|int64|int64"),
						fmt.Sprintf("%s|0|%d", table1, minTableSize),
						fmt.Sprintf("%s|0|%d", table2, minTableSize),
					),
				)
				env.tmc.setDBAResults(
					source.tablet,
					fmt.Sprintf("select table_name, table_rows, data_length from information_schema.tables where table_schema = 'vt_%s' and table_name in ('%s','%s')",
						sourceKs, table1, table2),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name|table_rows|data_length",
						"varchar|int64|int64"),
						fmt.Sprintf("%s|10|%d", table1, minTableSize),
						fmt.Sprintf("%s|10|%d", table2, minTableSize),
					),
				)
			},
			want: fmt.Sprintf("\nCopy Progress (approx):\n\n\ncustomer: rows copied 0/10 (0%%), size copied 16384/16384 (100%%)\ncustomer_order: rows copied 0/10 (0%%), size copied 16384/16384 (100%%)\n\n\n\nThe following vreplication streams exist for workflow %s:\n\nid=%d on %s/%s-0000000%d: Status: Copying. VStream has not started.\n\n\n",
				ksWf, vrID, shard, env.cell, target.tablet.Alias.Uid),
		},
		{
			name:         "Error",
			workflowType: wrangler.MoveTablesWorkflow,
			args:         []string{"Progress", ksWf},
			expectResults: func() {
				env.tmc.setVRResults(
					target.tablet,
					fmt.Sprintf("select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (%d) and id in (select max(id) from _vt.copy_state where vrepl_id in (%d) group by vrepl_id, table_name)",
						vrID, vrID),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"vrepl_id|table_name|lastpk",
						"int64|varchar|varbinary"),
						fmt.Sprintf("%d|%s|", vrID, table1),
						fmt.Sprintf("%d|%s|", vrID, table2),
					),
				)
				env.tmc.setDBAResults(
					target.tablet,
					fmt.Sprintf("select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = %d",
						vrID),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name",
						"varchar"),
						table1,
						table2,
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
					fmt.Sprintf("select table_name, table_rows, data_length from information_schema.tables where table_schema = 'vt_%s' and table_name in ('%s','%s')",
						targetKs, table1, table2),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name|table_rows|data_length",
						"varchar|int64|int64"),
						fmt.Sprintf("%s|5|%d", table1, minTableSize),
						fmt.Sprintf("%s|5|%d", table2, minTableSize),
					),
				)
				env.tmc.setDBAResults(
					source.tablet,
					fmt.Sprintf("select table_name, table_rows, data_length from information_schema.tables where table_schema = 'vt_%s' and table_name in ('%s','%s')",
						sourceKs, table1, table2),
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"table_name|table_rows|data_length",
						"varchar|int64|int64"),
						fmt.Sprintf("%s|10|%d", table1, minTableSize),
						fmt.Sprintf("%s|10|%d", table2, minTableSize),
					),
				)
			},
			want: fmt.Sprintf("\nCopy Progress (approx):\n\n\ncustomer: rows copied 5/10 (50%%), size copied 16384/16384 (100%%)\ncustomer_order: rows copied 5/10 (50%%), size copied 16384/16384 (100%%)\n\n\n\nThe following vreplication streams exist for workflow %s:\n\nid=%d on %s/%s-0000000%d: Status: Error: Duplicate entry '6' for key 'customer.PRIMARY' (errno 1062) (sqlstate 23000) during query: insert into customer(customer_id,email) values (6,'mlord@planetscale.com').\n\n\n",
				ksWf, vrID, shard, env.cell, target.tablet.Alias.Uid),
		},
		{
			name:         "Running",
			workflowType: wrangler.MoveTablesWorkflow,
			args:         []string{"Progress", ksWf},
			expectResults: func() {
				env.tmc.setVRResults(
					target.tablet,
					fmt.Sprintf("select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (%d) and id in (select max(id) from _vt.copy_state where vrepl_id in (%d) group by vrepl_id, table_name)",
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
			err := commandVReplicationWorkflow(ctx, env.wr, subFlags, tt.args, tt.workflowType)
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

func TestGenerateOnlineDDLQuery(t *testing.T) {
	tcases := []struct {
		cmd          string
		arg          string
		allSupported bool
		expectError  bool
		expectQuery  string
	}{
		{
			"launch",
			"all",
			true,
			false,
			"alter vitess_migration launch all",
		},
		{
			"launch-all",
			"",
			true,
			false,
			"alter vitess_migration launch all",
		},
		{
			"launch",
			"718169cc_1fea_11ee_82b1_0a43f95f28a3",
			true,
			false,
			"alter vitess_migration '718169cc_1fea_11ee_82b1_0a43f95f28a3' launch",
		},
		{
			"cancel",
			"718169cc_1fea_11ee_82b1_0a43f95f28a3",
			true,
			false,
			"alter vitess_migration '718169cc_1fea_11ee_82b1_0a43f95f28a3' cancel",
		},
		{
			"unthrottle",
			"718169cc_1fea_11ee_82b1_0a43f95f28a3",
			true,
			false,
			"alter vitess_migration '718169cc_1fea_11ee_82b1_0a43f95f28a3' unthrottle",
		},
		{
			"unthrottle",
			"",
			true,
			true,
			"",
		},
		{
			"unthrottle-all",
			"all",
			true,
			true,
			"",
		},
		{
			"unthrottle-all",
			"718169cc_1fea_11ee_82b1_0a43f95f28a3",
			true,
			true,
			"",
		},
		{
			"retry",
			"718169cc_1fea_11ee_82b1_0a43f95f28a3",
			false,
			false,
			"alter vitess_migration '718169cc_1fea_11ee_82b1_0a43f95f28a3' retry",
		},
		{
			"retry-all",
			"718169cc_1fea_11ee_82b1_0a43f95f28a3",
			false,
			true,
			"",
		},
		{
			"retry-all",
			"",
			false,
			true,
			"",
		},
		{
			"retry",
			"all",
			false,
			true,
			"",
		},
	}
	for _, tcase := range tcases {
		t.Run(fmt.Sprintf("%s %s", tcase.cmd, tcase.arg), func(t *testing.T) {
			query, err := generateOnlineDDLQuery(tcase.cmd, tcase.arg, tcase.allSupported)
			if tcase.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tcase.expectQuery, query)
			}
		})
	}
}
