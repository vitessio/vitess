/*
Copyright 2021 The Vitess Authors.

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

package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/ptr"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	allTabletTypes = []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}

	roTabletTypes = []topodatapb.TabletType{
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}
)

type fakeTMC struct {
	tmclient.TabletManagerClient
	vrepQueriesByTablet map[string]map[string]*querypb.QueryResult
}

func (fake *fakeTMC) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	alias := topoproto.TabletAliasString(tablet.Alias)
	tabletQueries, ok := fake.vrepQueriesByTablet[alias]
	if !ok {
		return nil, fmt.Errorf("no query map registered on fake for %s", alias)
	}

	p3qr, ok := tabletQueries[query]
	if !ok {
		return nil, fmt.Errorf("no result on fake for query %q on tablet %s", query, alias)
	}

	return p3qr, nil
}

func TestCheckReshardingJournalExistsOnTablet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  100,
		},
	}
	journal := &binlogdatapb.Journal{
		Id:            1,
		MigrationType: binlogdatapb.MigrationType_SHARDS,
		Tables:        []string{"t1", "t2"},
	}
	journalBytes, err := prototext.Marshal(journal)
	require.NoError(t, err, "could not marshal journal %+v into bytes", journal)

	// get some bytes that will fail to unmarshal into a binlogdatapb.Journal
	tabletBytes, err := prototext.Marshal(tablet)
	require.NoError(t, err, "could not marshal tablet %+v into bytes", tablet)

	p3qr := sqltypes.ResultToProto3(sqltypes.MakeTestResult([]*querypb.Field{
		{
			Name: "val",
			Type: querypb.Type_BLOB,
		},
	}, string(journalBytes)))

	tests := []struct {
		name        string
		tablet      *topodatapb.Tablet
		result      *querypb.QueryResult
		journal     *binlogdatapb.Journal
		shouldExist bool
		shouldErr   bool
	}{
		{
			name:        "journal exists",
			tablet:      tablet,
			result:      p3qr,
			shouldExist: true,
			journal:     journal,
		},
		{
			name:        "journal does not exist",
			tablet:      tablet,
			result:      sqltypes.ResultToProto3(sqltypes.MakeTestResult(nil)),
			journal:     &binlogdatapb.Journal{},
			shouldExist: false,
		},
		{
			name:   "cannot unmarshal into journal",
			tablet: tablet,
			result: sqltypes.ResultToProto3(sqltypes.MakeTestResult([]*querypb.Field{
				{
					Name: "val",
					Type: querypb.Type_BLOB,
				},
			}, string(tabletBytes))),
			shouldErr: true,
		},
		{
			name: "VReplicationExec fails on tablet",
			tablet: &topodatapb.Tablet{ // Here we use a different tablet to force the fake to return an error
				Alias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmc := &fakeTMC{
				vrepQueriesByTablet: map[string]map[string]*querypb.QueryResult{
					topoproto.TabletAliasString(tablet.Alias): { // always use the tablet shared by these tests cases
						"select val from _vt.resharding_journal where id=1": tt.result,
					},
				},
			}

			ws := NewServer(vtenv.NewTestEnv(), nil, tmc)
			journal, exists, err := ws.CheckReshardingJournalExistsOnTablet(ctx, tt.tablet, 1)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			existAssertionMsg := "expected journal to "
			if tt.shouldExist {
				existAssertionMsg += "already exist on tablet"
			} else {
				existAssertionMsg += "not exist"
			}

			assert.Equal(t, tt.shouldExist, exists, existAssertionMsg)
			utils.MustMatch(t, tt.journal, journal, "journal in resharding_journal did not match")
		})
	}
}

func TestMoveTablesComplete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	table1Name := "t1"
	table2Name := "t1_2"
	table3Name := "t1_3"
	tableTemplate := "CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))"
	sourceKeyspaceName := "sourceks"
	targetKeyspaceName := "targetks"
	lockName := fmt.Sprintf("%s/%s", targetKeyspaceName, workflowName)
	schema := map[string]*tabletmanagerdatapb.SchemaDefinition{
		table1Name: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   table1Name,
					Schema: fmt.Sprintf(tableTemplate, table1Name),
				},
			},
		},
		table2Name: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   table2Name,
					Schema: fmt.Sprintf(tableTemplate, table2Name),
				},
			},
		},
		table3Name: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   table3Name,
					Schema: fmt.Sprintf(tableTemplate, table3Name),
				},
			},
		},
	}

	testcases := []struct {
		name                           string
		sourceKeyspace, targetKeyspace *testKeyspace
		preFunc                        func(t *testing.T, env *testEnv)
		req                            *vtctldatapb.MoveTablesCompleteRequest
		expectedSourceQueries          []*queryResult
		expectedTargetQueries          []*queryResult
		want                           *vtctldatapb.MoveTablesCompleteResponse
		wantErr                        string
		postFunc                       func(t *testing.T, env *testEnv)
	}{
		{
			name: "basic",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.MoveTablesCompleteRequest{
				TargetKeyspace: targetKeyspaceName,
				Workflow:       workflowName,
			},
			expectedSourceQueries: []*queryResult{
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", sourceKeyspaceName, table1Name),
					result: &querypb.QueryResult{},
				},
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", sourceKeyspaceName, table2Name),
					result: &querypb.QueryResult{},
				},
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", sourceKeyspaceName, table3Name),
					result: &querypb.QueryResult{},
				},
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						sourceKeyspaceName, ReverseWorkflowName(workflowName)),
					result: &querypb.QueryResult{},
				},
			},
			expectedTargetQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						targetKeyspaceName, workflowName),
					result: &querypb.QueryResult{},
				},
			},
			want: &vtctldatapb.MoveTablesCompleteResponse{
				Summary: fmt.Sprintf("Successfully completed the %s workflow in the %s keyspace",
					workflowName, targetKeyspaceName),
			},
		},
		{
			name: "keep routing rules and data",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.MoveTablesCompleteRequest{
				TargetKeyspace:   targetKeyspaceName,
				Workflow:         workflowName,
				KeepRoutingRules: true,
				KeepData:         true,
			},
			expectedSourceQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						sourceKeyspaceName, ReverseWorkflowName(workflowName)),
					result: &querypb.QueryResult{},
				},
			},
			expectedTargetQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						targetKeyspaceName, workflowName),
					result: &querypb.QueryResult{},
				},
			},
			postFunc: func(t *testing.T, env *testEnv) {
				env.confirmRoutingAllTablesToTarget(t)
			},
			want: &vtctldatapb.MoveTablesCompleteResponse{
				Summary: fmt.Sprintf("Successfully completed the %s workflow in the %s keyspace",
					workflowName, targetKeyspaceName),
			},
		},
		{
			name: "rename tables",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.MoveTablesCompleteRequest{
				TargetKeyspace: targetKeyspaceName,
				Workflow:       workflowName,
				RenameTables:   true,
			},
			expectedSourceQueries: []*queryResult{
				{
					query:  fmt.Sprintf("rename table `vt_%s`.`%s` TO `vt_%s`.`_%s_old`", sourceKeyspaceName, table1Name, sourceKeyspaceName, table1Name),
					result: &querypb.QueryResult{},
				},
				{
					query:  fmt.Sprintf("rename table `vt_%s`.`%s` TO `vt_%s`.`_%s_old`", sourceKeyspaceName, table2Name, sourceKeyspaceName, table2Name),
					result: &querypb.QueryResult{},
				},
				{
					query:  fmt.Sprintf("rename table `vt_%s`.`%s` TO `vt_%s`.`_%s_old`", sourceKeyspaceName, table3Name, sourceKeyspaceName, table3Name),
					result: &querypb.QueryResult{},
				},
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						sourceKeyspaceName, ReverseWorkflowName(workflowName)),
					result: &querypb.QueryResult{},
				},
			},
			expectedTargetQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						targetKeyspaceName, workflowName),
					result: &querypb.QueryResult{},
				},
			},
			want: &vtctldatapb.MoveTablesCompleteResponse{
				Summary: fmt.Sprintf("Successfully completed the %s workflow in the %s keyspace",
					workflowName, targetKeyspaceName),
			},
		},
		{
			name: "ignore source keyspace",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.MoveTablesCompleteRequest{
				TargetKeyspace:       targetKeyspaceName,
				Workflow:             workflowName,
				IgnoreSourceKeyspace: true,
			},
			preFunc: func(t *testing.T, env *testEnv) {
				err := env.ts.DeleteKeyspace(ctx, sourceKeyspaceName)
				require.NoError(t, err)
			},
			postFunc: func(t *testing.T, env *testEnv) {
				err := env.ts.CreateKeyspace(ctx, sourceKeyspaceName, &topodatapb.Keyspace{})
				require.NoError(t, err)
			},
			expectedTargetQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						targetKeyspaceName, workflowName),
					result: &querypb.QueryResult{},
				},
			},
			want: &vtctldatapb.MoveTablesCompleteResponse{
				Summary: fmt.Sprintf("Successfully completed the %s workflow in the %s keyspace",
					workflowName, targetKeyspaceName),
			},
		},
		{
			name: "named lock held",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.MoveTablesCompleteRequest{
				TargetKeyspace:   targetKeyspaceName,
				Workflow:         workflowName,
				KeepRoutingRules: true,
			},
			preFunc: func(t *testing.T, env *testEnv) {
				_, _, err := env.ts.LockName(ctx, lockName, "test")
				require.NoError(t, err)
				topo.LockTimeout = 500 * time.Millisecond
			},
			postFunc: func(t *testing.T, env *testEnv) {
				topo.LockTimeout = 45 * time.Second // reset it to the default
			},
			wantErr: fmt.Sprintf("failed to lock the %s workflow: deadline exceeded: internal/named_locks/%s", lockName, lockName),
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotNil(t, tc.sourceKeyspace)
			require.NotNil(t, tc.targetKeyspace)
			require.NotNil(t, tc.req)
			env := newTestEnv(t, ctx, defaultCellName, tc.sourceKeyspace, tc.targetKeyspace)
			defer env.close()
			env.tmc.schema = schema
			env.tmc.frozen.Store(true)
			if tc.expectedSourceQueries != nil {
				require.NotNil(t, env.tablets[tc.sourceKeyspace.KeyspaceName])
				for _, eq := range tc.expectedSourceQueries {
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, eq)
				}
			}
			if tc.expectedTargetQueries != nil {
				require.NotNil(t, env.tablets[tc.targetKeyspace.KeyspaceName])
				for _, eq := range tc.expectedTargetQueries {
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, eq)
				}
			}
			if tc.preFunc != nil {
				tc.preFunc(t, env)
			}
			// Setup the routing rules as they would be after having previously done SwitchTraffic.
			env.updateTableRoutingRules(t, ctx, nil, []string{table1Name, table2Name, table3Name},
				tc.sourceKeyspace.KeyspaceName, tc.targetKeyspace.KeyspaceName, tc.targetKeyspace.KeyspaceName)
			got, err := env.ws.MoveTablesComplete(ctx, tc.req)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, got, tc.want, "Server.MoveTablesComplete() = %v, want %v", got, tc.want)
			}
			if tc.postFunc != nil {
				tc.postFunc(t, env)
			} else { // Default post checks
				// Confirm that we have no routing rules.
				rr, err := env.ts.GetRoutingRules(ctx)
				require.NoError(t, err)
				require.Zero(t, rr.Rules)

				// Confirm that we have no shard tablet controls, which is where
				// DeniedTables live.
				for _, keyspace := range []*testKeyspace{tc.sourceKeyspace, tc.targetKeyspace} {
					for _, shardName := range keyspace.ShardNames {
						checkDenyList(t, env.ts, keyspace.KeyspaceName, shardName, nil)
					}
				}
			}
		})
	}
}

func TestWorkflowDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	table1Name := "t1"
	table2Name := "t1_2"
	table3Name := "t1_3"
	tableTemplate := "CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))"
	sourceKeyspaceName := "sourceks"
	targetKeyspaceName := "targetks"
	lockName := fmt.Sprintf("%s/%s", targetKeyspaceName, workflowName)
	schema := map[string]*tabletmanagerdatapb.SchemaDefinition{
		table1Name: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   table1Name,
					Schema: fmt.Sprintf(tableTemplate, table1Name),
				},
			},
		},
		table2Name: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   table2Name,
					Schema: fmt.Sprintf(tableTemplate, table2Name),
				},
			},
		},
		table3Name: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   table3Name,
					Schema: fmt.Sprintf(tableTemplate, table3Name),
				},
			},
		},
	}

	testcases := []struct {
		name                            string
		sourceKeyspace, targetKeyspace  *testKeyspace
		preFunc                         func(t *testing.T, env *testEnv)
		req                             *vtctldatapb.WorkflowDeleteRequest
		expectedSourceQueries           []*queryResult
		expectedTargetQueries           []*queryResult
		readVReplicationWorkflowRequest *readVReplicationWorkflowRequestResponse
		want                            *vtctldatapb.WorkflowDeleteResponse
		wantErr                         string
		postFunc                        func(t *testing.T, env *testEnv)
		expectedLogs                    []string
	}{
		{
			name: "delete workflow",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowDeleteRequest{
				Keyspace: targetKeyspaceName,
				Workflow: workflowName,
			},
			expectedSourceQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						sourceKeyspaceName, ReverseWorkflowName(workflowName)),
					result: &querypb.QueryResult{},
				},
			},
			expectedTargetQueries: []*queryResult{
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, table1Name),
					result: &querypb.QueryResult{},
				},
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, table2Name),
					result: &querypb.QueryResult{},
				},
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, table3Name),
					result: &querypb.QueryResult{},
				},
			},
			want: &vtctldatapb.WorkflowDeleteResponse{
				Summary: fmt.Sprintf("Successfully cancelled the %s workflow in the %s keyspace",
					workflowName, targetKeyspaceName),
				Details: []*vtctldatapb.WorkflowDeleteResponse_TabletInfo{
					{
						Tablet:  &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID},
						Deleted: true,
					},
					{
						Tablet:  &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID + tabletUIDStep},
						Deleted: true,
					},
				},
			},
		},
		{
			name: "delete workflow with only reads switched",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowDeleteRequest{
				Keyspace: targetKeyspaceName,
				Workflow: workflowName,
			},
			expectedSourceQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						sourceKeyspaceName, ReverseWorkflowName(workflowName)),
					result: &querypb.QueryResult{},
				},
			},
			expectedTargetQueries: []*queryResult{
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, table1Name),
					result: &querypb.QueryResult{},
				},
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, table2Name),
					result: &querypb.QueryResult{},
				},
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, table3Name),
					result: &querypb.QueryResult{},
				},
			},
			preFunc: func(t *testing.T, env *testEnv) {
				// Setup the routing rules as they would be after having previously done SwitchTraffic
				// for replica and rdonly tablets.
				env.updateTableRoutingRules(t, ctx, roTabletTypes, []string{table1Name, table2Name, table3Name},
					sourceKeyspaceName, targetKeyspaceName, targetKeyspaceName)
			},
			want: &vtctldatapb.WorkflowDeleteResponse{
				Summary: fmt.Sprintf("Successfully cancelled the %s workflow in the %s keyspace",
					workflowName, targetKeyspaceName),
				Details: []*vtctldatapb.WorkflowDeleteResponse_TabletInfo{
					{
						Tablet:  &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID},
						Deleted: true,
					},
					{
						Tablet:  &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID + tabletUIDStep},
						Deleted: true,
					},
				},
			},
		},
		{
			name: "delete workflow with writes switched",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowDeleteRequest{
				Keyspace: targetKeyspaceName,
				Workflow: workflowName,
			},
			preFunc: func(t *testing.T, env *testEnv) {
				// Setup the routing rules as they would be after having previously
				// done SwitchTraffic with for all tablet types.
				env.updateTableRoutingRules(t, ctx, allTabletTypes, []string{table1Name, table2Name, table3Name},
					sourceKeyspaceName, targetKeyspaceName, targetKeyspaceName)
			},
			wantErr: ErrWorkflowDeleteWritesSwitched.Error(),
			postFunc: func(t *testing.T, env *testEnv) {
				// Clear out the routing rules we put in place.
				err := env.ts.SaveRoutingRules(ctx, &vschemapb.RoutingRules{})
				require.NoError(t, err)
			},
		},
		{
			name: "missing table",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowDeleteRequest{
				Keyspace: targetKeyspaceName,
				Workflow: workflowName,
			},
			expectedSourceQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						sourceKeyspaceName, ReverseWorkflowName(workflowName)),
					result: &querypb.QueryResult{},
				},
			},
			expectedTargetQueries: []*queryResult{
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, table1Name),
					result: &querypb.QueryResult{},
				},
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, table2Name),
					result: &querypb.QueryResult{},
					// We don't care that the cell and tablet info is off in the error message, only that
					// it contains the expected SQL error we'd encounter when attempting to drop a table
					// that doesn't exist. That will then cause this error to be non-fatal and the workflow
					// delete work will continue.
					err: fmt.Errorf("rpc error: code = Unknown desc = TabletManager.ExecuteFetchAsDba on cell-01: rpc error: code = Unknown desc = Unknown table 'vt_%s.%s' (errno 1051) (sqlstate 42S02) during query: drop table `vt_%s`.`%s`",
						targetKeyspaceName, table2Name, targetKeyspaceName, table2Name),
				},
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, table3Name),
					result: &querypb.QueryResult{},
				},
			},
			expectedLogs: []string{ // Confirm that the custom logger is working as expected
				fmt.Sprintf("Table `%s` did not exist when attempting to remove it", table2Name),
			},
			want: &vtctldatapb.WorkflowDeleteResponse{
				Summary: fmt.Sprintf("Successfully cancelled the %s workflow in the %s keyspace",
					workflowName, targetKeyspaceName),
				Details: []*vtctldatapb.WorkflowDeleteResponse_TabletInfo{
					{
						Tablet:  &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID},
						Deleted: true,
					},
					{
						Tablet:  &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID + tabletUIDStep},
						Deleted: true,
					},
				},
			},
		},
		{
			name: "multi-tenant workflow",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowDeleteRequest{
				Keyspace: targetKeyspaceName,
				Workflow: workflowName,
			},
			expectedSourceQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						sourceKeyspaceName, ReverseWorkflowName(workflowName)),
					result: &querypb.QueryResult{},
				},
			},
			readVReplicationWorkflowRequest: &readVReplicationWorkflowRequestResponse{
				req: &tabletmanagerdatapb.ReadVReplicationWorkflowRequest{
					Workflow: workflowName,
				},
				res: &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
					Workflow:     workflowName,
					WorkflowType: binlogdatapb.VReplicationWorkflowType_MoveTables,
					Options:      `{"tenant_id": "1"}`,
					Streams: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
						{
							Id: 1,
							Bls: &binlogdatapb.BinlogSource{
								Keyspace: sourceKeyspaceName,
								Shard:    "0",
								Filter: &binlogdatapb.Filter{
									Rules: []*binlogdatapb.Rule{
										{
											Match:  "t1",
											Filter: "select * from t1 where tenant_id = 1",
										},
									},
								},
							},
						},
					},
				},
			},
			preFunc: func(t *testing.T, env *testEnv) {
				err := env.ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
					Name: targetKeyspaceName,
					Keyspace: &vschemapb.Keyspace{
						Sharded: true,
						MultiTenantSpec: &vschemapb.MultiTenantSpec{
							TenantIdColumnName: "tenant_id",
							TenantIdColumnType: sqltypes.Int64,
						},
					},
				})
				require.NoError(t, err)
			},
			want: &vtctldatapb.WorkflowDeleteResponse{
				Summary: fmt.Sprintf("Successfully cancelled the %s workflow in the %s keyspace",
					workflowName, targetKeyspaceName),
				Details: []*vtctldatapb.WorkflowDeleteResponse_TabletInfo{
					{
						Tablet:  &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID},
						Deleted: true,
					},
					{
						Tablet:  &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID + tabletUIDStep},
						Deleted: true,
					},
				},
			},
		},
		{
			name: "multi-tenant workflow with keep-data",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowDeleteRequest{
				Keyspace: targetKeyspaceName,
				Workflow: workflowName,
				KeepData: true,
			},
			expectedSourceQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						sourceKeyspaceName, ReverseWorkflowName(workflowName)),
					result: &querypb.QueryResult{},
				},
			},
			want: &vtctldatapb.WorkflowDeleteResponse{
				Summary: fmt.Sprintf("Successfully cancelled the %s workflow in the %s keyspace",
					workflowName, targetKeyspaceName),
				Details: []*vtctldatapb.WorkflowDeleteResponse_TabletInfo{
					{
						Tablet:  &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID},
						Deleted: true,
					},
					{
						Tablet:  &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID + tabletUIDStep},
						Deleted: true,
					},
				},
			},
		},
		{
			name: "multi-tenant reshard",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowDeleteRequest{
				Keyspace: targetKeyspaceName,
				Workflow: workflowName,
			},
			expectedSourceQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						sourceKeyspaceName, ReverseWorkflowName(workflowName)),
					result: &querypb.QueryResult{},
				},
			},
			readVReplicationWorkflowRequest: &readVReplicationWorkflowRequestResponse{
				req: &tabletmanagerdatapb.ReadVReplicationWorkflowRequest{
					Workflow: workflowName,
				},
				res: &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
					Workflow:     workflowName,
					WorkflowType: binlogdatapb.VReplicationWorkflowType_Reshard,
					Options:      `{"tenant_id": "1"}`,
					Streams: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
						{
							Id: 1,
							Bls: &binlogdatapb.BinlogSource{
								Keyspace: sourceKeyspaceName,
								Shard:    "0",
								Filter: &binlogdatapb.Filter{
									Rules: []*binlogdatapb.Rule{
										{
											Match:  "t1",
											Filter: "select * from t1 where tenant_id = 1",
										},
									},
								},
							},
						},
					},
				},
			},
			preFunc: func(t *testing.T, env *testEnv) {
				err := env.ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
					Name: targetKeyspaceName,
					Keyspace: &vschemapb.Keyspace{
						Sharded: true,
						MultiTenantSpec: &vschemapb.MultiTenantSpec{
							TenantIdColumnName: "tenant_id",
							TenantIdColumnType: sqltypes.Int64,
						},
					},
				})
				require.NoError(t, err)
			},
			wantErr: "unsupported workflow type \"Reshard\" for multi-tenant migration",
		},
		{
			name: "multi-tenant workflow without predicate",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowDeleteRequest{
				Keyspace: targetKeyspaceName,
				Workflow: workflowName,
			},
			expectedSourceQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						sourceKeyspaceName, ReverseWorkflowName(workflowName)),
					result: &querypb.QueryResult{},
				},
			},
			readVReplicationWorkflowRequest: &readVReplicationWorkflowRequestResponse{
				req: &tabletmanagerdatapb.ReadVReplicationWorkflowRequest{
					Workflow: workflowName,
				},
				res: &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
					Workflow:     workflowName,
					WorkflowType: binlogdatapb.VReplicationWorkflowType_Reshard,
					Options:      `{"tenant_id": "1"}`,
					Streams: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
						{
							Id: 1,
							Bls: &binlogdatapb.BinlogSource{
								Keyspace: sourceKeyspaceName,
								Shard:    "0",
								Filter: &binlogdatapb.Filter{
									Rules: []*binlogdatapb.Rule{
										{
											Match:  "t1",
											Filter: "select * from t1 where tenant_id = 1",
										},
									},
								},
							},
						},
					},
				},
			},
			preFunc: func(t *testing.T, env *testEnv) {
				err := env.ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
					Name: targetKeyspaceName,
					Keyspace: &vschemapb.Keyspace{
						Sharded: true,
						MultiTenantSpec: &vschemapb.MultiTenantSpec{
							TenantIdColumnName: "tenant_id",
							TenantIdColumnType: sqltypes.Int64,
						},
					},
				})
				require.NoError(t, err)
			},
			wantErr: "unsupported workflow type \"Reshard\" for multi-tenant migration",
		},
		{
			name: "multi-tenant workflow without multi-tenant-spec in vschema",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowDeleteRequest{
				Keyspace: targetKeyspaceName,
				Workflow: workflowName,
			},
			expectedSourceQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						sourceKeyspaceName, ReverseWorkflowName(workflowName)),
					result: &querypb.QueryResult{},
				},
			},
			readVReplicationWorkflowRequest: &readVReplicationWorkflowRequestResponse{
				req: &tabletmanagerdatapb.ReadVReplicationWorkflowRequest{
					Workflow: workflowName,
				},
				res: &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
					Workflow:     workflowName,
					WorkflowType: binlogdatapb.VReplicationWorkflowType_MoveTables,
					Options:      `{"tenant_id": "1"}`,
					Streams: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
						{
							Id: 1,
							Bls: &binlogdatapb.BinlogSource{
								Keyspace: sourceKeyspaceName,
								Shard:    "0",
								Filter: &binlogdatapb.Filter{
									Rules: []*binlogdatapb.Rule{
										{
											Match:  "t1",
											Filter: "select * from t1 where tenant_id = 1",
										},
									},
								},
							},
						},
					},
				},
			},
			wantErr: "failed to fully delete all migrated data for tenant 1, please retry the operation: failed to build delete filter: target keyspace not defined, or it does not have multi-tenant spec",
		},
		{
			name: "missing denied table entries",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			preFunc: func(t *testing.T, env *testEnv) {
				lockCtx, targetUnlock, lockErr := env.ts.LockKeyspace(ctx, targetKeyspaceName, "test")
				require.NoError(t, lockErr)
				var err error
				defer require.NoError(t, err)
				defer targetUnlock(&err)
				for _, shard := range env.targetKeyspace.ShardNames {
					_, err := env.ts.UpdateShardFields(lockCtx, targetKeyspaceName, shard, func(si *topo.ShardInfo) error {
						// So t1_2 and t1_3 do not exist in the denied table list when we go
						// to remove t1, t1_2, and t1_3.
						err := si.UpdateDeniedTables(lockCtx, topodatapb.TabletType_PRIMARY, nil, false, []string{table1Name, "t2", "t3"})
						return err
					})
					require.NoError(t, err)
				}
			},
			req: &vtctldatapb.WorkflowDeleteRequest{
				Keyspace: targetKeyspaceName,
				Workflow: workflowName,
			},
			expectedSourceQueries: []*queryResult{
				{
					query: fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'",
						sourceKeyspaceName, ReverseWorkflowName(workflowName)),
					result: &querypb.QueryResult{},
				},
			},
			expectedTargetQueries: []*queryResult{
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, table1Name),
					result: &querypb.QueryResult{},
				},
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, table2Name),
					result: &querypb.QueryResult{},
				},
				{
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, table3Name),
					result: &querypb.QueryResult{},
				},
			},
			want: &vtctldatapb.WorkflowDeleteResponse{
				Summary: fmt.Sprintf("Successfully cancelled the %s workflow in the %s keyspace",
					workflowName, targetKeyspaceName),
				Details: []*vtctldatapb.WorkflowDeleteResponse_TabletInfo{
					{
						Tablet:  &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID},
						Deleted: true,
					},
					{
						Tablet:  &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID + tabletUIDStep},
						Deleted: true,
					},
				},
			},
			postFunc: func(t *testing.T, env *testEnv) {
				for _, shard := range env.targetKeyspace.ShardNames {
					si, err := env.ts.GetShard(ctx, targetKeyspaceName, shard)
					require.NoError(t, err)
					require.NotNil(t, si)
					tc := si.GetTabletControl(topodatapb.TabletType_PRIMARY)
					require.NotNil(t, tc)
					require.EqualValues(t, []string{"t2", "t3"}, tc.DeniedTables)
				}
			},
		},
		{
			name: "named lock held",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowDeleteRequest{
				Keyspace: targetKeyspaceName,
				Workflow: workflowName,
			},
			preFunc: func(t *testing.T, env *testEnv) {
				_, _, err := env.ts.LockName(ctx, lockName, "test")
				require.NoError(t, err)
				topo.LockTimeout = 500 * time.Millisecond
			},
			postFunc: func(t *testing.T, env *testEnv) {
				topo.LockTimeout = 45 * time.Second // reset it to the default
			},
			wantErr: fmt.Sprintf("failed to lock the %s workflow: deadline exceeded: internal/named_locks/%s", lockName, lockName),
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotNil(t, tc.sourceKeyspace)
			require.NotNil(t, tc.targetKeyspace)
			require.NotNil(t, tc.req)
			env := newTestEnv(t, ctx, defaultCellName, tc.sourceKeyspace, tc.targetKeyspace)
			defer env.close()
			memlogger := logutil.NewMemoryLogger()
			defer memlogger.Clear()
			env.ws.options.logger = memlogger
			env.tmc.schema = schema
			if tc.expectedSourceQueries != nil {
				require.NotNil(t, env.tablets[tc.sourceKeyspace.KeyspaceName])
				for _, eq := range tc.expectedSourceQueries {
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, eq)
				}
			}
			if tc.expectedTargetQueries != nil {
				require.NotNil(t, env.tablets[tc.targetKeyspace.KeyspaceName])
				for _, eq := range tc.expectedTargetQueries {
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, eq)
				}
			}
			if tc.readVReplicationWorkflowRequest != nil {
				targetTablets := env.tablets[tc.targetKeyspace.KeyspaceName]
				require.NotNil(t, targetTablets)
				for _, tablet := range targetTablets {
					env.tmc.expectReadVReplicationWorkflowRequest(tablet.Alias.Uid, tc.readVReplicationWorkflowRequest)
				}
			}
			if tc.preFunc != nil {
				tc.preFunc(t, env)
			}
			got, err := env.ws.WorkflowDelete(ctx, tc.req)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, got, tc.want, "Server.WorkflowDelete() = %v, want %v", got, tc.want)
			}
			if tc.postFunc != nil {
				tc.postFunc(t, env)
			} else { // Default post checks
				// Confirm that we have no routing rules.
				rr, err := env.ts.GetRoutingRules(ctx)
				require.NoError(t, err)
				require.Zero(t, rr.Rules)

				// Confirm that we have no shard tablet controls, which is where
				// DeniedTables live.
				for _, keyspace := range []*testKeyspace{tc.sourceKeyspace, tc.targetKeyspace} {
					for _, shardName := range keyspace.ShardNames {
						checkDenyList(t, env.ts, keyspace.KeyspaceName, shardName, nil)
					}
				}
			}
			logs := memlogger.String()
			// Confirm that the custom logger was passed on to the trafficSwitcher.
			for _, expectedLog := range tc.expectedLogs {
				require.Contains(t, logs, expectedLog)
			}
		})
	}
}

func TestMoveTablesTrafficSwitching(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"
	sourceKeyspaceName := "sourceks"
	targetKeyspaceName := "targetks"
	vrID := 1

	schema := map[string]*tabletmanagerdatapb.SchemaDefinition{
		tableName: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   tableName,
					Schema: fmt.Sprintf("CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))", tableName),
				},
			},
		},
	}

	copyTableQR := &queryResult{
		query: fmt.Sprintf("select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (%d) and id in (select max(id) from _vt.copy_state where vrepl_id in (%d) group by vrepl_id, table_name)",
			vrID, vrID),
		result: &querypb.QueryResult{},
	}
	journalQR := &queryResult{
		query:  "/select val from _vt.resharding_journal.*",
		result: &querypb.QueryResult{},
	}
	lockTableQR := &queryResult{
		query:  fmt.Sprintf("LOCK TABLES `%s` READ", tableName),
		result: &querypb.QueryResult{},
	}
	cutoverQR := &queryResult{
		query:  "/update _vt.vreplication set state='Stopped', message='stopped for cutover' where id=.*",
		result: &querypb.QueryResult{},
	}
	createWFQR := &queryResult{
		query:  "/insert into _vt.vreplication.*",
		result: &querypb.QueryResult{},
	}
	deleteWFQR := &queryResult{
		query:  fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'", targetKeyspaceName, workflowName),
		result: &querypb.QueryResult{},
	}
	deleteReverseWFQR := &queryResult{
		query:  fmt.Sprintf("delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'", sourceKeyspaceName, ReverseWorkflowName(workflowName)),
		result: &querypb.QueryResult{},
	}
	createReverseWFQR := &queryResult{
		query:  "/insert into _vt.vreplication.*_reverse.*",
		result: &querypb.QueryResult{},
	}
	createJournalQR := &queryResult{
		query:  "/insert into _vt.resharding_journal.*",
		result: &querypb.QueryResult{},
	}
	freezeWFQR := &queryResult{
		query:  fmt.Sprintf("update _vt.vreplication set message = 'FROZEN' where db_name='vt_%s' and workflow='%s'", targetKeyspaceName, workflowName),
		result: &querypb.QueryResult{},
	}
	freezeReverseWFQR := &queryResult{
		query:  fmt.Sprintf("update _vt.vreplication set message = 'FROZEN' where db_name='vt_%s' and workflow='%s'", sourceKeyspaceName, ReverseWorkflowName(workflowName)),
		result: &querypb.QueryResult{},
	}

	hasDeniedTableEntry := func(si *topo.ShardInfo) bool {
		if si == nil || len(si.TabletControls) == 0 {
			return false
		}
		for _, tc := range si.Shard.TabletControls {
			return slices.Equal(tc.DeniedTables, []string{tableName})
		}
		return false
	}

	testcases := []struct {
		name                           string
		sourceKeyspace, targetKeyspace *testKeyspace
		req                            *vtctldatapb.WorkflowSwitchTrafficRequest
		multiTenant                    bool
		preFunc                        func(env *testEnv)
		want                           *vtctldatapb.WorkflowSwitchTrafficResponse
		wantErr                        bool
	}{
		{
			name: "basic forward",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowSwitchTrafficRequest{
				Keyspace:    targetKeyspaceName,
				Workflow:    workflowName,
				Direction:   int32(DirectionForward),
				TabletTypes: allTabletTypes,
			},
			want: &vtctldatapb.WorkflowSwitchTrafficResponse{
				Summary:      fmt.Sprintf("SwitchTraffic was successful for workflow %s.%s", targetKeyspaceName, workflowName),
				StartState:   "Reads Not Switched. Writes Not Switched",
				CurrentState: "All Reads Switched. Writes Switched",
			},
		},
		{
			name: "basic backward",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowSwitchTrafficRequest{
				Keyspace:    targetKeyspaceName,
				Workflow:    workflowName,
				Direction:   int32(DirectionBackward),
				TabletTypes: allTabletTypes,
			},
			want: &vtctldatapb.WorkflowSwitchTrafficResponse{
				Summary:      fmt.Sprintf("ReverseTraffic was successful for workflow %s.%s", targetKeyspaceName, workflowName),
				StartState:   "All Reads Switched. Writes Switched",
				CurrentState: "Reads Not Switched. Writes Not Switched",
			},
		},
		{
			name: "backward for read-only tablets",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowSwitchTrafficRequest{
				Keyspace:    targetKeyspaceName,
				Workflow:    workflowName,
				Direction:   int32(DirectionBackward),
				TabletTypes: roTabletTypes,
			},
			want: &vtctldatapb.WorkflowSwitchTrafficResponse{
				Summary:      fmt.Sprintf("ReverseTraffic was successful for workflow %s.%s", targetKeyspaceName, workflowName),
				StartState:   "All Reads Switched. Writes Not Switched",
				CurrentState: "Reads Not Switched. Writes Not Switched",
			},
		},
		{
			name: "backward for multi-tenant workflow and read-only tablets",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowSwitchTrafficRequest{
				Keyspace:    targetKeyspaceName,
				Workflow:    workflowName,
				Direction:   int32(DirectionBackward),
				TabletTypes: roTabletTypes,
			},
			multiTenant: true,
			want: &vtctldatapb.WorkflowSwitchTrafficResponse{
				Summary:      fmt.Sprintf("ReverseTraffic was successful for workflow %s.%s", targetKeyspaceName, workflowName),
				StartState:   "All Reads Switched. Writes Not Switched",
				CurrentState: "Reads Not Switched. Writes Not Switched",
			},
		},
		{
			name: "backward for multi-tenant workflow for all tablet types",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			multiTenant: true,
			req: &vtctldatapb.WorkflowSwitchTrafficRequest{
				Keyspace:    targetKeyspaceName,
				Workflow:    workflowName,
				Direction:   int32(DirectionBackward),
				TabletTypes: allTabletTypes,
			},
			wantErr: true,
		},
		{
			name: "forward with tablet refresh error",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowSwitchTrafficRequest{
				Keyspace:    targetKeyspaceName,
				Workflow:    workflowName,
				Direction:   int32(DirectionForward),
				TabletTypes: allTabletTypes,
			},
			preFunc: func(env *testEnv) {
				env.tmc.SetRefreshStateError(env.tablets[sourceKeyspaceName][startingSourceTabletUID], errors.New("tablet refresh error"))
				env.tmc.SetRefreshStateError(env.tablets[targetKeyspaceName][startingTargetTabletUID], errors.New("tablet refresh error"))
			},
			wantErr: true,
		},
		{
			name: "forward with tablet refresh error and force",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowSwitchTrafficRequest{
				Keyspace:    targetKeyspaceName,
				Workflow:    workflowName,
				Direction:   int32(DirectionForward),
				TabletTypes: allTabletTypes,
				Force:       true,
			},
			preFunc: func(env *testEnv) {
				env.tmc.SetRefreshStateError(env.tablets[sourceKeyspaceName][startingSourceTabletUID], errors.New("tablet refresh error"))
				env.tmc.SetRefreshStateError(env.tablets[targetKeyspaceName][startingTargetTabletUID], errors.New("tablet refresh error"))
			},
			want: &vtctldatapb.WorkflowSwitchTrafficResponse{
				Summary:      fmt.Sprintf("SwitchTraffic was successful for workflow %s.%s", targetKeyspaceName, workflowName),
				StartState:   "Reads Not Switched. Writes Not Switched",
				CurrentState: "All Reads Switched. Writes Switched",
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotNil(t, tc.sourceKeyspace)
			require.NotNil(t, tc.targetKeyspace)
			require.NotNil(t, tc.req)
			env := newTestEnv(t, ctx, defaultCellName, tc.sourceKeyspace, tc.targetKeyspace)
			defer env.close()
			env.tmc.schema = schema

			if tc.req.Direction == int32(DirectionForward) {
				env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, copyTableQR)
				env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, cutoverQR)
				for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, journalQR)
				}
				for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, lockTableQR)
				}
				env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, deleteReverseWFQR)
				for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, createReverseWFQR)
				}
				env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, createJournalQR)
				env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, freezeWFQR)
			} else {
				env.tmc.reverse.Store(true)
				// Setup the routing rules as they would be after having previously done SwitchTraffic.
				env.updateTableRoutingRules(t, ctx, tc.req.TabletTypes, []string{tableName},
					tc.sourceKeyspace.KeyspaceName, tc.targetKeyspace.KeyspaceName, tc.targetKeyspace.KeyspaceName)
				if !slices.Contains(tc.req.TabletTypes, topodatapb.TabletType_PRIMARY) {
					for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
						env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, journalQR)
					}
				} else {
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, copyTableQR)
					for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
						env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, journalQR)
					}
					for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
						env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, lockTableQR)
					}
					for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
						env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, cutoverQR)
						env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, deleteWFQR)
						env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, createWFQR)
						env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, createJournalQR)
					}
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, freezeReverseWFQR)
				}
			}

			if tc.preFunc != nil {
				tc.preFunc(env)
			}

			if tc.multiTenant {
				rwr := &readVReplicationWorkflowRequestResponse{
					req: &tabletmanagerdatapb.ReadVReplicationWorkflowRequest{
						Workflow: workflowName,
					},
					res: &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
						Options: `{"tenant_id": "1"}`, // This is all we need for it to be considered a multi-tenant workflow
						Streams: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
							{
								Id: 1,
								Bls: &binlogdatapb.BinlogSource{
									Keyspace: sourceKeyspaceName,
									Shard:    "0",
									Filter: &binlogdatapb.Filter{
										Rules: []*binlogdatapb.Rule{
											{
												Match:  "t1",
												Filter: "select * from t1",
											},
										},
									},
								},
							},
						},
					},
				}
				env.tmc.expectReadVReplicationWorkflowRequestOnTargetTablets(rwr)
				// Multi-tenant workflows also use keyspace routing rules. So we set those
				// up as if we've already switched the traffic.
				if tc.req.Direction == int32(DirectionBackward) {
					err := changeKeyspaceRouting(ctx, env.ts, tc.req.TabletTypes, tc.sourceKeyspace.KeyspaceName,
						tc.targetKeyspace.KeyspaceName, "SwitchTraffic")
					require.NoError(t, err)
				}
			}

			got, err := env.ws.WorkflowSwitchTraffic(ctx, tc.req)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want.String(), got.String(), "Server.WorkflowSwitchTraffic() = %v, want %v", got, tc.want)

			if tc.multiTenant { // Confirm the keyspace routing rules
				gotKrrs, err := env.ts.GetKeyspaceRoutingRules(ctx)
				require.NoError(t, err)
				sort.Slice(gotKrrs.Rules, func(i, j int) bool {
					return gotKrrs.Rules[i].FromKeyspace < gotKrrs.Rules[j].FromKeyspace
				})
				expectedKrrs := &vschemapb.KeyspaceRoutingRules{}
				for _, tabletType := range tc.req.TabletTypes {
					suffix := ""
					if tabletType != topodatapb.TabletType_PRIMARY {
						suffix = "@" + strings.ToLower(tabletType.String())
					}
					toKs, fromKs := tc.sourceKeyspace.KeyspaceName, tc.targetKeyspace.KeyspaceName
					if tc.req.Direction == int32(DirectionBackward) {
						fromKs, toKs = toKs, fromKs
					}
					expectedKrrs.Rules = append(expectedKrrs.Rules, &vschemapb.KeyspaceRoutingRule{
						FromKeyspace: fromKs + suffix,
						ToKeyspace:   toKs,
					})
				}
				sort.Slice(expectedKrrs.Rules, func(i, j int) bool {
					return expectedKrrs.Rules[i].FromKeyspace < expectedKrrs.Rules[j].FromKeyspace
				})
				require.Equal(t, expectedKrrs.String(), gotKrrs.String())
			} else { // Confirm the [table] routing rules
				rr, err := env.ts.GetRoutingRules(ctx)
				require.NoError(t, err)
				for _, rr := range rr.Rules {
					_, rrTabletType, found := strings.Cut(rr.FromTable, "@")
					if !found { // No @<tablet_type> is primary
						rrTabletType = topodatapb.TabletType_PRIMARY.String()
					}
					tabletType, err := topoproto.ParseTabletType(rrTabletType)
					require.NoError(t, err)

					var to string
					if slices.Contains(tc.req.TabletTypes, tabletType) {
						to = fmt.Sprintf("%s.%s", tc.targetKeyspace.KeyspaceName, tableName)
						if tc.req.Direction == int32(DirectionBackward) {
							to = fmt.Sprintf("%s.%s", tc.sourceKeyspace.KeyspaceName, tableName)
						}
					} else {
						to = fmt.Sprintf("%s.%s", tc.sourceKeyspace.KeyspaceName, tableName)
						if tc.req.Direction == int32(DirectionBackward) {
							to = fmt.Sprintf("%s.%s", tc.targetKeyspace.KeyspaceName, tableName)
						}
					}
					for _, tt := range rr.ToTables {
						require.Equal(t, to, tt, "Additional info: tablet type: %s, rr.FromTable: %s, rr.ToTables: %v, to string: %s",
							tabletType.String(), rr.FromTable, rr.ToTables, to)
					}
				}
			}

			// Confirm that we have the expected denied tables entries.
			if slices.Contains(tc.req.TabletTypes, topodatapb.TabletType_PRIMARY) {
				for _, keyspace := range []*testKeyspace{tc.sourceKeyspace, tc.targetKeyspace} {
					for _, shardName := range keyspace.ShardNames {
						si, err := env.ts.GetShard(ctx, keyspace.KeyspaceName, shardName)
						require.NoError(t, err)
						switch {
						case keyspace == tc.sourceKeyspace && tc.req.Direction == int32(DirectionForward):
							require.True(t, hasDeniedTableEntry(si))
						case keyspace == tc.sourceKeyspace && tc.req.Direction == int32(DirectionBackward):
							require.False(t, hasDeniedTableEntry(si))
						case keyspace == tc.targetKeyspace && tc.req.Direction == int32(DirectionForward):
							require.False(t, hasDeniedTableEntry(si))
						case keyspace == tc.targetKeyspace && tc.req.Direction == int32(DirectionBackward):
							require.True(t, hasDeniedTableEntry(si))
						}
					}
				}
			}
		})
	}
}

func TestMoveTablesTrafficSwitchingDryRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	table1Name := "t1"
	table2Name := "a1"
	tables := []string{table1Name, table2Name}
	sort.Strings(tables)
	tablesStr := strings.Join(tables, ",")
	sourceKeyspaceName := "sourceks"
	targetKeyspaceName := "targetks"
	vrID := 1
	schema := map[string]*tabletmanagerdatapb.SchemaDefinition{
		table1Name: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   table1Name,
					Schema: fmt.Sprintf("CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))", table1Name),
				},
			},
		},
		table2Name: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   table2Name,
					Schema: fmt.Sprintf("CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))", table2Name),
				},
			},
		},
	}
	copyTableQR := &queryResult{
		query: fmt.Sprintf("select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (%d) and id in (select max(id) from _vt.copy_state where vrepl_id in (%d) group by vrepl_id, table_name)",
			vrID, vrID),
		result: &querypb.QueryResult{},
	}
	journalQR := &queryResult{
		query:  "/select val from _vt.resharding_journal.*",
		result: &querypb.QueryResult{},
	}
	lockTableQR := &queryResult{
		query:  fmt.Sprintf("LOCK TABLES `%s` READ,`%s` READ", table2Name, table1Name),
		result: &querypb.QueryResult{},
	}

	testcases := []struct {
		name                           string
		sourceKeyspace, targetKeyspace *testKeyspace
		req                            *vtctldatapb.WorkflowSwitchTrafficRequest
		want                           []string
	}{
		{
			name: "basic forward",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowSwitchTrafficRequest{
				Keyspace:    targetKeyspaceName,
				Workflow:    workflowName,
				Direction:   int32(DirectionForward),
				TabletTypes: allTabletTypes,
				DryRun:      true,
			},
			want: []string{
				"Lock keyspace " + sourceKeyspaceName,
				fmt.Sprintf("Mirroring 0.00 percent of traffic from keyspace %s to keyspace %s for tablet types [REPLICA,RDONLY]", sourceKeyspaceName, targetKeyspaceName),
				fmt.Sprintf("Switch reads for tables [%s] to keyspace %s for tablet types [REPLICA,RDONLY]", tablesStr, targetKeyspaceName),
				fmt.Sprintf("Routing rules for tables [%s] will be updated", tablesStr),
				"Unlock keyspace " + sourceKeyspaceName,
				"Lock keyspace " + sourceKeyspaceName,
				"Lock keyspace " + targetKeyspaceName,
				fmt.Sprintf("Mirroring 0.00 percent of traffic from keyspace %s to keyspace %s for tablet types [PRIMARY]", sourceKeyspaceName, targetKeyspaceName),
				fmt.Sprintf("Stop writes on keyspace %s for tables [%s]: [keyspace:%s;shard:-80;position:%s,keyspace:%s;shard:80-;position:%s]",
					sourceKeyspaceName, tablesStr, sourceKeyspaceName, position, sourceKeyspaceName, position),
				"Wait for vreplication on stopped streams to catchup for up to 30s",
				"Create reverse vreplication workflow " + ReverseWorkflowName(workflowName),
				"Create journal entries on source databases",
				fmt.Sprintf("Enable writes on keyspace %s for tables [%s]", targetKeyspaceName, tablesStr),
				fmt.Sprintf("Switch routing from keyspace %s to keyspace %s", sourceKeyspaceName, targetKeyspaceName),
				fmt.Sprintf("Routing rules for tables [%s] will be updated", tablesStr),
				fmt.Sprintf("Switch writes completed, freeze and delete vreplication streams on: [tablet:%d,tablet:%d]", startingTargetTabletUID, startingTargetTabletUID+tabletUIDStep),
				fmt.Sprintf("Mark vreplication streams frozen on: [keyspace:%s;shard:-80;tablet:%d;workflow:%s;dbname:vt_%s,keyspace:%s;shard:80-;tablet:%d;workflow:%s;dbname:vt_%s]",
					targetKeyspaceName, startingTargetTabletUID, workflowName, targetKeyspaceName, targetKeyspaceName, startingTargetTabletUID+tabletUIDStep, workflowName, targetKeyspaceName),
				"Unlock keyspace " + targetKeyspaceName,
				"Unlock keyspace " + sourceKeyspaceName,
			},
		},
		{
			name: "basic backward",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowSwitchTrafficRequest{
				Keyspace:    targetKeyspaceName,
				Workflow:    workflowName,
				Direction:   int32(DirectionBackward),
				TabletTypes: allTabletTypes,
				DryRun:      true,
			},
			want: []string{
				"Lock keyspace " + targetKeyspaceName,
				fmt.Sprintf("Mirroring 0.00 percent of traffic from keyspace %s to keyspace %s for tablet types [REPLICA,RDONLY]", targetKeyspaceName, sourceKeyspaceName),
				fmt.Sprintf("Switch reads for tables [%s] to keyspace %s for tablet types [REPLICA,RDONLY]", tablesStr, sourceKeyspaceName),
				fmt.Sprintf("Routing rules for tables [%s] will be updated", tablesStr),
				"Unlock keyspace " + targetKeyspaceName,
				"Lock keyspace " + targetKeyspaceName,
				"Lock keyspace " + sourceKeyspaceName,
				fmt.Sprintf("Mirroring 0.00 percent of traffic from keyspace %s to keyspace %s for tablet types [PRIMARY]", targetKeyspaceName, sourceKeyspaceName),
				fmt.Sprintf("Stop writes on keyspace %s for tables [%s]: [keyspace:%s;shard:-80;position:%s,keyspace:%s;shard:80-;position:%s]",
					targetKeyspaceName, tablesStr, targetKeyspaceName, position, targetKeyspaceName, position),
				"Wait for vreplication on stopped streams to catchup for up to 30s",
				"Create reverse vreplication workflow " + workflowName,
				"Create journal entries on source databases",
				fmt.Sprintf("Enable writes on keyspace %s for tables [%s]", sourceKeyspaceName, tablesStr),
				fmt.Sprintf("Switch routing from keyspace %s to keyspace %s", targetKeyspaceName, sourceKeyspaceName),
				fmt.Sprintf("Routing rules for tables [%s] will be updated", tablesStr),
				fmt.Sprintf("Switch writes completed, freeze and delete vreplication streams on: [tablet:%d,tablet:%d]", startingSourceTabletUID, startingSourceTabletUID+tabletUIDStep),
				fmt.Sprintf("Mark vreplication streams frozen on: [keyspace:%s;shard:-80;tablet:%d;workflow:%s;dbname:vt_%s,keyspace:%s;shard:80-;tablet:%d;workflow:%s;dbname:vt_%s]",
					sourceKeyspaceName, startingSourceTabletUID, ReverseWorkflowName(workflowName), sourceKeyspaceName, sourceKeyspaceName, startingSourceTabletUID+tabletUIDStep, ReverseWorkflowName(workflowName), sourceKeyspaceName),
				"Unlock keyspace " + sourceKeyspaceName,
				"Unlock keyspace " + targetKeyspaceName,
			},
		},
		{
			name: "backward for read-only tablets",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			req: &vtctldatapb.WorkflowSwitchTrafficRequest{
				Keyspace:    targetKeyspaceName,
				Workflow:    workflowName,
				Direction:   int32(DirectionBackward),
				TabletTypes: roTabletTypes,
				DryRun:      true,
			},
			want: []string{
				"Lock keyspace " + sourceKeyspaceName,
				fmt.Sprintf("Mirroring 0.00 percent of traffic from keyspace %s to keyspace %s for tablet types [REPLICA,RDONLY]", sourceKeyspaceName, targetKeyspaceName),
				fmt.Sprintf("Switch reads for tables [%s] to keyspace %s for tablet types [REPLICA,RDONLY]", tablesStr, sourceKeyspaceName),
				fmt.Sprintf("Routing rules for tables [%s] will be updated", tablesStr),
				fmt.Sprintf("Serving VSchema will be rebuilt for the %s keyspace", sourceKeyspaceName),
				"Unlock keyspace " + sourceKeyspaceName,
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotNil(t, tc.sourceKeyspace)
			require.NotNil(t, tc.targetKeyspace)
			require.NotNil(t, tc.req)
			env := newTestEnv(t, ctx, defaultCellName, tc.sourceKeyspace, tc.targetKeyspace)
			defer env.close()
			env.tmc.schema = schema
			if tc.req.Direction == int32(DirectionForward) {
				env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, copyTableQR)
				for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, journalQR)
				}
				for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, lockTableQR)
				}
			} else {
				env.tmc.reverse.Store(true)
				// Setup the routing rules as they would be after having previously done SwitchTraffic.
				env.updateTableRoutingRules(t, ctx, tc.req.TabletTypes, tables,
					tc.sourceKeyspace.KeyspaceName, tc.targetKeyspace.KeyspaceName, tc.targetKeyspace.KeyspaceName)
				if !slices.Contains(tc.req.TabletTypes, topodatapb.TabletType_PRIMARY) {
					for i := 0; i < len(tc.sourceKeyspace.ShardNames); i++ { // Per stream
						env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, journalQR)
					}
				} else {
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, copyTableQR)
					for i := 0; i < len(tc.sourceKeyspace.ShardNames); i++ { // Per stream
						env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, journalQR)
					}
					for i := 0; i < len(tc.sourceKeyspace.ShardNames); i++ { // Per stream
						env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, lockTableQR)
					}
				}
			}
			got, err := env.ws.WorkflowSwitchTraffic(ctx, tc.req)
			require.NoError(t, err)

			require.EqualValues(t, tc.want, got.DryRunResults, "Server.WorkflowSwitchTraffic(DryRun:true) = %v, want %v", got.DryRunResults, tc.want)
		})
	}
}

func TestMirrorTraffic(t *testing.T) {
	ctx := context.Background()
	sourceKs := "source"
	sourceShards := []string{"-"}
	targetKs := "target"
	targetShards := []string{"-80", "80-"}
	table1 := "table1"
	table2 := "table2"
	workflow := "src2target"

	mirrorRules := map[string]map[string]float32{}
	routingRules := map[string][]string{
		fmt.Sprintf("%s.%s@rdonly", sourceKs, table1): {fmt.Sprintf("%s.%s", targetKs, table1)},
		fmt.Sprintf("%s.%s@rdonly", sourceKs, table2): {fmt.Sprintf("%s.%s", targetKs, table2)},
	}

	tabletTypes := []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}

	initialRoutingRules := map[string][]string{
		fmt.Sprintf("%s.%s", sourceKs, table1):         {fmt.Sprintf("%s.%s", sourceKs, table1)},
		fmt.Sprintf("%s.%s", sourceKs, table2):         {fmt.Sprintf("%s.%s", sourceKs, table2)},
		fmt.Sprintf("%s.%s@replica", sourceKs, table1): {fmt.Sprintf("%s.%s@replica", sourceKs, table1)},
		fmt.Sprintf("%s.%s@replica", sourceKs, table2): {fmt.Sprintf("%s.%s@replica", sourceKs, table2)},
		fmt.Sprintf("%s.%s@rdonly", sourceKs, table1):  {fmt.Sprintf("%s.%s@rdonly", sourceKs, table1)},
		fmt.Sprintf("%s.%s@rdonly", sourceKs, table2):  {fmt.Sprintf("%s.%s@rdonly", sourceKs, table2)},
	}

	tests := []struct {
		name string

		req            *vtctldatapb.WorkflowMirrorTrafficRequest
		mirrorRules    map[string]map[string]float32
		routingRules   map[string][]string
		setup          func(*testing.T, context.Context, *testMaterializerEnv)
		sourceKeyspace string
		sourceShards   []string
		targetKeyspace string
		targetShards   []string

		wantErr         string
		wantMirrorRules map[string]map[string]float32
	}{
		{
			name: "no such keyspace",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    "no_ks",
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			wantErr:         "FindAllShardsInKeyspace(no_ks): List: node doesn't exist: keyspaces/no_ks/shards",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "no such workflow",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    "no_workflow",
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			setup: func(t *testing.T, ctx context.Context, te *testMaterializerEnv) {
				te.tmc.readVReplicationWorkflow = func(
					ctx context.Context,
					tablet *topodatapb.Tablet,
					request *tabletmanagerdatapb.ReadVReplicationWorkflowRequest,
				) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
					return nil, nil
				}
			},
			wantErr:         "no streams found in keyspace target for no_workflow",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "cannot mirror traffic for migrate workflows",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    "migrate",
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			setup: func(t *testing.T, ctx context.Context, te *testMaterializerEnv) {
				te.tmc.readVReplicationWorkflow = createReadVReplicationWorkflowFunc(t, binlogdatapb.VReplicationWorkflowType_Migrate, nil, te.tmc.keyspace, sourceShards, []string{table1, table2})
			},
			wantErr:         "invalid action for Migrate workflow: MirrorTraffic",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "cannot mirror traffic for reshard workflows",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    sourceKs,
				Workflow:    "reshard",
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			sourceKeyspace: sourceKs,
			sourceShards:   []string{"-80", "80-"},
			targetKeyspace: sourceKs,
			targetShards:   []string{"-55", "55-aa", "55-"},
			setup: func(t *testing.T, ctx context.Context, te *testMaterializerEnv) {
				te.tmc.readVReplicationWorkflow = createReadVReplicationWorkflowFunc(t, binlogdatapb.VReplicationWorkflowType_Reshard, nil, sourceKs, []string{"-80", "80-"}, []string{table1, table2})
			},
			wantErr:         "invalid action for Reshard workflow: MirrorTraffic",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "cannot mirror rdonly traffic after switch rdonly traffic",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			routingRules: map[string][]string{
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table1): {fmt.Sprintf("%s.%s@rdonly", targetKs, table1)},
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table2): {fmt.Sprintf("%s.%s@rdonly", targetKs, table2)},
			},
			wantErr:         "cannot mirror [rdonly] traffic for workflow src2target at this time: traffic for those tablet types is switched",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "cannot mirror replica traffic after switch replica traffic",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			routingRules: map[string][]string{
				fmt.Sprintf("%s.%s@replica", sourceKs, table1): {fmt.Sprintf("%s.%s@replica", targetKs, table1)},
				fmt.Sprintf("%s.%s@replica", sourceKs, table2): {fmt.Sprintf("%s.%s@replica", targetKs, table2)},
			},
			wantErr:         "cannot mirror [replica] traffic for workflow src2target at this time: traffic for those tablet types is switched",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "cannot mirror write traffic after switch traffic",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			routingRules: map[string][]string{
				fmt.Sprintf("%s.%s", sourceKs, table1): {fmt.Sprintf("%s.%s", targetKs, table1)},
				fmt.Sprintf("%s.%s", sourceKs, table2): {fmt.Sprintf("%s.%s", targetKs, table2)},
			},
			wantErr:         "cannot mirror [primary] traffic for workflow src2target at this time: traffic for those tablet types is switched",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "does not mirror traffic for partial move tables",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			setup: func(t *testing.T, ctx context.Context, te *testMaterializerEnv) {
				te.tmc.readVReplicationWorkflow = func(
					ctx context.Context,
					tablet *topodatapb.Tablet,
					request *tabletmanagerdatapb.ReadVReplicationWorkflowRequest,
				) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
					if tablet.Shard != "-80" {
						return nil, nil
					}
					return &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
						Workflow:     request.Workflow,
						WorkflowType: binlogdatapb.VReplicationWorkflowType_MoveTables,
						Streams: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
							{
								Id: 1,
								Bls: &binlogdatapb.BinlogSource{
									Keyspace: sourceKs,
									Shard:    "-80",
									Filter: &binlogdatapb.Filter{
										Rules: []*binlogdatapb.Rule{
											{Match: table1},
											{Match: table2},
										},
									},
								},
							},
						},
					}, nil
				}
			},
			sourceShards:    []string{"-80", "80-"},
			targetShards:    []string{"-80", "80-"},
			wantErr:         "invalid action for partial migration: MirrorTraffic",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "does not mirror traffic for multi-tenant move tables",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			setup: func(t *testing.T, ctx context.Context, te *testMaterializerEnv) {
				te.tmc.readVReplicationWorkflow = createReadVReplicationWorkflowFunc(t, binlogdatapb.VReplicationWorkflowType_MoveTables, &vtctldatapb.WorkflowOptions{TenantId: "123"}, te.tmc.keyspace, sourceShards, []string{table1, table2})
			},
			wantErr:         "invalid action for multi-tenant migration: MirrorTraffic",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "does not mirror traffic for reverse move tables",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow + "_reverse",
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			wantErr:         "invalid action for reverse workflow: MirrorTraffic",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "ok",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			routingRules: initialRoutingRules,
			wantMirrorRules: map[string]map[string]float32{
				fmt.Sprintf("%s.%s", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 50.0,
				},
				fmt.Sprintf("%s.%s@replica", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 50.0,
				},
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 50.0,
				},
				fmt.Sprintf("%s.%s", sourceKs, table2): {
					fmt.Sprintf("%s.%s", targetKs, table2): 50.0,
				},
				fmt.Sprintf("%s.%s@replica", sourceKs, table2): {
					fmt.Sprintf("%s.%s", targetKs, table2): 50.0,
				},
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table2): {
					fmt.Sprintf("%s.%s", targetKs, table2): 50.0,
				},
			},
		},
		{
			name: "does not overwrite unrelated mirror rules",
			mirrorRules: map[string]map[string]float32{
				"other_source.table2": {
					targetKs + ".table2": 25.0,
				},
			},
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			routingRules: initialRoutingRules,
			wantMirrorRules: map[string]map[string]float32{
				fmt.Sprintf("%s.%s", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 50.0,
				},
				fmt.Sprintf("%s.%s@replica", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 50.0,
				},
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 50.0,
				},
				fmt.Sprintf("%s.%s", sourceKs, table2): {
					fmt.Sprintf("%s.%s", targetKs, table2): 50.0,
				},
				fmt.Sprintf("%s.%s@replica", sourceKs, table2): {
					fmt.Sprintf("%s.%s", targetKs, table2): 50.0,
				},
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table2): {
					fmt.Sprintf("%s.%s", targetKs, table2): 50.0,
				},
				"other_source.table2": {
					targetKs + ".table2": 25.0,
				},
			},
		},
		{
			name: "does not overwrite when some but not all mirror rules already exist",
			mirrorRules: map[string]map[string]float32{
				fmt.Sprintf("%s.%s", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 25.0,
				},
				fmt.Sprintf("%s.%s@replica", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 25.0,
				},
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 25.0,
				},
			},
			routingRules: initialRoutingRules,
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			wantErr: "wrong number of pre-existing mirror rules",
			wantMirrorRules: map[string]map[string]float32{
				fmt.Sprintf("%s.%s", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 25.0,
				},
				fmt.Sprintf("%s.%s@replica", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 25.0,
				},
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 25.0,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mirrorRules == nil {
				tt.mirrorRules = mirrorRules
			}
			if tt.routingRules == nil {
				tt.routingRules = routingRules
			}
			if tt.sourceKeyspace == "" {
				tt.sourceKeyspace = sourceKs
			}
			if tt.sourceShards == nil {
				tt.sourceShards = sourceShards
			}
			if tt.targetKeyspace == "" {
				tt.targetKeyspace = targetKs
			}
			if tt.targetShards == nil {
				tt.targetShards = targetShards
			}

			te := newTestMaterializerEnv(t, ctx, &vtctldatapb.MaterializeSettings{
				SourceKeyspace: tt.sourceKeyspace,
				TargetKeyspace: tt.targetKeyspace,
				Workflow:       workflow,
				TableSettings: []*vtctldatapb.TableMaterializeSettings{
					{
						TargetTable:      table1,
						SourceExpression: "select * from " + table1,
					},
					{
						TargetTable:      table2,
						SourceExpression: "select * from " + table2,
					},
				},
			}, tt.sourceShards, tt.targetShards)

			require.NoError(t, topotools.SaveMirrorRules(ctx, te.topoServ, tt.mirrorRules))
			require.NoError(t, topotools.SaveRoutingRules(ctx, te.topoServ, tt.routingRules))
			require.NoError(t, te.topoServ.RebuildSrvVSchema(ctx, []string{te.cell}))

			if tt.setup != nil {
				tt.setup(t, ctx, te)
			}

			got, err := te.ws.WorkflowMirrorTraffic(ctx, tt.req)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.NotNil(t, got)
			}
			mr, err := topotools.GetMirrorRules(ctx, te.topoServ)
			require.NoError(t, err)
			wantMirrorRules := tt.mirrorRules
			if tt.wantMirrorRules != nil {
				wantMirrorRules = tt.wantMirrorRules
			}
			require.Equal(t, wantMirrorRules, mr)
		})
	}
}

func createReadVReplicationWorkflowFunc(t *testing.T, workflowType binlogdatapb.VReplicationWorkflowType, workflowOptions *vtctldatapb.WorkflowOptions, sourceKeyspace string, sourceShards []string, sourceTables []string) readVReplicationWorkflowFunc {
	return func(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
		streams := make([]*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream, 0)
		for i, shard := range sourceShards {
			if shard == tablet.Shard {
				return nil, nil
			}
			rules := make([]*binlogdatapb.Rule, len(sourceTables))
			for i, table := range sourceTables {
				rules[i] = &binlogdatapb.Rule{Match: table}
			}
			streams = append(streams, &tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
				Id: int32(i + 1),
				Bls: &binlogdatapb.BinlogSource{
					Keyspace: sourceKeyspace,
					Shard:    shard,
					Filter:   &binlogdatapb.Filter{Rules: rules},
				},
			})
		}

		var err error
		var options []byte
		if workflowOptions != nil {
			options, err = json.Marshal(workflowOptions)
			require.NoError(t, err)
		}

		return &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
			Workflow:     request.Workflow,
			Options:      string(options),
			WorkflowType: workflowType,
			Streams:      streams,
		}, nil
	}
}

// Test checks that we don't include logs from non-existent streams in the result.
// Ensures that we just skip the logs from non-existent streams and include the rest.
func TestGetWorkflowsStreamLogs(t *testing.T) {
	ctx := context.Background()

	sourceKeyspace := "source_keyspace"
	targetKeyspace := "target_keyspace"
	workflow := "test_workflow"

	sourceShards := []string{"-"}
	targetShards := []string{"-"}

	te := newTestMaterializerEnv(t, ctx, &vtctldatapb.MaterializeSettings{
		SourceKeyspace: sourceKeyspace,
		TargetKeyspace: targetKeyspace,
		Workflow:       workflow,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{
			{
				TargetTable:      "table1",
				SourceExpression: "select * from " + "table1",
			},
			{
				TargetTable:      "table2",
				SourceExpression: "select * from " + "table2",
			},
		},
	}, sourceShards, targetShards)

	logResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|vrepl_id|type|state|message|created_at|updated_at|count", "int64|int64|varchar|varchar|varchar|varchar|varchar|int64"),
		"1|0|State Change|Running|test message for non-existent 1|2006-01-02 15:04:05|2006-01-02 15:04:05|1",
		"2|0|State Change|Stopped|test message for non-existent 2|2006-01-02 15:04:06|2006-01-02 15:04:06|1",
		"3|1|State Change|Running|log message|2006-01-02 15:04:07|2006-01-02 15:04:07|1",
	)

	te.tmc.expectVRQuery(200, "select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (1) and id in (select max(id) from _vt.copy_state where vrepl_id in (1) group by vrepl_id, table_name)", &sqltypes.Result{})
	te.tmc.expectVRQuery(200, "select id from _vt.vreplication where db_name = 'vt_target_keyspace' and workflow = 'test_workflow'", &sqltypes.Result{})
	te.tmc.expectVRQuery(200, "select id, vrepl_id, type, state, message, created_at, updated_at, `count` from _vt.vreplication_log where vrepl_id in (1) order by vrepl_id asc, id asc", logResult)

	res, err := te.ws.GetWorkflows(ctx, &vtctldatapb.GetWorkflowsRequest{
		Keyspace:    targetKeyspace,
		Workflow:    workflow,
		IncludeLogs: true,
	})
	require.NoError(t, err)

	assert.Len(t, res.Workflows, 1)
	assert.NotNil(t, res.Workflows[0].ShardStreams["-/cell-0000000200"])
	assert.Len(t, res.Workflows[0].ShardStreams["-/cell-0000000200"].Streams, 1)

	gotLogs := res.Workflows[0].ShardStreams["-/cell-0000000200"].Streams[0].Logs

	// The non-existent stream logs shouldn't be part of the result
	assert.Len(t, gotLogs, 1)
	assert.Equal(t, gotLogs[0].Message, "log message")
	assert.Equal(t, gotLogs[0].State, "Running")
	assert.Equal(t, gotLogs[0].Id, int64(3))
}

func TestWorkflowStatus(t *testing.T) {
	ctx := context.Background()

	sourceKeyspace := "source_keyspace"
	targetKeyspace := "target_keyspace"
	workflow := "test_workflow"

	sourceShards := []string{"-"}
	targetShards := []string{"-"}

	te := newTestMaterializerEnv(t, ctx, &vtctldatapb.MaterializeSettings{
		SourceKeyspace: sourceKeyspace,
		TargetKeyspace: targetKeyspace,
		Workflow:       workflow,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{
			{
				TargetTable:      "table1", // Already finished copying
				SourceExpression: "select * from " + "table1",
			},
			{
				TargetTable:      "table2", // In progress
				SourceExpression: "select * from " + "table2",
			},
			{
				TargetTable:      "table3", // Not started
				SourceExpression: "select * from " + "table3",
			},
		},
	}, sourceShards, targetShards)

	// We don't specify table1 here as we finished copying it already.
	// table1 should still be returned in the copy status though.
	tablesResult := sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name", "varchar"), "table2", "table3")
	te.tmc.expectVRQuery(200, "select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1", tablesResult)

	tablesTargetCopyResult := sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|table_rows|data_length", "varchar|int64|int64"), "table1|100|1000", "table2|100|250", "table3|0|16")
	te.tmc.expectVRQuery(200, "select table_name, table_rows, data_length from information_schema.tables where table_schema = 'vt_target_keyspace' and table_name in ('table1','table2','table3')", tablesTargetCopyResult)

	tablesSourceCopyResult := sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|table_rows|data_length", "varchar|int64|int64"), "table1|100|1000", "table2|200|500", "table3|5000|32000")
	te.tmc.expectVRQuery(100, "select table_name, table_rows, data_length from information_schema.tables where table_schema = 'vt_source_keyspace' and table_name in ('table1','table2','table3')", tablesSourceCopyResult)

	te.tmc.expectVRQuery(200, "select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (1) and id in (select max(id) from _vt.copy_state where vrepl_id in (1) group by vrepl_id, table_name)", &sqltypes.Result{})

	res, err := te.ws.WorkflowStatus(ctx, &vtctldatapb.WorkflowStatusRequest{
		Keyspace: targetKeyspace,
		Workflow: workflow,
		Shards:   targetShards,
	})

	assert.NoError(t, err)

	require.NotNil(t, res.TableCopyState)

	stateTable1 := res.TableCopyState["table1"]
	stateTable2 := res.TableCopyState["table2"]
	stateTable3 := res.TableCopyState["table3"]
	require.NotNil(t, stateTable1)
	require.NotNil(t, stateTable2)
	require.NotNil(t, stateTable3)

	assert.Equal(t, int64(100), stateTable1.RowsTotal)
	assert.Equal(t, int64(100), stateTable1.RowsCopied)
	assert.Equal(t, float32(100), stateTable1.RowsPercentage)
	assert.Equal(t, vtctldatapb.TableCopyPhase_COMPLETE, stateTable1.Phase)
	assert.Equal(t, int64(200), stateTable2.RowsTotal)
	assert.Equal(t, int64(100), stateTable2.RowsCopied)
	assert.Equal(t, float32(50), stateTable2.RowsPercentage)
	assert.Equal(t, vtctldatapb.TableCopyPhase_IN_PROGRESS, stateTable2.Phase)
	assert.Equal(t, int64(5000), stateTable3.RowsTotal)
	assert.Equal(t, int64(0), stateTable3.RowsCopied)
	assert.Equal(t, float32(0), stateTable3.RowsPercentage)
	assert.Equal(t, vtctldatapb.TableCopyPhase_NOT_STARTED, stateTable3.Phase)
}

func TestDeleteShard(t *testing.T) {
	ctx := context.Background()

	sourceKeyspace := &testKeyspace{"source_keyspace", []string{"-"}}
	targetKeyspace := &testKeyspace{"target_keyspace", []string{"-"}}

	te := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer te.close()

	// Verify that shard exists.
	si, err := te.ts.GetShard(ctx, targetKeyspace.KeyspaceName, targetKeyspace.ShardNames[0])
	require.NoError(t, err)
	require.NotNil(t, si)

	// Expect to fail if recursive is false.
	err = te.ws.DeleteShard(ctx, targetKeyspace.KeyspaceName, targetKeyspace.ShardNames[0], false, true)
	assert.ErrorContains(t, err, "shard target_keyspace/- still has 1 tablets in cell")

	// Should not throw error if given keyspace or shard is invalid.
	err = te.ws.DeleteShard(ctx, "invalid_keyspace", "-", false, true)
	assert.NoError(t, err)

	// Successful shard delete.
	err = te.ws.DeleteShard(ctx, targetKeyspace.KeyspaceName, targetKeyspace.ShardNames[0], true, true)
	assert.NoError(t, err)

	// Check if the shard was deleted.
	_, err = te.ts.GetShard(ctx, targetKeyspace.KeyspaceName, targetKeyspace.ShardNames[0])
	assert.ErrorContains(t, err, "node doesn't exist")
}

func TestCopySchemaShard(t *testing.T) {
	ctx := context.Background()

	sourceKeyspace := &testKeyspace{"source_keyspace", []string{"-"}}
	targetKeyspace := &testKeyspace{"target_keyspace", []string{"-"}}

	te := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer te.close()

	sqlSchema := `create table t1(id bigint(20) unsigned auto_increment, msg varchar(64), primary key (id)) Engine=InnoDB;`
	te.tmc.schema[sourceKeyspace.KeyspaceName+".t1"] = &tabletmanagerdatapb.SchemaDefinition{
		DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:   "t1",
				Schema: sqlSchema,
				Columns: []string{
					"id",
					"msg",
				},
				Type: tmutils.TableBaseTable,
			},
		},
	}

	// Expect queries on target shards
	te.tmc.expectApplySchemaRequest(200, &applySchemaRequestResponse{
		change: &tmutils.SchemaChange{
			SQL:              "CREATE DATABASE `vt_target_keyspace`",
			Force:            false,
			AllowReplication: true,
			SQLMode:          vreplication.SQLMode,
		},
	})
	te.tmc.expectApplySchemaRequest(200, &applySchemaRequestResponse{
		change: &tmutils.SchemaChange{
			SQL:              sqlSchema,
			Force:            false,
			AllowReplication: true,
			SQLMode:          vreplication.SQLMode,
		},
	})

	sourceTablet := te.tablets[sourceKeyspace.KeyspaceName][100]
	err := te.ws.CopySchemaShard(ctx, sourceTablet.Alias, []string{"/.*/"}, nil, false, targetKeyspace.KeyspaceName, "-", 1*time.Second, true)
	assert.NoError(t, err)
	assert.Empty(t, te.tmc.applySchemaRequests[200])
}

func TestValidateShardsHaveVReplicationPermissions(t *testing.T) {
	ctx := context.Background()

	sourceKeyspace := &testKeyspace{"source_keyspace", []string{"-"}}
	targetKeyspace := &testKeyspace{"target_keyspace", []string{"-80", "80-"}}

	te := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer te.close()

	si1, err := te.ts.GetShard(ctx, targetKeyspace.KeyspaceName, targetKeyspace.ShardNames[0])
	require.NoError(t, err)
	si2, err := te.ts.GetShard(ctx, targetKeyspace.KeyspaceName, targetKeyspace.ShardNames[1])
	require.NoError(t, err)

	testcases := []struct {
		name                string
		response            *validateVReplicationPermissionsResponse
		expectedErrContains string
	}{
		{
			// Expect no error in this case.
			name: "unimplemented error",
			response: &validateVReplicationPermissionsResponse{
				err: status.Error(codes.Unimplemented, "unimplemented test"),
			},
		},
		{
			name: "tmc error",
			response: &validateVReplicationPermissionsResponse{
				err: errors.New("tmc throws error"),
			},
			expectedErrContains: "tmc throws error",
		},
		{
			name: "no permissions",
			response: &validateVReplicationPermissionsResponse{
				res: &tabletmanagerdatapb.ValidateVReplicationPermissionsResponse{
					User:  "vt_test_user",
					Ok:    false,
					Error: "vt_test_user does not have the required set of permissions",
				},
			},
			expectedErrContains: "vt_test_user does not have the required set of permissions",
		},
		{
			name: "success",
			response: &validateVReplicationPermissionsResponse{
				res: &tabletmanagerdatapb.ValidateVReplicationPermissionsResponse{
					User: "vt_filtered",
					Ok:   true,
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			te.tmc.expectValidateVReplicationPermissionsResponse(200, tc.response)
			te.tmc.expectValidateVReplicationPermissionsResponse(210, tc.response)
			err = te.ws.validateShardsHaveVReplicationPermissions(ctx, targetKeyspace.KeyspaceName, []*topo.ShardInfo{si1, si2})
			if tc.expectedErrContains == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tc.expectedErrContains)
		})
	}
}

func TestWorkflowUpdate(t *testing.T) {
	ctx := context.Background()

	sourceKeyspace := &testKeyspace{"source_keyspace", []string{"-"}}
	targetKeyspace := &testKeyspace{"target_keyspace", []string{"-80", "80-"}}

	te := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer te.close()

	req := &vtctldatapb.WorkflowUpdateRequest{
		Keyspace: targetKeyspace.KeyspaceName,
		TabletRequest: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
			Workflow: "wf1",
			State:    binlogdatapb.VReplicationWorkflowState_Running.Enum(),
		},
	}

	testcases := []struct {
		name     string
		response map[uint32]*tabletmanagerdatapb.UpdateVReplicationWorkflowResponse
		err      map[uint32]error

		// Match the tablet `changed` field from response.
		expectedResponse    map[uint32]bool
		expectedErrContains string
	}{
		{
			name: "one tablet stream changed",
			response: map[uint32]*tabletmanagerdatapb.UpdateVReplicationWorkflowResponse{
				200: {
					Result: &querypb.QueryResult{
						RowsAffected: 1,
					},
				},
				210: {
					Result: &querypb.QueryResult{
						RowsAffected: 0,
					},
				},
			},
			expectedResponse: map[uint32]bool{
				200: true,
				210: false,
			},
		},
		{
			name: "two tablet stream changed",
			response: map[uint32]*tabletmanagerdatapb.UpdateVReplicationWorkflowResponse{
				200: {
					Result: &querypb.QueryResult{
						RowsAffected: 1,
					},
				},
				210: {
					Result: &querypb.QueryResult{
						RowsAffected: 2,
					},
				},
			},
			expectedResponse: map[uint32]bool{
				200: true,
				210: true,
			},
		},
		{
			name: "tablet throws error",
			err: map[uint32]error{
				200: errors.New("test error from 200"),
			},
			expectedErrContains: "test error from 200",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// Add responses
			for tabletID, resp := range tc.response {
				te.tmc.AddUpdateVReplicationWorkflowRequestResponse(tabletID, &updateVReplicationWorkflowRequestResponse{
					req: req.TabletRequest,
					res: resp,
				})
			}
			// Add errors
			for tabletID, err := range tc.err {
				te.tmc.AddUpdateVReplicationWorkflowRequestResponse(tabletID, &updateVReplicationWorkflowRequestResponse{
					req: req.TabletRequest,
					err: err,
				})
			}

			res, err := te.ws.WorkflowUpdate(ctx, req)
			if tc.expectedErrContains != "" {
				assert.ErrorContains(t, err, tc.expectedErrContains)
				return
			}

			assert.NoError(t, err)
			for tabletID, changed := range tc.expectedResponse {
				i := slices.IndexFunc(res.Details, func(det *vtctldatapb.WorkflowUpdateResponse_TabletInfo) bool {
					return det.Tablet.Uid == tabletID
				})
				assert.NotEqual(t, -1, i)
				assert.Equal(t, changed, res.Details[i].Changed)
			}
		})
	}
}

func TestFinalizeMigrateWorkflow(t *testing.T) {
	ctx := context.Background()

	workflowName := "wf1"
	tableName1 := "t1"
	tableName2 := "t2"

	sourceKeyspace := &testKeyspace{"source_keyspace", []string{"-"}}
	targetKeyspace := &testKeyspace{"target_keyspace", []string{"-80", "80-"}}

	schema := map[string]*tabletmanagerdatapb.SchemaDefinition{
		tableName1: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   tableName1,
					Schema: fmt.Sprintf("CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))", tableName1),
				},
			},
		},
		tableName2: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   tableName2,
					Schema: fmt.Sprintf("CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))", tableName2),
				},
			},
		},
	}

	testcases := []struct {
		name          string
		expectQueries []string
		cancel        bool
		keepData      bool
	}{
		{
			name: "cancel false, keepData true",
			expectQueries: []string{
				"delete from _vt.vreplication where db_name = 'vt_target_keyspace' and workflow = 'wf1'",
			},
			cancel:   false,
			keepData: true,
		},
		{
			name: "cancel true, keepData false",
			expectQueries: []string{
				"delete from _vt.vreplication where db_name = 'vt_target_keyspace' and workflow = 'wf1'",
				"drop table `vt_target_keyspace`.`t1`",
				"drop table `vt_target_keyspace`.`t2`",
			},
			cancel:   true,
			keepData: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			te := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
			defer te.close()
			te.tmc.schema = schema

			ts, _, err := te.ws.getWorkflowState(ctx, targetKeyspace.KeyspaceName, workflowName)
			require.NoError(t, err)

			for _, q := range tc.expectQueries {
				te.tmc.expectVRQuery(200, q, nil)
				te.tmc.expectVRQuery(210, q, nil)
			}

			_, err = te.ws.finalizeMigrateWorkflow(ctx, ts, "", tc.cancel, tc.keepData, false, false)
			assert.NoError(t, err)

			ks, err := te.ts.GetSrvVSchema(ctx, "cell")
			require.NoError(t, err)
			assert.NotNil(t, ks.Keyspaces[targetKeyspace.KeyspaceName])

			// Expect tables to be present in the VSchema if cancel was false.
			if !tc.cancel {
				assert.Len(t, ks.Keyspaces[targetKeyspace.KeyspaceName].Tables, 2)
				assert.NotNil(t, ks.Keyspaces[targetKeyspace.KeyspaceName].Tables[tableName1])
				assert.NotNil(t, ks.Keyspaces[targetKeyspace.KeyspaceName].Tables[tableName2])
			} else {
				assert.Len(t, ks.Keyspaces[targetKeyspace.KeyspaceName].Tables, 0)
				assert.Nil(t, ks.Keyspaces[targetKeyspace.KeyspaceName].Tables[tableName1])
				assert.Nil(t, ks.Keyspaces[targetKeyspace.KeyspaceName].Tables[tableName2])
			}

			// Expect queries to be used.
			assert.Empty(t, te.tmc.applySchemaRequests[200])
			assert.Empty(t, te.tmc.applySchemaRequests[210])
		})
	}
}

func TestMaterializeAddTables(t *testing.T) {
	ctx := context.Background()

	sourceKeyspace := &testKeyspace{"source_keyspace", []string{"-"}}
	targetKeyspace := &testKeyspace{"target_keyspace", []string{"-80", "80-"}}

	te := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer te.close()

	tableName1 := "t1"
	tableName2 := "t2"
	schema := map[string]*tabletmanagerdatapb.SchemaDefinition{
		tableName1: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   tableName1,
					Schema: fmt.Sprintf("CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))", tableName1),
				},
			},
		},
		// This will be used in deploySchema().
		fmt.Sprintf("%s.%s", sourceKeyspace.KeyspaceName, tableName2): {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   tableName2,
					Schema: fmt.Sprintf("CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))", tableName2),
				},
			},
		},
	}
	te.tmc.schema = schema

	testcases := []struct {
		name                                          string
		request                                       *vtctldatapb.WorkflowAddTablesRequest
		expectApplySchemaRequest                      bool
		addUpdateVReplicationWorkflowRequestResponses []*updateVReplicationWorkflowRequestResponse
		expectedErrContains                           string
	}{
		{
			name: "success",
			request: &vtctldatapb.WorkflowAddTablesRequest{
				Workflow: "wf",
				Keyspace: targetKeyspace.KeyspaceName,
				TableSettings: []*vtctldatapb.TableMaterializeSettings{
					{
						TargetTable: "t2",
					},
				},
				MaterializationIntent: vtctldatapb.MaterializationIntent_REFERENCE,
			},
			addUpdateVReplicationWorkflowRequestResponses: []*updateVReplicationWorkflowRequestResponse{
				{
					req: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
						Workflow: "wf",
						State:    ptr.Of(binlogdatapb.VReplicationWorkflowState_Stopped),
					},
				},
				{
					req: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
						Workflow: "wf",
						State:    ptr.Of(binlogdatapb.VReplicationWorkflowState_Running),
						FilterRules: []*binlogdatapb.Rule{
							{
								Match:  "t2",
								Filter: "select * from t2",
							},
						},
					},
				},
			},
			expectApplySchemaRequest: true,
		},
		{
			name: "rule already exists error",
			request: &vtctldatapb.WorkflowAddTablesRequest{
				Workflow: "wf",
				Keyspace: targetKeyspace.KeyspaceName,
				TableSettings: []*vtctldatapb.TableMaterializeSettings{
					{
						TargetTable: "t1",
					},
				},
				MaterializationIntent: vtctldatapb.MaterializationIntent_REFERENCE,
			},
			expectedErrContains: "rule for table t1 already exists",
		},
		{
			name: "source table doesn't exist error",
			request: &vtctldatapb.WorkflowAddTablesRequest{
				Workflow: "wf",
				Keyspace: targetKeyspace.KeyspaceName,
				TableSettings: []*vtctldatapb.TableMaterializeSettings{
					{
						TargetTable: "t3",
					},
				},
				MaterializationIntent: vtctldatapb.MaterializationIntent_REFERENCE,
			},
			addUpdateVReplicationWorkflowRequestResponses: []*updateVReplicationWorkflowRequestResponse{
				{
					req: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
						Workflow: "wf",
						State:    ptr.Of(binlogdatapb.VReplicationWorkflowState_Stopped),
					},
				},
				{
					req: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
						Workflow: "wf",
						State:    ptr.Of(binlogdatapb.VReplicationWorkflowState_Running),
						// Don't change anything else, so pass simulated NULLs.
						Cells:       textutil.SimulatedNullStringSlice,
						TabletTypes: textutil.SimulatedNullTabletTypeSlice,
					},
				},
			},
			expectedErrContains: "source table t3",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectApplySchemaRequest {
				te.tmc.expectApplySchemaRequest(200, &applySchemaRequestResponse{
					change: &tmutils.SchemaChange{
						SQL:                     "/create table t2",
						Force:                   false,
						AllowReplication:        true,
						SQLMode:                 vreplication.SQLMode,
						DisableForeignKeyChecks: true,
					},
					matchSqlOnly: true,
				})
				te.tmc.expectApplySchemaRequest(210, &applySchemaRequestResponse{
					change: &tmutils.SchemaChange{
						SQL:                     "/create table t2",
						Force:                   false,
						AllowReplication:        true,
						SQLMode:                 vreplication.SQLMode,
						DisableForeignKeyChecks: true,
					},
					matchSqlOnly: true,
				})
			}
			for _, reqres := range tc.addUpdateVReplicationWorkflowRequestResponses {
				te.tmc.AddUpdateVReplicationWorkflowRequestResponse(200, reqres)
				te.tmc.AddUpdateVReplicationWorkflowRequestResponse(210, reqres)
			}
			err := te.ws.WorkflowAddTables(ctx, tc.request)
			if tc.expectedErrContains == "" {
				assert.NoError(t, err)
				assert.Empty(t, te.tmc.applySchemaRequests[200])
				assert.Empty(t, te.tmc.applySchemaRequests[210])
				assert.Empty(t, te.tmc.updateVReplicationWorklowRequests[200])
				assert.Empty(t, te.tmc.updateVReplicationWorklowRequests[210])
				return
			}
			assert.ErrorContains(t, err, tc.expectedErrContains)
			assert.Empty(t, te.tmc.applySchemaRequests[200])
			assert.Empty(t, te.tmc.applySchemaRequests[210])
			assert.Empty(t, te.tmc.updateVReplicationWorklowRequests[200])
			assert.Empty(t, te.tmc.updateVReplicationWorklowRequests[210])
		})
	}
}
