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
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
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

// TestVDiffCreate performs some basic tests of the VDiffCreate function
// to ensure that it behaves as expected given a specific request.
func TestVDiffCreate(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	tmc := &fakeTMC{}
	s := NewServer(vtenv.NewTestEnv(), ts, tmc)

	tests := []struct {
		name    string
		req     *vtctldatapb.VDiffCreateRequest
		wantErr string
	}{
		{
			name: "no values",
			req:  &vtctldatapb.VDiffCreateRequest{},
			// We did not provide any keyspace or shard.
			wantErr: "FindAllShardsInKeyspace() invalid keyspace name: UnescapeID err: invalid input identifier ''",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.VDiffCreate(ctx, tt.req)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, got)
			require.NotEmpty(t, got.UUID)
		})
	}
}

func TestWorkflowDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"
	sourceKeyspaceName := "sourceks"
	targetKeyspaceName := "targetks"
	schema := map[string]*tabletmanagerdatapb.SchemaDefinition{
		"t1": {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   tableName,
					Schema: fmt.Sprintf("CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))", tableName),
				},
			},
		},
	}

	testcases := []struct {
		name                           string
		sourceKeyspace, targetKeyspace *testKeyspace
		preFunc                        func(t *testing.T, env *testEnv)
		req                            *vtctldatapb.WorkflowDeleteRequest
		expectedSourceQueries          []*queryResult
		expectedTargetQueries          []*queryResult
		want                           *vtctldatapb.WorkflowDeleteResponse
		wantErr                        bool
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
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, tableName),
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
			name: "basic with existing denied table entries",
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
						err := si.UpdateDeniedTables(lockCtx, topodatapb.TabletType_PRIMARY, nil, false, []string{tableName, "t2", "t3"})
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
					query:  fmt.Sprintf("drop table `vt_%s`.`%s`", targetKeyspaceName, tableName),
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
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotNil(t, tc.sourceKeyspace)
			require.NotNil(t, tc.targetKeyspace)
			require.NotNil(t, tc.req)
			env := newTestEnv(t, ctx, defaultCellName, tc.sourceKeyspace, tc.targetKeyspace)
			defer env.close()
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
			if tc.preFunc != nil {
				tc.preFunc(t, env)
			}
			got, err := env.ws.WorkflowDelete(ctx, tc.req)
			if (err != nil) != tc.wantErr {
				require.Fail(t, "unexpected error value", "Server.WorkflowDelete() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			require.EqualValues(t, got, tc.want, "Server.WorkflowDelete() = %v, want %v", got, tc.want)
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
						si, err := env.ts.GetShard(ctx, keyspace.KeyspaceName, shardName)
						require.NoError(t, err)
						require.Zero(t, si.Shard.TabletControls)
					}
				}
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
	tabletTypes := []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}
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
				TabletTypes: tabletTypes,
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
				TabletTypes: tabletTypes,
			},
			want: &vtctldatapb.WorkflowSwitchTrafficResponse{
				Summary:      fmt.Sprintf("ReverseTraffic was successful for workflow %s.%s", targetKeyspaceName, workflowName),
				StartState:   "All Reads Switched. Writes Switched",
				CurrentState: "Reads Not Switched. Writes Not Switched",
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
				env.addTableRoutingRules(t, ctx, tabletTypes, []string{tableName})
				env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, copyTableQR)
				for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, cutoverQR)
				}
				for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, journalQR)
				}
				for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, lockTableQR)
				}
				env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, deleteWFQR)
				env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, createWFQR)
				env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, createJournalQR)
				env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, freezeReverseWFQR)
			}
			got, err := env.ws.WorkflowSwitchTraffic(ctx, tc.req)
			if (err != nil) != tc.wantErr {
				require.Fail(t, "unexpected error value", "Server.WorkflowSwitchTraffic() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			require.Equal(t, tc.want.String(), got.String(), "Server.WorkflowSwitchTraffic() = %v, want %v", got, tc.want)

			// Confirm that we have the expected routing rules.
			rr, err := env.ts.GetRoutingRules(ctx)
			require.NoError(t, err)
			to := fmt.Sprintf("%s.%s", tc.targetKeyspace.KeyspaceName, tableName)
			if tc.req.Direction == int32(DirectionBackward) {
				to = fmt.Sprintf("%s.%s", tc.sourceKeyspace.KeyspaceName, tableName)
			}
			for _, rr := range rr.Rules {
				for _, tt := range rr.ToTables {
					require.Equal(t, to, tt)
				}
			}
			// Confirm that we have the expected denied tables entires.
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
	tabletTypes := []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}
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
				TabletTypes: tabletTypes,
				DryRun:      true,
			},
			want: []string{
				fmt.Sprintf("Lock keyspace %s", sourceKeyspaceName),
				fmt.Sprintf("Switch reads for tables [%s] to keyspace %s for tablet types [REPLICA,RDONLY]", tablesStr, targetKeyspaceName),
				fmt.Sprintf("Routing rules for tables [%s] will be updated", tablesStr),
				fmt.Sprintf("Unlock keyspace %s", sourceKeyspaceName),
				fmt.Sprintf("Lock keyspace %s", sourceKeyspaceName),
				fmt.Sprintf("Lock keyspace %s", targetKeyspaceName),
				fmt.Sprintf("Stop writes on keyspace %s for tables [%s]: [keyspace:%s;shard:-80;position:%s,keyspace:%s;shard:80-;position:%s]",
					sourceKeyspaceName, tablesStr, sourceKeyspaceName, position, sourceKeyspaceName, position),
				"Wait for vreplication on stopped streams to catchup for up to 30s",
				fmt.Sprintf("Create reverse vreplication workflow %s", ReverseWorkflowName(workflowName)),
				"Create journal entries on source databases",
				fmt.Sprintf("Enable writes on keyspace %s for tables [%s]", targetKeyspaceName, tablesStr),
				fmt.Sprintf("Switch routing from keyspace %s to keyspace %s", sourceKeyspaceName, targetKeyspaceName),
				fmt.Sprintf("Routing rules for tables [%s] will be updated", tablesStr),
				fmt.Sprintf("Switch writes completed, freeze and delete vreplication streams on: [tablet:%d,tablet:%d]", startingTargetTabletUID, startingTargetTabletUID+tabletUIDStep),
				fmt.Sprintf("Mark vreplication streams frozen on: [keyspace:%s;shard:-80;tablet:%d;workflow:%s;dbname:vt_%s,keyspace:%s;shard:80-;tablet:%d;workflow:%s;dbname:vt_%s]",
					targetKeyspaceName, startingTargetTabletUID, workflowName, targetKeyspaceName, targetKeyspaceName, startingTargetTabletUID+tabletUIDStep, workflowName, targetKeyspaceName),
				fmt.Sprintf("Unlock keyspace %s", targetKeyspaceName),
				fmt.Sprintf("Unlock keyspace %s", sourceKeyspaceName),
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
				TabletTypes: tabletTypes,
				DryRun:      true,
			},
			want: []string{
				fmt.Sprintf("Lock keyspace %s", targetKeyspaceName),
				fmt.Sprintf("Switch reads for tables [%s] to keyspace %s for tablet types [REPLICA,RDONLY]", tablesStr, targetKeyspaceName),
				fmt.Sprintf("Routing rules for tables [%s] will be updated", tablesStr),
				fmt.Sprintf("Unlock keyspace %s", targetKeyspaceName),
				fmt.Sprintf("Lock keyspace %s", targetKeyspaceName),
				fmt.Sprintf("Lock keyspace %s", sourceKeyspaceName),
				fmt.Sprintf("Stop writes on keyspace %s for tables [%s]: [keyspace:%s;shard:-80;position:%s,keyspace:%s;shard:80-;position:%s]",
					targetKeyspaceName, tablesStr, targetKeyspaceName, position, targetKeyspaceName, position),
				"Wait for vreplication on stopped streams to catchup for up to 30s",
				fmt.Sprintf("Create reverse vreplication workflow %s", workflowName),
				"Create journal entries on source databases",
				fmt.Sprintf("Enable writes on keyspace %s for tables [%s]", sourceKeyspaceName, tablesStr),
				fmt.Sprintf("Switch routing from keyspace %s to keyspace %s", targetKeyspaceName, sourceKeyspaceName),
				fmt.Sprintf("Routing rules for tables [%s] will be updated", tablesStr),
				fmt.Sprintf("Switch writes completed, freeze and delete vreplication streams on: [tablet:%d,tablet:%d]", startingSourceTabletUID, startingSourceTabletUID+tabletUIDStep),
				fmt.Sprintf("Mark vreplication streams frozen on: [keyspace:%s;shard:-80;tablet:%d;workflow:%s;dbname:vt_%s,keyspace:%s;shard:80-;tablet:%d;workflow:%s;dbname:vt_%s]",
					sourceKeyspaceName, startingSourceTabletUID, ReverseWorkflowName(workflowName), sourceKeyspaceName, sourceKeyspaceName, startingSourceTabletUID+tabletUIDStep, ReverseWorkflowName(workflowName), sourceKeyspaceName),
				fmt.Sprintf("Unlock keyspace %s", sourceKeyspaceName),
				fmt.Sprintf("Unlock keyspace %s", targetKeyspaceName),
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
				env.addTableRoutingRules(t, ctx, tabletTypes, tables)
				env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.sourceKeyspace.KeyspaceName, copyTableQR)
				for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, journalQR)
				}
				for i := 0; i < len(tc.targetKeyspace.ShardNames); i++ { // Per stream
					env.tmc.expectVRQueryResultOnKeyspaceTablets(tc.targetKeyspace.KeyspaceName, lockTableQR)
				}
			}
			got, err := env.ws.WorkflowSwitchTraffic(ctx, tc.req)
			require.NoError(t, err)

			require.EqualValues(t, tc.want, got.DryRunResults, "Server.WorkflowSwitchTraffic(DryRun:true) = %v, want %v", got.DryRunResults, tc.want)
		})
	}
}
