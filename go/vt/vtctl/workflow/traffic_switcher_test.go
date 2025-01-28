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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type testTrafficSwitcher struct {
	trafficSwitcher
	sourceKeyspaceSchema *vindexes.KeyspaceSchema
}

func (tts *testTrafficSwitcher) SourceKeyspaceSchema() *vindexes.KeyspaceSchema {
	return tts.sourceKeyspaceSchema
}

func TestReverseWorkflowName(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{
			in:  "aa",
			out: "aa_reverse",
		},
		{
			in:  "aa_reverse",
			out: "aa",
		},
		{
			in:  "aa_reverse_aa",
			out: "aa_reverse_aa_reverse",
		},
	}
	for _, test := range tests {
		got := ReverseWorkflowName(test.in)
		assert.Equal(t, test.out, got)
	}
}

func TestGetTargetSequenceMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cell := "cell1"
	workflow := "wf1"
	table := "`t1`"
	tableDDL := "create table t1 (id int not null auto_increment primary key, c1 varchar(10))"
	table2 := "t2"
	unescapedTable := "t1"
	sourceKeyspace := &testKeyspace{
		KeyspaceName: "source-ks",
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: "targetks",
		ShardNames:   []string{"-80", "80-"},
	}
	vindexes := map[string]*vschema.Vindex{
		"xxhash": {
			Type: "xxhash",
		},
	}
	env := newTestEnv(t, ctx, cell, sourceKeyspace, targetKeyspace)
	defer env.close()

	env.tmc.schema = map[string]*tabletmanagerdatapb.SchemaDefinition{
		unescapedTable: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   unescapedTable,
					Schema: tableDDL,
				},
			},
		},
	}

	type testCase struct {
		name                                   string
		sourceVSchema                          *vschema.Keyspace
		targetVSchema                          *vschema.Keyspace
		options                                *vtctldatapb.WorkflowOptions
		want                                   map[string]*sequenceMetadata
		expectSourceApplySchemaRequestResponse *applySchemaRequestResponse
		err                                    string
	}
	tests := []testCase{
		{
			name: "no sequences",
			want: nil,
		},
		{
			name: "sequences with backticks and qualified table",
			sourceVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					"`my-seq1`": {
						Type: "sequence",
					},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					table: {
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "`my-col`",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "`my-col`",
							Sequence: fmt.Sprintf("`%s`.`my-seq1`", sourceKeyspace.KeyspaceName),
						},
					},
				},
			},
			want: map[string]*sequenceMetadata{
				"my-seq1": {
					backingTableName:     "my-seq1",
					backingTableKeyspace: "source-ks",
					backingTableDBName:   "vt_source-ks",
					usingTableName:       unescapedTable,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Column: "my-col",
								Name:   "xxhash",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: fmt.Sprintf("%s.my-seq1", sourceKeyspace.KeyspaceName),
						},
					},
				},
			},
		},
		{
			name: "auto_increment replaced with sequence",
			sourceVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables:   map[string]*vschema.Table{}, // Sequence table will be created
			},
			options: &vtctldatapb.WorkflowOptions{
				ShardedAutoIncrementHandling: vtctldatapb.ShardedAutoIncrementHandling_REPLACE,
				GlobalKeyspace:               sourceKeyspace.KeyspaceName,
			},
			expectSourceApplySchemaRequestResponse: &applySchemaRequestResponse{
				change: &tmutils.SchemaChange{
					SQL: sqlparser.BuildParsedQuery(sqlCreateSequenceTable,
						sqlescape.EscapeID(fmt.Sprintf(autoSequenceTableFormat, unescapedTable))).Query,
					Force:                   false,
					AllowReplication:        true,
					SQLMode:                 vreplication.SQLMode,
					DisableForeignKeyChecks: true,
				},
				res: &tabletmanagerdatapb.SchemaChangeResult{},
			},
			targetVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					table: {
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "`my-col`",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: fmt.Sprintf(autoSequenceTableFormat, unescapedTable),
						},
					},
				},
			},
			want: map[string]*sequenceMetadata{
				fmt.Sprintf(autoSequenceTableFormat, unescapedTable): {
					backingTableName:     fmt.Sprintf(autoSequenceTableFormat, unescapedTable),
					backingTableKeyspace: "source-ks",
					backingTableDBName:   "vt_source-ks",
					usingTableName:       unescapedTable,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Column: "my-col",
								Name:   "xxhash",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: fmt.Sprintf(autoSequenceTableFormat, unescapedTable),
						},
					},
				},
			},
		},
		{
			name: "sequences with backticks",
			sourceVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					"`my-seq1`": {
						Type: "sequence",
					},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					table: {
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "`my-col`",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "`my-col`",
							Sequence: "`my-seq1`",
						},
					},
				},
			},
			want: map[string]*sequenceMetadata{
				"my-seq1": {
					backingTableName:     "my-seq1",
					backingTableKeyspace: "source-ks",
					backingTableDBName:   "vt_source-ks",
					usingTableName:       unescapedTable,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Column: "my-col",
								Name:   "xxhash",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: "my-seq1",
						},
					},
				},
			},
		},
		{
			name: "sequences using vindexes with both column definition structures",
			sourceVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					"seq1": {
						Type: "sequence",
					},
					"seq2": {
						Type: "sequence",
					},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					table: {
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "col1",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "col1",
							Sequence: fmt.Sprintf("%s.seq1", sourceKeyspace.KeyspaceName),
						},
					},
					table2: {
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Name:    "xxhash",
								Columns: []string{"col2"},
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "col2",
							Sequence: fmt.Sprintf("%s.seq2", sourceKeyspace.KeyspaceName),
						},
					},
				},
			},
			want: map[string]*sequenceMetadata{
				"seq1": {
					backingTableName:     "seq1",
					backingTableKeyspace: "source-ks",
					backingTableDBName:   "vt_source-ks",
					usingTableName:       unescapedTable,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Column: "col1",
								Name:   "xxhash",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "col1",
							Sequence: fmt.Sprintf("%s.seq1", sourceKeyspace.KeyspaceName),
						},
					},
				},
				"seq2": {
					backingTableName:     "seq2",
					backingTableKeyspace: "source-ks",
					backingTableDBName:   "vt_source-ks",
					usingTableName:       table2,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Columns: []string{"col2"},
								Name:    "xxhash",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "col2",
							Sequence: fmt.Sprintf("%s.seq2", sourceKeyspace.KeyspaceName),
						},
					},
				},
			},
		},
		{
			name: "sequence with table having mult-col vindex",
			sourceVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					"seq1": {
						Type: "sequence",
					},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					table: {
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Name:    "xxhash",
								Columns: []string{"col3", "col4"},
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "col1",
							Sequence: fmt.Sprintf("%s.seq1", sourceKeyspace.KeyspaceName),
						},
					},
				},
			},
			want: map[string]*sequenceMetadata{
				"seq1": {
					backingTableName:     "seq1",
					backingTableKeyspace: "source-ks",
					backingTableDBName:   "vt_source-ks",
					usingTableName:       unescapedTable,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Columns: []string{"col3", "col4"},
								Name:    "xxhash",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "col1",
							Sequence: fmt.Sprintf("%s.seq1", sourceKeyspace.KeyspaceName),
						},
					},
				},
			},
		},
		{
			name: "invalid table name",
			sourceVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					"`my-`seq1`": {
						Type: "sequence",
					},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					table: {
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "`my-col`",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "`my-col`",
							Sequence: "`my-seq1`",
						},
					},
				},
			},
			err: "invalid table name \"`my-`seq1`\" in keyspace source-ks: UnescapeID err: unexpected single backtick at position 3 in 'my-`seq1'",
		},
		{
			name: "invalid keyspace name",
			sourceVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					"`my-seq1`": {
						Type: "sequence",
					},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					table: {
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "`my-col`",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "`my-col`",
							Sequence: "`ks`1`.`my-seq1`",
						},
					},
				},
			},
			err: "invalid keyspace in qualified sequence table name \"`ks`1`.`my-seq1`\" defined in sequence table column_vindexes:{column:\"`my-col`\" name:\"xxhash\"} auto_increment:{column:\"`my-col`\" sequence:\"`ks`1`.`my-seq1`\"}: UnescapeID err: unexpected single backtick at position 2 in 'ks`1'",
		},
		{
			name: "invalid auto-inc column name",
			sourceVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					"`my-seq1`": {
						Type: "sequence",
					},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					table: {
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "`my-col`",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "`my`-col`",
							Sequence: "`my-seq1`",
						},
					},
				},
			},
			err: "invalid auto-increment column name \"`my`-col`\" defined in sequence table column_vindexes:{column:\"my-col\" name:\"xxhash\"} auto_increment:{column:\"`my`-col`\" sequence:\"my-seq1\"}: UnescapeID err: unexpected single backtick at position 2 in 'my`-col'",
		},
		{
			name: "invalid sequence name",
			sourceVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					"`my-seq1`": {
						Type: "sequence",
					},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					table: {
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "`my-col`",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "`my-col`",
							Sequence: "`my-`seq1`",
						},
					},
				},
			},
			err: "invalid sequence table name \"`my-`seq1`\" defined in sequence table column_vindexes:{column:\"`my-col`\" name:\"xxhash\"} auto_increment:{column:\"`my-col`\" sequence:\"`my-`seq1`\"}: UnescapeID err: unexpected single backtick at position 3 in 'my-`seq1'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := env.ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
				Name:     sourceKeyspace.KeyspaceName,
				Keyspace: tc.sourceVSchema,
			})
			require.NoError(t, err)
			err = env.ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
				Name:     targetKeyspace.KeyspaceName,
				Keyspace: tc.targetVSchema,
			})
			require.NoError(t, err)
			err = env.ts.RebuildSrvVSchema(ctx, nil)
			require.NoError(t, err)
			sources := make(map[string]*MigrationSource, len(sourceKeyspace.ShardNames))
			targets := make(map[string]*MigrationTarget, len(targetKeyspace.ShardNames))
			for i, shard := range sourceKeyspace.ShardNames {
				tablet := env.tablets[sourceKeyspace.KeyspaceName][startingSourceTabletUID+(i*tabletUIDStep)]
				sources[shard] = &MigrationSource{
					primary: &topo.TabletInfo{
						Tablet: tablet,
					},
				}
				if tc.expectSourceApplySchemaRequestResponse != nil {
					env.tmc.expectApplySchemaRequest(tablet.Alias.Uid, tc.expectSourceApplySchemaRequestResponse)
				}
			}
			for i, shard := range targetKeyspace.ShardNames {
				tablet := env.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID+(i*tabletUIDStep)]
				targets[shard] = &MigrationTarget{
					primary: &topo.TabletInfo{
						Tablet: tablet,
					},
				}
			}
			ts := &trafficSwitcher{
				id:             1,
				ws:             env.ws,
				workflow:       workflow,
				tables:         []string{table, table2},
				sourceKeyspace: sourceKeyspace.KeyspaceName,
				targetKeyspace: targetKeyspace.KeyspaceName,
				sources:        sources,
				targets:        targets,
				options:        tc.options,
			}
			got, err := ts.getTargetSequenceMetadata(ctx)
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
			require.EqualValues(t, tc.want, got)
		})
	}
}

// TestSwitchTrafficPositionHandling confirms that if any writes are somehow
// executed against the source between the stop source writes and wait for
// catchup steps, that we have the correct position and do not lose the write(s).
func TestTrafficSwitchPositionHandling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"
	sourceKeyspaceName := "sourceks"
	targetKeyspaceName := "targetks"

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

	sourceKeyspace := &testKeyspace{
		KeyspaceName: sourceKeyspaceName,
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: targetKeyspaceName,
		ShardNames:   []string{"0"},
	}

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspaceName, workflowName)
	require.NoError(t, err)
	sw := &switcher{ts: ts, s: env.ws}

	lockCtx, sourceUnlock, lockErr := sw.lockKeyspace(ctx, ts.SourceKeyspaceName(), "test")
	require.NoError(t, lockErr)
	ctx = lockCtx
	defer sourceUnlock(&err)
	lockCtx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.TargetKeyspaceName(), "test")
	require.NoError(t, lockErr)
	ctx = lockCtx
	defer targetUnlock(&err)

	err = ts.stopSourceWrites(ctx)
	require.NoError(t, err)

	// Now we simulate a write on the source.
	newPosition := position[:strings.LastIndex(position, "-")+1]
	oldSeqNo, err := strconv.Atoi(position[strings.LastIndex(position, "-")+1:])
	require.NoError(t, err)
	newPosition = fmt.Sprintf("%s%d", newPosition, oldSeqNo+1)
	env.tmc.setPrimaryPosition(env.tablets[sourceKeyspaceName][startingSourceTabletUID], newPosition)

	// And confirm that we picked up the new position.
	err = ts.gatherSourcePositions(ctx)
	require.NoError(t, err)
	err = ts.ForAllSources(func(ms *MigrationSource) error {
		require.Equal(t, newPosition, ms.Position)
		return nil
	})
	require.NoError(t, err)
}

func TestInitializeTargetSequences(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"
	sourceKeyspaceName := "sourceks"
	targetKeyspaceName := "targetks"

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

	sourceKeyspace := &testKeyspace{
		KeyspaceName: sourceKeyspaceName,
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: targetKeyspaceName,
		ShardNames:   []string{"0"},
	}

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspaceName, workflowName)
	require.NoError(t, err)
	sw := &switcher{ts: ts, s: env.ws}

	sequencesByBackingTable := map[string]*sequenceMetadata{
		"my-seq1": {
			backingTableName:     "my-seq1",
			backingTableKeyspace: sourceKeyspaceName,
			backingTableDBName:   fmt.Sprintf("vt_%s", sourceKeyspaceName),
			usingTableName:       tableName,
			usingTableDBName:     "vt_targetks",
			usingTableDefinition: &vschema.Table{
				AutoIncrement: &vschema.AutoIncrement{
					Column:   "my-col",
					Sequence: fmt.Sprintf("%s.my-seq1", sourceKeyspace.KeyspaceName),
				},
			},
		},
	}

	env.tmc.expectVRQuery(200, "/select max.*", sqltypes.MakeTestResult(sqltypes.MakeTestFields("maxval", "int64"), "34"))
	// Expect the insert query to be executed with 35 as a params, since we provide a maxID of 34 in the last query
	env.tmc.expectVRQuery(100, "/insert into.*35.*", &sqltypes.Result{RowsAffected: 1})

	err = sw.initializeTargetSequences(ctx, sequencesByBackingTable)
	assert.NoError(t, err)

	// Expect the queries to be cleared
	assert.Empty(t, env.tmc.vrQueries[100])
	assert.Empty(t, env.tmc.vrQueries[200])
}

func TestAddTenantFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"
	sourceKeyspaceName := "sourceks"
	targetKeyspaceName := "targetks"

	sourceKeyspace := &testKeyspace{
		KeyspaceName: sourceKeyspaceName,
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: targetKeyspaceName,
		ShardNames:   []string{"0"},
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

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	err := env.ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: targetKeyspaceName,
		Keyspace: &vschema.Keyspace{
			MultiTenantSpec: &vschema.MultiTenantSpec{
				TenantIdColumnName: "tenant_id",
				TenantIdColumnType: sqltypes.Int64,
			},
		},
	})
	require.NoError(t, err)

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspaceName, workflowName)
	require.NoError(t, err)

	ts.options.TenantId = "123"

	filter, err := ts.addTenantFilter(ctx, fmt.Sprintf("select * from %s where id < 5", tableName))
	assert.NoError(t, err)
	assert.Equal(t, "select * from t1 where tenant_id = 123 and id < 5", filter)
}

func TestChangeShardRouting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"
	sourceKeyspaceName := "sourceks"
	targetKeyspaceName := "targetks"

	sourceKeyspace := &testKeyspace{
		KeyspaceName: sourceKeyspaceName,
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: targetKeyspaceName,
		ShardNames:   []string{"0"},
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

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspaceName, workflowName)
	require.NoError(t, err)

	err = env.ws.ts.UpdateSrvKeyspace(ctx, "cell", targetKeyspaceName, &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: "0",
					},
				},
			},
		},
	})
	require.NoError(t, err)

	err = env.ws.ts.UpdateSrvKeyspace(ctx, "cell", sourceKeyspaceName, &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: "0",
					},
				},
			},
		},
	})
	require.NoError(t, err)

	ctx, _, err = env.ws.ts.LockShard(ctx, targetKeyspaceName, "0", "targetks0")
	require.NoError(t, err)

	ctx, _, err = env.ws.ts.LockKeyspace(ctx, targetKeyspaceName, "targetks0")
	require.NoError(t, err)

	err = ts.changeShardRouting(ctx)
	assert.NoError(t, err)

	sourceShardInfo, err := env.ws.ts.GetShard(ctx, sourceKeyspaceName, "0")
	assert.NoError(t, err)
	assert.False(t, sourceShardInfo.IsPrimaryServing, "source shard shouldn't have it's primary serving after changeShardRouting() is called.")

	targetShardInfo, err := env.ws.ts.GetShard(ctx, targetKeyspaceName, "0")
	assert.NoError(t, err)
	assert.True(t, targetShardInfo.IsPrimaryServing, "target shard should have it's primary serving after changeShardRouting() is called.")
}

func TestAddParticipatingTablesToKeyspace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"
	sourceKeyspaceName := "sourceks"
	targetKeyspaceName := "targetks"

	sourceKeyspace := &testKeyspace{
		KeyspaceName: sourceKeyspaceName,
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: targetKeyspaceName,
		ShardNames:   []string{"0"},
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

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspaceName, workflowName)
	require.NoError(t, err)

	err = ts.addParticipatingTablesToKeyspace(ctx, sourceKeyspaceName, "")
	assert.NoError(t, err)

	vs, err := env.ts.GetVSchema(ctx, sourceKeyspaceName)
	assert.NoError(t, err)
	assert.NotNil(t, vs.Tables["t1"])
	assert.Empty(t, vs.Tables["t1"])

	specs := `{"t1":{"column_vindexes":[{"column":"col1","name":"v1"}, {"column":"col2","name":"v2"}]},"t2":{"column_vindexes":[{"column":"col2","name":"v2"}]}}`
	err = ts.addParticipatingTablesToKeyspace(ctx, sourceKeyspaceName, specs)
	assert.NoError(t, err)

	vs, err = env.ts.GetVSchema(ctx, sourceKeyspaceName)
	assert.NoError(t, err)
	require.NotNil(t, vs.Tables["t1"])
	require.NotNil(t, vs.Tables["t2"])
	assert.Len(t, vs.Tables["t1"].ColumnVindexes, 2)
	assert.Len(t, vs.Tables["t2"].ColumnVindexes, 1)
}

func TestCancelMigration_TABLES(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"

	sourceKeyspace := &testKeyspace{
		KeyspaceName: "sourceks",
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: "targetks",
		ShardNames:   []string{"0"},
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

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspace.KeyspaceName, workflowName)
	require.NoError(t, err)

	sm, err := BuildStreamMigrator(ctx, ts, false, sqlparser.NewTestParser())
	require.NoError(t, err)

	env.tmc.expectVRQuery(200, "update _vt.vreplication set state='Running', message='' where db_name='vt_targetks' and workflow='wf1'", &sqltypes.Result{})
	env.tmc.expectVRQuery(100, "delete from _vt.vreplication where db_name = 'vt_sourceks' and workflow = 'wf1_reverse'", &sqltypes.Result{})

	ctx, _, err = env.ts.LockKeyspace(ctx, targetKeyspace.KeyspaceName, "test")
	require.NoError(t, err)

	ctx, _, err = env.ts.LockKeyspace(ctx, sourceKeyspace.KeyspaceName, "test")
	require.NoError(t, err)

	err = topo.CheckKeyspaceLocked(ctx, ts.targetKeyspace)
	require.NoError(t, err)

	err = topo.CheckKeyspaceLocked(ctx, ts.sourceKeyspace)
	require.NoError(t, err)

	ts.cancelMigration(ctx, sm)

	// Expect the queries to be cleared
	assert.Empty(t, env.tmc.vrQueries[100])
	assert.Empty(t, env.tmc.vrQueries[200])
}

func TestCancelMigration_SHARDS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"

	sourceKeyspace := &testKeyspace{
		KeyspaceName: "sourceks",
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: "targetks",
		ShardNames:   []string{"0"},
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

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspace.KeyspaceName, workflowName)
	require.NoError(t, err)
	ts.migrationType = binlogdata.MigrationType_SHARDS

	sm, err := BuildStreamMigrator(ctx, ts, false, sqlparser.NewTestParser())
	require.NoError(t, err)

	env.tmc.expectVRQuery(100, "update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state='Running', stop_pos=null, message='' where db_name='vt_sourceks' and workflow != 'wf1_reverse'", &sqltypes.Result{})
	env.tmc.expectVRQuery(200, "update _vt.vreplication set state='Running', message='' where db_name='vt_targetks' and workflow='wf1'", &sqltypes.Result{})
	env.tmc.expectVRQuery(100, "delete from _vt.vreplication where db_name = 'vt_sourceks' and workflow = 'wf1_reverse'", &sqltypes.Result{})

	ctx, _, err = env.ts.LockKeyspace(ctx, targetKeyspace.KeyspaceName, "test")
	require.NoError(t, err)

	ctx, _, err = env.ts.LockKeyspace(ctx, sourceKeyspace.KeyspaceName, "test")
	require.NoError(t, err)

	err = topo.CheckKeyspaceLocked(ctx, ts.targetKeyspace)
	require.NoError(t, err)

	err = topo.CheckKeyspaceLocked(ctx, ts.sourceKeyspace)
	require.NoError(t, err)

	ts.cancelMigration(ctx, sm)

	// Expect the queries to be cleared
	assert.Empty(t, env.tmc.vrQueries[100])
	assert.Empty(t, env.tmc.vrQueries[200])
}
