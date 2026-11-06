/*
Copyright 2025 The Vitess Authors.

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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

func TestInitializeTargetSequences(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"
	tableName2 := "t2"
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
		tableName2: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   tableName2,
					Schema: fmt.Sprintf("CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))", tableName2),
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

	// my-seq1 was missing from every keyspace's vschema, and both backing
	// tables were missing from the vschema when it was checked, but my-seq2
	// has been added since.
	mySeq2Definition := func() *vschema.Table {
		return &vschema.Table{Type: "sequence", Columns: []*vschema.Column{{Name: "id"}}}
	}
	err = env.ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name:     sourceKeyspaceName,
		Keyspace: &vschema.Keyspace{Tables: map[string]*vschema.Table{"my-seq2": mySeq2Definition()}},
	})
	require.NoError(t, err)
	sequencesByBackingTable := map[string]*sequenceMetadata{
		"my-seq1": {
			backingTableName:     "my-seq1",
			backingTableKeyspace: sourceKeyspaceName,
			backingTableDBName:   "vt_" + sourceKeyspaceName,
			usingTableName:       tableName,
			usingTableDBName:     "vt_targetks",
			usingTableDefinition: &vschema.Table{
				AutoIncrement: &vschema.AutoIncrement{
					Column:   "my-col",
					Sequence: sourceKeyspace.KeyspaceName + ".my-seq1",
				},
			},
			registerInVSchema:  true,
			createBackingTable: true,
		},
		"my-seq2": {
			backingTableName:     "my-seq2",
			backingTableKeyspace: sourceKeyspaceName,
			backingTableDBName:   "vt_" + sourceKeyspaceName,
			usingTableName:       tableName2,
			usingTableDBName:     "vt_targetks",
			usingTableDefinition: &vschema.Table{
				AutoIncrement: &vschema.AutoIncrement{
					Column:   "my-col-2",
					Sequence: sourceKeyspace.KeyspaceName + ".my-seq2",
				},
			},
			registerInVSchema: true,
		},
	}

	type testCase struct {
		name                  string
		maxValueRequest       *getMaxValueForSequencesRequestResponse
		updateSeqTableRequest *tabletmanagerdatapb.UpdateSequenceTablesRequest
	}

	testCases := []testCase{
		{
			name: "initialize sequences",
			maxValueRequest: &getMaxValueForSequencesRequestResponse{
				req: &tabletmanagerdatapb.GetMaxValueForSequencesRequest{
					Sequences: []*tabletmanagerdatapb.GetMaxValueForSequencesRequest_SequenceMetadata{
						{
							BackingTableName:        "my-seq1",
							UsingColEscaped:         "`my-col`",
							UsingTableNameEscaped:   fmt.Sprintf("`%s`", tableName),
							UsingTableDbNameEscaped: "`vt_targetks`",
						},
						{
							BackingTableName:        "my-seq2",
							UsingColEscaped:         "`my-col-2`",
							UsingTableNameEscaped:   fmt.Sprintf("`%s`", tableName2),
							UsingTableDbNameEscaped: "`vt_targetks`",
						},
					},
				},
				res: &tabletmanagerdatapb.GetMaxValueForSequencesResponse{
					MaxValuesBySequenceTable: map[string]int64{
						"my-seq1": 34,
						"my-seq2": 10,
					},
				},
			},
			updateSeqTableRequest: &tabletmanagerdatapb.UpdateSequenceTablesRequest{
				Sequences: []*tabletmanagerdatapb.UpdateSequenceTablesRequest_SequenceMetadata{
					{
						BackingTableName:   "my-seq1",
						BackingTableDbName: "vt_" + sourceKeyspaceName,
						MaxValue:           34,
					},
					{
						BackingTableName:   "my-seq2",
						BackingTableDbName: "vt_" + sourceKeyspaceName,
						MaxValue:           10,
					},
				},
			},
		},
		{
			name: "initialize sequences for empty tables",
			maxValueRequest: &getMaxValueForSequencesRequestResponse{
				req: &tabletmanagerdatapb.GetMaxValueForSequencesRequest{
					Sequences: []*tabletmanagerdatapb.GetMaxValueForSequencesRequest_SequenceMetadata{
						{
							BackingTableName:        "my-seq1",
							UsingColEscaped:         "`my-col`",
							UsingTableNameEscaped:   fmt.Sprintf("`%s`", tableName),
							UsingTableDbNameEscaped: "`vt_targetks`",
						},
						{
							BackingTableName:        "my-seq2",
							UsingColEscaped:         "`my-col-2`",
							UsingTableNameEscaped:   fmt.Sprintf("`%s`", tableName2),
							UsingTableDbNameEscaped: "`vt_targetks`",
						},
					},
				},
				res: &tabletmanagerdatapb.GetMaxValueForSequencesResponse{
					MaxValuesBySequenceTable: map[string]int64{},
				},
			},
			updateSeqTableRequest: &tabletmanagerdatapb.UpdateSequenceTablesRequest{
				Sequences: []*tabletmanagerdatapb.UpdateSequenceTablesRequest_SequenceMetadata{
					{
						BackingTableName:   "my-seq1",
						BackingTableDbName: "vt_" + sourceKeyspaceName,
						MaxValue:           0,
					},
					{
						BackingTableName:   "my-seq2",
						BackingTableDbName: "vt_" + sourceKeyspaceName,
						MaxValue:           0,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotNil(t, tc.maxValueRequest)
			require.NotNil(t, tc.updateSeqTableRequest)
			env.tmc.expectGetMaxValueForSequencesRequest(200, tc.maxValueRequest)
			env.tmc.expectUpdateSequenceTablesRequest(100, tc.updateSeqTableRequest)

			// SwitchTraffic prepares the sequences before it stops writes:
			// it creates my-seq1, and adds only it to the vschema.
			env.tmc.expectApplySchemaRequest(100, &applySchemaRequestResponse{
				change: &tmutils.SchemaChange{
					SQL:                     sqlparser.BuildParsedQuery(sqlCreateSequenceTable, sqlescape.EscapeID("my-seq1")).Query,
					AllowReplication:        true,
					SQLMode:                 vreplication.SQLMode,
					DisableForeignKeyChecks: true,
				},
				res: &tabletmanagerdatapb.SchemaChangeResult{},
			})
			err = sw.prepareTargetSequences(ctx, sequencesByBackingTable)
			require.NoError(t, err)
			assert.Empty(t, env.tmc.applySchemaRequests[100], "expected no remaining ApplySchema requests")
			kvs, err := env.ts.GetVSchema(ctx, sourceKeyspaceName)
			require.NoError(t, err)
			assert.Equal(t, map[string]*vschema.Table{
				"my-seq1": {Type: "sequence"},
				"my-seq2": mySeq2Definition(),
			}, kvs.Tables)

			err = sw.initializeTargetSequences(ctx, sequencesByBackingTable)
			assert.NoError(t, err)

			// Expect the requests to be cleared.
			assert.Emptyf(t, env.tmc.updateSequenceTablesRequests, "expected no remaining UpdateSequenceTables requests")
			assert.Emptyf(t, env.tmc.getMaxValueForSequencesRequests, "expected no remaining GetMaxValueForSequences requests")
		})
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
	// An unsharded keyspace, other than the source, that qualified sequence
	// references can name.
	globalKeyspace := "global-ks"
	env.addTablet(t, ctx, 300, globalKeyspace, "0", topodatapb.TabletType_PRIMARY, true)
	// An unsharded keyspace whose only shard has no primary.
	noPrimaryKeyspace := "no-primary-ks"
	env.addTablet(t, ctx, 400, noPrimaryKeyspace, "0", topodatapb.TabletType_REPLICA, true)
	require.NoError(t, env.ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{Name: noPrimaryKeyspace, Keyspace: &vschema.Keyspace{}}))

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
		name          string
		sourceVSchema *vschema.Keyspace
		targetVSchema *vschema.Keyspace
		globalVSchema *vschema.Keyspace
		options       *vtctldatapb.WorkflowOptions
		want          map[string]*sequenceMetadata
		err           string
		errCode       vtrpcpb.Code
	}
	tests := []testCase{
		{
			name: "no sequences",
			want: nil,
		},
		{
			// VTGate uses the table names in a vschema as they are, so the
			// entry whose name is in backticks doesn't define my-seq1, and
			// it's to be added under its raw name.
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
							Sequence: sourceKeyspace.KeyspaceName + ".my-seq1",
						},
					},
					registerInVSchema: true,
				},
			},
		},
		{
			name:          "qualified sequence missing from vschema",
			sourceVSchema: &vschema.Keyspace{},
			targetVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					table: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: fmt.Sprintf("`%s`.`my-seq1`", globalKeyspace),
						},
					},
				},
			},
			want: map[string]*sequenceMetadata{
				"my-seq1": {
					backingTableName:     "my-seq1",
					backingTableKeyspace: globalKeyspace,
					backingTableDBName:   "vt_" + globalKeyspace,
					usingTableName:       unescapedTable,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: globalKeyspace + ".my-seq1",
						},
					},
					registerInVSchema: true,
				},
			},
		},
		{
			// The qualified reference names global-ks, so the same-named
			// sequence in the source keyspace, which the search for the
			// unqualified, missing one sees, must not replace it.
			name: "qualified and unqualified sequences",
			sourceVSchema: &vschema.Keyspace{
				Tables: map[string]*vschema.Table{
					"my-seq1": {Type: "sequence"},
				},
			},
			options: &vtctldatapb.WorkflowOptions{
				ShardedAutoIncrementHandling: vtctldatapb.ShardedAutoIncrementHandling_REPLACE,
				GlobalKeyspace:               sourceKeyspace.KeyspaceName,
			},
			targetVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					table: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: fmt.Sprintf("`%s`.`my-seq1`", globalKeyspace),
						},
					},
					table2: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: "my-seq2",
						},
					},
				},
			},
			want: map[string]*sequenceMetadata{
				"my-seq1": {
					backingTableName:     "my-seq1",
					backingTableKeyspace: globalKeyspace,
					backingTableDBName:   "vt_" + globalKeyspace,
					usingTableName:       unescapedTable,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: globalKeyspace + ".my-seq1",
						},
					},
					registerInVSchema: true,
				},
				"my-seq2": {
					backingTableName:     "my-seq2",
					backingTableKeyspace: sourceKeyspace.KeyspaceName,
					backingTableDBName:   "vt_" + sourceKeyspace.KeyspaceName,
					usingTableName:       table2,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: "my-seq2",
						},
					},
					registerInVSchema:  true,
					createBackingTable: true,
				},
			},
		},
		{
			// VTGate uses the table names in a vschema as they are, so the
			// search doesn't find my-seq1 in the entry whose name is in
			// backticks, and both sequences are to be created.
			name: "escaped and missing unqualified sequences",
			sourceVSchema: &vschema.Keyspace{
				Tables: map[string]*vschema.Table{
					"`my-seq1`": {Type: "sequence"},
				},
			},
			options: &vtctldatapb.WorkflowOptions{
				ShardedAutoIncrementHandling: vtctldatapb.ShardedAutoIncrementHandling_REPLACE,
				GlobalKeyspace:               sourceKeyspace.KeyspaceName,
			},
			targetVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					table: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: "my-seq1",
						},
					},
					table2: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: "my-seq2",
						},
					},
				},
			},
			want: map[string]*sequenceMetadata{
				"my-seq1": {
					backingTableName:     "my-seq1",
					backingTableKeyspace: sourceKeyspace.KeyspaceName,
					backingTableDBName:   "vt_" + sourceKeyspace.KeyspaceName,
					usingTableName:       unescapedTable,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: "my-seq1",
						},
					},
					registerInVSchema:  true,
					createBackingTable: true,
				},
				"my-seq2": {
					backingTableName:     "my-seq2",
					backingTableKeyspace: sourceKeyspace.KeyspaceName,
					backingTableDBName:   "vt_" + sourceKeyspace.KeyspaceName,
					usingTableName:       table2,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: "my-seq2",
						},
					},
					registerInVSchema:  true,
					createBackingTable: true,
				},
			},
		},
		{
			name: "missing sequence that the global keyspace defines as a table",
			globalVSchema: &vschema.Keyspace{
				Tables: map[string]*vschema.Table{
					"my-seq1": {},
				},
			},
			options: &vtctldatapb.WorkflowOptions{
				ShardedAutoIncrementHandling: vtctldatapb.ShardedAutoIncrementHandling_REPLACE,
				GlobalKeyspace:               globalKeyspace,
			},
			targetVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					table: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: "my-seq1",
						},
					},
				},
			},
			err:     "table global-ks.my-seq1 is not a sequence in vschema; correct the backing table definition or the target auto_increment sequence reference",
			errCode: vtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			name: "qualified sequence in a keyspace that doesn't exist",
			targetVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					table: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: "`nonexistent-ks`.`my-seq1`",
						},
					},
				},
			},
			err: "failed to get vschema for sequence keyspace nonexistent-ks: node doesn't exist: keyspaces/nonexistent-ks/VSchema",
		},
		{
			name: "qualified sequence that is not a sequence",
			globalVSchema: &vschema.Keyspace{
				Tables: map[string]*vschema.Table{
					"my-seq1": {},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					table: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: fmt.Sprintf("`%s`.`my-seq1`", globalKeyspace),
						},
					},
				},
			},
			err:     "table global-ks.my-seq1 is not a sequence in vschema; correct the backing table definition or the target auto_increment sequence reference",
			errCode: vtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			// The sharded keyspace has one shard, so only the missing
			// sequence is refused.
			name: "qualified sequence in a sharded keyspace",
			globalVSchema: &vschema.Keyspace{
				Sharded: true,
			},
			targetVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					table: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: fmt.Sprintf("`%s`.`my-seq1`", globalKeyspace),
						},
					},
				},
			},
			err:     "sharded keyspace global-ks doesn't define sequence my-seq1 in its vschema; define it there as a pinned sequence, or update the target auto_increment sequence reference to an unsharded keyspace",
			errCode: vtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			// VSchema table names are raw, so an unrelated table's name can
			// contain a backtick.
			name: "qualified sequence next to a table whose name has a backtick",
			globalVSchema: &vschema.Keyspace{
				Tables: map[string]*vschema.Table{
					"my-seq1":  {Type: "sequence"},
					"odd`name": {},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					table: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: fmt.Sprintf("`%s`.`my-seq1`", globalKeyspace),
						},
					},
				},
			},
			want: map[string]*sequenceMetadata{
				"my-seq1": {
					backingTableName:     "my-seq1",
					backingTableKeyspace: globalKeyspace,
					backingTableDBName:   "vt_" + globalKeyspace,
					usingTableName:       unescapedTable,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: globalKeyspace + ".my-seq1",
						},
					},
				},
			},
		},
		{
			// A sharded keyspace with one shard can hold a pinned sequence,
			// which is left as it is.
			name: "qualified pinned sequence in a sharded keyspace",
			globalVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					"my-seq1": {Type: "sequence", Pinned: "00"},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					table: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: fmt.Sprintf("`%s`.`my-seq1`", globalKeyspace),
						},
					},
				},
			},
			want: map[string]*sequenceMetadata{
				"my-seq1": {
					backingTableName:     "my-seq1",
					backingTableKeyspace: globalKeyspace,
					backingTableDBName:   "vt_" + globalKeyspace,
					usingTableName:       unescapedTable,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: globalKeyspace + ".my-seq1",
						},
					},
				},
			},
		},
		{
			// Initializing the sequence reads and updates the backing table on
			// the only shard's primary, so a pinned sequence in a keyspace with
			// two shards is refused before writes are stopped.
			name: "qualified pinned sequence in a keyspace with two shards",
			targetVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					"my-seq1": {Type: "sequence", Pinned: "00"},
					table: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: fmt.Sprintf("`%s`.`my-seq1`", targetKeyspace.KeyspaceName),
						},
					},
				},
			},
			err:     "sequence keyspace targetks has 2 shards, but initializing its sequences needs it to have exactly one; update the target auto_increment sequence references to a keyspace with one shard",
			errCode: vtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			// Initializing the sequence updates the backing table on the
			// primary, so a keyspace without one is refused before writes are
			// stopped.
			name: "qualified sequence in a keyspace with no primary",
			targetVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					table: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: fmt.Sprintf("`%s`.`my-seq1`", noPrimaryKeyspace),
						},
					},
				},
			},
			err:     "sequence keyspace no-primary-ks does not currently have a primary tablet",
			errCode: vtrpcpb.Code_FAILED_PRECONDITION,
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
					registerInVSchema:    true,
					createBackingTable:   true,
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
			// The target vschema's names are in backticks. VSchema table names
			// are raw, so a table's name in another keyspace that's searched
			// can contain a backtick.
			name: "sequences with backticks",
			sourceVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					"my-seq1": {
						Type: "sequence",
					},
				},
			},
			globalVSchema: &vschema.Keyspace{
				Tables: map[string]*vschema.Table{
					"odd`name": {},
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
							Sequence: sourceKeyspace.KeyspaceName + ".seq1",
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
							Sequence: sourceKeyspace.KeyspaceName + ".seq2",
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
							Sequence: sourceKeyspace.KeyspaceName + ".seq1",
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
							Sequence: sourceKeyspace.KeyspaceName + ".seq2",
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
							Sequence: sourceKeyspace.KeyspaceName + ".seq1",
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
							Sequence: sourceKeyspace.KeyspaceName + ".seq1",
						},
					},
				},
			},
		},
		{
			// VTGate uses the table names in a vschema as they are, so the
			// entry whose name is in backticks doesn't define my-seq1, and
			// there's no global keyspace to create it in.
			name: "unqualified sequence whose vschema name is in backticks",
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
			err: "failed to locate all of the backing sequence tables being used and no global-keyspace was provided to auto create them in: my-seq1",
		},
		{
			// The tablet manager would initialize another table than this
			// one, whose name starts and ends with a backtick, so it's
			// refused even though the vschema defines it.
			name: "unqualified sequence whose name has backticks",
			sourceVSchema: &vschema.Keyspace{
				Tables: map[string]*vschema.Table{
					"`my-seq1`": {Type: "sequence"},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					table: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: "```my-seq1```",
						},
					},
				},
			},
			err:     "sequence table name \"`my-seq1`\" used by table t1 contains a backtick, which initializing target sequences doesn't support; rename the sequence table, or switch traffic without initializing target sequences",
			errCode: vtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			// Initializing this one would fail after writes are stopped, so
			// it's refused even though the vschema defines it.
			name: "qualified sequence whose name has a backtick",
			globalVSchema: &vschema.Keyspace{
				Tables: map[string]*vschema.Table{
					"my`seq1": {Type: "sequence"},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Sharded: true,
				Tables: map[string]*vschema.Table{
					table: {
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "my-col",
							Sequence: fmt.Sprintf("`%s`.`my``seq1`", globalKeyspace),
						},
					},
				},
			},
			err:     "sequence table name \"my`seq1\" used by table t1 contains a backtick, which initializing target sequences doesn't support; rename the sequence table, or switch traffic without initializing target sequences",
			errCode: vtrpcpb.Code_FAILED_PRECONDITION,
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
			globalVSchema := tc.globalVSchema
			if globalVSchema == nil {
				globalVSchema = &vschema.Keyspace{}
			}
			err = env.ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
				Name:     globalKeyspace,
				Keyspace: globalVSchema,
			})
			require.NoError(t, err)
			err = env.ts.RebuildSrvVSchema(ctx, nil)
			require.NoError(t, err)
			vschemas := func() map[string]*vschema.Keyspace {
				vschemas := make(map[string]*vschema.Keyspace)
				for _, keyspace := range []string{sourceKeyspace.KeyspaceName, targetKeyspace.KeyspaceName, globalKeyspace} {
					kvs, err := env.ts.GetVSchema(ctx, keyspace)
					require.NoError(t, err)
					vschemas[keyspace] = kvs.Keyspace
				}
				return vschemas
			}
			vschemasBefore := vschemas()
			sources := make(map[string]*MigrationSource, len(sourceKeyspace.ShardNames))
			targets := make(map[string]*MigrationTarget, len(targetKeyspace.ShardNames))
			for i, shard := range sourceKeyspace.ShardNames {
				tablet := env.tablets[sourceKeyspace.KeyspaceName][startingSourceTabletUID+(i*tabletUIDStep)]
				sources[shard] = &MigrationSource{
					primary: &topo.TabletInfo{
						Tablet: tablet,
					},
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
				if tc.errCode != vtrpcpb.Code_OK {
					assert.Equal(t, tc.errCode, vterrors.Code(err))
				}
			} else {
				require.NoError(t, err)
			}
			require.EqualValues(t, tc.want, got)
			// This also runs on dry runs, so it changes no vschema, and it
			// creates no backing table: the test tablet manager client
			// fails an unexpected ApplySchema.
			assert.Equal(t, vschemasBefore, vschemas())
		})
	}
}

// TestDryRunInitializeTargetSequences validates that we get the max value of the using tables and initialize the backing
// tables to the next value.
func TestDryRunInitializeTargetSequences(t *testing.T) {
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
	drLog := NewLogRecorder()
	dr := &switcherDryRun{
		drLog: drLog,
		ts:    ts,
	}

	// The metadata is keyed by backing table, as getTargetSequenceMetadata
	// keys it. seq1 is in the source keyspace, seq2 in another keyspace, and
	// seq3 is created in a global keyspace and added to its vschema. Each
	// keyspace's primary reads its own sequence.
	otherKeyspaceName := "other-ks"
	globalKeyspaceName := "global-ks"
	env.addTablet(t, ctx, 300, otherKeyspaceName, "0", topodatapb.TabletType_PRIMARY, true)
	env.addTablet(t, ctx, 400, globalKeyspaceName, "0", topodatapb.TabletType_PRIMARY, true)
	newSequence := func(backingTable, backingKeyspace, usingTable string) *sequenceMetadata {
		return &sequenceMetadata{
			backingTableName:     backingTable,
			backingTableKeyspace: backingKeyspace,
			backingTableDBName:   "vt_" + backingKeyspace,
			usingTableName:       usingTable,
			usingTableDBName:     "vt_" + targetKeyspaceName,
			usingTableDefinition: &vschema.Table{
				AutoIncrement: &vschema.AutoIncrement{Column: "id", Sequence: backingTable},
			},
		}
	}
	backingTables := []string{"seq1", "seq2", "seq3"}
	tables := map[string]*sequenceMetadata{
		"seq1": newSequence("seq1", sourceKeyspaceName, "t1"),
		"seq2": newSequence("seq2", otherKeyspaceName, "t2"),
		"seq3": newSequence("seq3", globalKeyspaceName, "t3"),
	}
	tables["seq3"].registerInVSchema = true
	tables["seq3"].createBackingTable = true
	vschemasBefore := make(map[string]*vschema.Keyspace)
	for _, keyspace := range []string{sourceKeyspaceName, otherKeyspaceName, globalKeyspaceName} {
		kvs, err := env.ts.GetVSchema(ctx, keyspace)
		require.NoError(t, err)
		vschemasBefore[keyspace] = kvs.Keyspace
	}

	// The target reports a different maximum for each sequence. seq1 is at
	// 4, seq2 at 6, and seq3 doesn't exist yet.
	maxValues := map[string]int64{"seq1": 10, "seq2": 20, "seq3": 30}
	var requested []*tabletmanagerdatapb.GetMaxValueForSequencesRequest_SequenceMetadata
	for _, backingTable := range backingTables {
		requested = append(requested, &tabletmanagerdatapb.GetMaxValueForSequencesRequest_SequenceMetadata{
			BackingTableName:        backingTable,
			UsingColEscaped:         "`id`",
			UsingTableNameEscaped:   fmt.Sprintf("`%s`", tables[backingTable].usingTableName),
			UsingTableDbNameEscaped: fmt.Sprintf("`vt_%s`", targetKeyspaceName),
		})
	}
	env.tmc.expectGetMaxValueForSequencesRequest(startingTargetTabletUID, &getMaxValueForSequencesRequestResponse{
		req: &tabletmanagerdatapb.GetMaxValueForSequencesRequest{Sequences: requested},
		res: &tabletmanagerdatapb.GetMaxValueForSequencesResponse{MaxValuesBySequenceTable: maxValues},
	})
	env.tmc.expectVRQuery(startingSourceTabletUID, "select next_id from `vt_sourceks`.`seq1` where id = 0",
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("next_id", "int64"), "4"))
	env.tmc.expectVRQuery(300, "select next_id from `vt_other-ks`.`seq2` where id = 0",
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("next_id", "int64"), "6"))
	env.tmc.expectVRQueryResultOnKeyspaceTablets(globalKeyspaceName, &queryResult{
		query: "select next_id from `vt_global-ks`.`seq3` where id = 0",
		err:   sqlerror.NewSQLError(sqlerror.ERNoSuchTable, sqlerror.SSUnknownTable, "Table 'vt_global-ks.seq3' doesn't exist"),
	})

	err = dr.prepareTargetSequences(ctx, tables)
	require.NoError(t, err)
	err = dr.initializeTargetSequences(ctx, tables)
	require.NoError(t, err)
	require.Equal(t, []string{
		"Sequence backing tables [global-ks.seq3] will be created",
		"Sequence backing tables [global-ks.seq3] will be added to the vschema of their keyspace",
		"The following sequence backing tables used by tables being moved will be initialized:",
		"\tBacking table: seq1, current value 4, new value 11",
		"\tBacking table: seq2, current value 6, new value 21",
		"\tBacking table: seq3, will be created, new value 31",
	}, drLog.GetLogs())
	func() {
		env.tmc.mu.Lock()
		defer env.tmc.mu.Unlock()
		assert.Empty(t, env.tmc.getMaxValueForSequencesRequests, "the maxima weren't read")
		for tabletID, queries := range env.tmc.vrQueries {
			assert.Empty(t, queries, "queries not run on tablet %d", tabletID)
		}
	}()
	for keyspace, before := range vschemasBefore {
		kvs, err := env.ts.GetVSchema(ctx, keyspace)
		require.NoError(t, err)
		assert.Equal(t, before, kvs.Keyspace, "a dry run changed the vschema of %s", keyspace)
	}

	// A backing table that doesn't exist yet, as on a dry run nothing
	// creates it, is reported as one that initializing the sequences
	// creates.
	sm4 := sequenceMetadata{
		backingTableName:     "seq4",
		backingTableKeyspace: "sourceks",
		backingTableDBName:   "ks1",
		usingTableName:       "t4",
		usingTableDBName:     "targetks",
		usingTableDefinition: &vschema.Table{
			AutoIncrement: &vschema.AutoIncrement{Column: "id", Sequence: "seq4"},
		},
	}
	env.tmc.expectVRQueryResultOnKeyspaceTablets(sourceKeyspaceName, &queryResult{
		query: "/select next_id from `ks1`.`seq4`.*",
		err:   sqlerror.NewSQLError(sqlerror.ERNoSuchTable, sqlerror.SSUnknownTable, "Table 'ks1.seq4' doesn't exist"),
	})
	drLog = NewLogRecorder()
	dr.drLog = drLog
	err = dr.initializeTargetSequences(ctx, map[string]*sequenceMetadata{"seq4": &sm4})
	require.NoError(t, err)
	require.Len(t, drLog.logs, 2)
	require.Contains(t, drLog.logs[1], "Backing table: seq4, will be created, new value 1")

	// A backing table whose tablespace is missing (ER_TABLESPACE_MISSING,
	// 1812) still exists, so the dry run fails rather than reporting that it will
	// be created.
	sm5 := sequenceMetadata{
		backingTableName:     "seq5",
		backingTableKeyspace: "sourceks",
		backingTableDBName:   "ks1",
		usingTableName:       "t5",
		usingTableDBName:     "targetks",
		usingTableDefinition: &vschema.Table{
			AutoIncrement: &vschema.AutoIncrement{Column: "id", Sequence: "seq5"},
		},
	}
	env.tmc.expectVRQueryResultOnKeyspaceTablets(sourceKeyspaceName, &queryResult{
		query: "/select next_id from `ks1`.`seq5`.*",
		err:   sqlerror.NewSQLError(sqlerror.ErrorCode(1812), sqlerror.SSUnknownSQLState, "Tablespace is missing for table `ks1`.`seq5`."),
	})
	err = dr.initializeTargetSequences(ctx, map[string]*sequenceMetadata{"seq5": &sm5})
	require.ErrorContains(t, err, "Tablespace is missing for table `ks1`.`seq5`")
}

// TestDryRunInitializeTargetSequencesDbNameOverride validates that the dry run
// reads the current sequence value from the db name that the tablet serving
// the sequence actually uses (its db name override) rather than the default
// vt_<keyspace> name, even when the sequence metadata was already escaped by
// an earlier step (getting the max values).
func TestDryRunInitializeTargetSequencesDbNameOverride(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"
	sourceKeyspaceName := "sourceks"
	targetKeyspaceName := "targetks"
	dbNameOverride := "sourceks" // e.g. init_db_name_override=<keyspace>

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

	// The source primary (which serves the sequence) uses a db name override.
	_, err := env.ts.UpdateTabletFields(ctx, env.tablets[sourceKeyspaceName][startingSourceTabletUID].Alias, func(tablet *topodatapb.Tablet) error {
		tablet.DbNameOverride = dbNameOverride
		return nil
	})
	require.NoError(t, err)

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspaceName, workflowName)
	require.NoError(t, err)
	drLog := NewLogRecorder()
	dr := &switcherDryRun{
		drLog: drLog,
		ts:    ts,
	}

	sm1 := sequenceMetadata{
		backingTableName:     "seq1",
		backingTableKeyspace: sourceKeyspaceName,
		backingTableDBName:   "vt_" + sourceKeyspaceName, // The default, which does not exist on the tablet
		usingTableName:       tableName,
		usingTableDBName:     targetKeyspaceName,
		usingTableDefinition: &vschema.Table{
			AutoIncrement: &vschema.AutoIncrement{Column: "id", Sequence: "seq1"},
		},
	}
	tables := map[string]*sequenceMetadata{
		"seq1": &sm1,
	}

	env.tmc.expectVRQuery(startingTargetTabletUID, "/select max.*", sqltypes.MakeTestResult(sqltypes.MakeTestFields("maxval", "int64"), "10"))
	// The query must use the db name override, not vt_<keyspace>.
	env.tmc.expectVRQuery(startingSourceTabletUID, fmt.Sprintf("select next_id from `%s`.`seq1` where id = 0", dbNameOverride),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("next_id", "int64"), "5"))

	err = dr.initializeTargetSequences(ctx, tables)
	require.NoError(t, err)
	require.Equal(t, dbNameOverride, sm1.backingTableDBName)
	require.Len(t, drLog.logs, 2)
	require.Contains(t, drLog.logs[1], "Backing table: seq1, current value 5")
}
