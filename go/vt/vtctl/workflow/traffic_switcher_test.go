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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
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

	err = ts.cancelMigration(ctx, sm)
	require.NoError(t, err)

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

	err = ts.cancelMigration(ctx, sm)
	require.NoError(t, err)

	// Expect the queries to be cleared
	assert.Empty(t, env.tmc.vrQueries[100])
	assert.Empty(t, env.tmc.vrQueries[200])
}
