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

package workflow

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestDropTargetVReplicationStreams(t *testing.T) {
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
		ShardNames:   []string{"-80", "80-"},
	}

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspaceName, workflowName)
	require.NoError(t, err)

	drLog := NewLogRecorder()
	dr := switcherDryRun{
		ts:    ts,
		drLog: drLog,
	}

	err = dr.dropTargetVReplicationStreams(ctx)
	require.NoError(t, err)
	require.Len(t, drLog.logs, 1)
	log := drLog.logs[0]

	// Make sure both the target streams are included in the logs
	assert.Contains(t, log, "-80")
	assert.Contains(t, log, "80-")
}

func TestStartReverseVReplication(t *testing.T) {
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
		ShardNames:   []string{"-80", "80-"},
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
	dr := switcherDryRun{
		ts:    ts,
		drLog: drLog,
	}

	err = dr.startReverseVReplication(ctx)
	require.NoError(t, err)
	require.Len(t, drLog.logs, 1)
	log := drLog.logs[0]

	// Make sure both the source tablets are included in the logs
	assert.Contains(t, log, "tablet:100")
	assert.Contains(t, log, "tablet:110")
}

func TestRemoveSourceTables(t *testing.T) {
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
		ShardNames:   []string{"-80", "80-"},
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
	dr := switcherDryRun{
		ts:    ts,
		drLog: drLog,
	}

	err = dr.removeSourceTables(ctx, RenameTable)
	require.NoError(t, err)
	require.Len(t, drLog.logs, 1)
	log := drLog.logs[0]

	assert.Contains(t, log, "Renaming")
	// Make sure both the source tablets are included in the logs
	assert.Contains(t, log, "tablet:100")
	assert.Contains(t, log, "tablet:110")

	err = dr.removeSourceTables(ctx, DropTable)
	require.NoError(t, err)
	require.Len(t, drLog.logs, 2)
	log = drLog.logs[1]

	assert.Contains(t, log, "Dropping")
	// Make sure both the source tablets are included in the logs
	assert.Contains(t, log, "tablet:100")
	assert.Contains(t, log, "tablet:110")
}

func TestDropShards(t *testing.T) {
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
		ShardNames:   []string{"-80", "80-"},
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
	dr := switcherDryRun{
		ts:    ts,
		drLog: drLog,
	}

	err = dr.dropSourceShards(ctx)
	require.NoError(t, err)
	require.Len(t, drLog.logs, 1)
	log := drLog.logs[0]

	// Make sure both the source shards are included in the logs
	assert.Contains(t, log, "[-80]")
	assert.Contains(t, log, "[80-]")

	err = dr.dropTargetShards(ctx)
	require.NoError(t, err)
	require.Len(t, drLog.logs, 2)
	log = drLog.logs[1]
	assert.Contains(t, log, "[0]")
}

func TestDropSourceReverseVReplicationStreams(t *testing.T) {
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
		ShardNames:   []string{"-80", "80-"},
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
	dr := switcherDryRun{
		ts:    ts,
		drLog: drLog,
	}

	err = dr.dropSourceReverseVReplicationStreams(ctx)
	require.NoError(t, err)
	require.Len(t, drLog.logs, 1)
	log := drLog.logs[0]

	// Make sure both the source streams are included in the logs
	assert.Contains(t, log, "-80")
	assert.Contains(t, log, "80-")
	assert.Contains(t, log, fmt.Sprintf("%s_reverse", workflowName))
}

func TestDropSourceDeniedTables(t *testing.T) {
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
		ShardNames:   []string{"-80", "80-"},
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
	dr := switcherDryRun{
		ts:    ts,
		drLog: drLog,
	}

	err = dr.dropSourceDeniedTables(ctx)
	require.NoError(t, err)
	require.Len(t, drLog.logs, 1)
	log := drLog.logs[0]

	// Make sure both the source streams are included in the logs
	assert.Contains(t, log, "-80")
	assert.Contains(t, log, "80-")
	// Make sure both the tables are included in the logs
	assert.Contains(t, log, "t1")
	assert.Contains(t, log, "t2")
}

func TestDropTargetDeniedTables(t *testing.T) {
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
		ShardNames:   []string{"-80", "80-"},
	}

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspaceName, workflowName)
	require.NoError(t, err)

	drLog := NewLogRecorder()
	dr := switcherDryRun{
		ts:    ts,
		drLog: drLog,
	}

	err = dr.dropTargetDeniedTables(ctx)
	require.NoError(t, err)
	require.Len(t, drLog.logs, 1)
	log := drLog.logs[0]

	// Make sure both the target streams are included in the logs
	assert.Contains(t, log, "-80")
	assert.Contains(t, log, "80-")
	// Make sure both the tables are included in the logs
	assert.Contains(t, log, "t1")
	assert.Contains(t, log, "t2")
}

func TestRemoveTargetTables(t *testing.T) {
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
		ShardNames:   []string{"-80", "80-"},
	}

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspaceName, workflowName)
	require.NoError(t, err)

	drLog := NewLogRecorder()
	dr := switcherDryRun{
		ts:    ts,
		drLog: drLog,
	}

	err = dr.removeTargetTables(ctx)
	require.NoError(t, err)
	require.Len(t, drLog.logs, 1)
	log := drLog.logs[0]

	assert.Contains(t, log, targetKeyspaceName)
	// Make sure both the target streams are included in the logs
	assert.Contains(t, log, "-80")
	assert.Contains(t, log, "80-")
	// Make sure both the tables are included in the logs
	assert.Contains(t, log, "t1")
	assert.Contains(t, log, "t2")
}

func TestSwitchKeyspaceReads(t *testing.T) {
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
		ShardNames:   []string{"-80", "80-"},
	}

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspaceName, workflowName)
	require.NoError(t, err)

	drLog := NewLogRecorder()
	dr := switcherDryRun{
		ts:    ts,
		drLog: drLog,
	}

	err = dr.switchKeyspaceReads(ctx, []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_RDONLY})
	require.NoError(t, err)
	require.Len(t, drLog.logs, 1)
	log := drLog.logs[0]
	assert.Contains(t, log, fmt.Sprintf("keyspace %s to keyspace %s", sourceKeyspaceName, targetKeyspaceName))
	assert.Contains(t, log, "PRIMARY")
	assert.Contains(t, log, "RDONLY")
}

func TestSwitchShardReads(t *testing.T) {
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
		ShardNames:   []string{"-80", "80-"},
	}

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspaceName, workflowName)
	require.NoError(t, err)

	drLog := NewLogRecorder()
	dr := switcherDryRun{
		ts:    ts,
		drLog: drLog,
	}

	err = dr.switchShardReads(ctx, nil, []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_RDONLY}, DirectionForward)
	require.NoError(t, err)
	require.Len(t, drLog.logs, 1)
	log := drLog.logs[0]
	assert.Contains(t, log, fmt.Sprintf("keyspace %s to keyspace %s", sourceKeyspaceName, targetKeyspaceName))
	assert.Contains(t, log, "-80")
	assert.Contains(t, log, "80-")
	assert.Contains(t, log, "[0]")

	err = dr.switchShardReads(ctx, nil, []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_RDONLY}, DirectionBackward)
	require.NoError(t, err)
	require.Len(t, drLog.logs, 2)
	log = drLog.logs[1]
	// Ensure the reverse direction is logged.
	assert.Contains(t, log, fmt.Sprintf("keyspace %s to keyspace %s", targetKeyspaceName, sourceKeyspaceName))
	assert.Contains(t, log, "-80")
	assert.Contains(t, log, "80-")
	assert.Contains(t, log, "[0]")
}

func TestChangeRouting(t *testing.T) {
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
		ShardNames:   []string{"-80", "80-"},
	}

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspaceName, workflowName)
	require.NoError(t, err)

	drLog := NewLogRecorder()
	dr := switcherDryRun{
		ts:    ts,
		drLog: drLog,
	}

	ts.migrationType = binlogdatapb.MigrationType_TABLES
	err = dr.changeRouting(ctx)
	require.NoError(t, err)
	assert.Len(t, drLog.logs, 2)
	assert.Contains(t, drLog.logs[0], fmt.Sprintf("keyspace %s to keyspace %s", sourceKeyspaceName, targetKeyspaceName))
	assert.Contains(t, drLog.logs[1], "t1")
	assert.Contains(t, drLog.logs[1], "t2")

	ts.migrationType = binlogdatapb.MigrationType_SHARDS
	err = dr.changeRouting(ctx)
	require.NoError(t, err)
	assert.Len(t, drLog.logs, 5)
	assert.Contains(t, drLog.logs[3], "false")
	assert.Contains(t, drLog.logs[3], "shard:0")
	assert.Contains(t, drLog.logs[4], "true")
	assert.Contains(t, drLog.logs[4], "shard:-80")
	assert.Contains(t, drLog.logs[4], "shard:80-")
}

func TestDRInitializeTargetSequences(t *testing.T) {
	ctx := context.Background()
	drLog := NewLogRecorder()
	dr := &switcherDryRun{
		drLog: drLog,
	}

	tables := map[string]*sequenceMetadata{
		"t1": nil,
		"t2": nil,
		"t3": nil,
	}
	err := dr.initializeTargetSequences(ctx, tables)
	require.NoError(t, err)
	assert.Len(t, drLog.logs, 1)
	assert.Contains(t, drLog.logs[0], "t1")
	assert.Contains(t, drLog.logs[0], "t2")
	assert.Contains(t, drLog.logs[0], "t3")
}
