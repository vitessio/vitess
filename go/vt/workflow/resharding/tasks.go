/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resharding

import (
	"fmt"
	"log"
	"strings"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/automation"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/workflow"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	workflowpb "vitess.io/vitess/go/vt/proto/workflow"
)

func createTaskID(phase workflow.PhaseType, shardName string) string {
	return fmt.Sprintf("%s/%s", phase, shardName)
}

// GetTasks returns selected tasks for a phase from the checkpoint
// with expected execution order.
func (hw *horizontalReshardingWorkflow) GetTasks(phase workflow.PhaseType) []*workflowpb.Task {
	var shards []string
	switch phase {
	case phaseCopySchema, phaseWaitForFilteredReplication, phaseDiff:
		shards = strings.Split(hw.checkpoint.Settings["destination_shards"], ",")
	case phaseClone, phaseMigrateRdonly, phaseMigrateReplica, phaseMigrateMaster:
		shards = strings.Split(hw.checkpoint.Settings["source_shards"], ",")
	default:
		log.Fatalf("BUG: unknown phase type: %v", phase)
	}

	var tasks []*workflowpb.Task
	for _, s := range shards {
		taskID := createTaskID(phase, s)
		tasks = append(tasks, hw.checkpoint.Tasks[taskID])
	}
	return tasks
}

func (hw *horizontalReshardingWorkflow) runCopySchema(ctx context.Context, t *workflowpb.Task) error {
	keyspace := t.Attributes["keyspace"]
	sourceShard := t.Attributes["source_shard"]
	destShard := t.Attributes["destination_shard"]
	return hw.wr.CopySchemaShardFromShard(ctx, nil /* tableArray*/, nil /* excludeTableArray */, true, /*includeViews*/
		keyspace, sourceShard, keyspace, destShard, wrangler.DefaultWaitSlaveTimeout)
}

func (hw *horizontalReshardingWorkflow) runSplitClone(ctx context.Context, t *workflowpb.Task) error {
	keyspace := t.Attributes["keyspace"]
	sourceShard := t.Attributes["source_shard"]
	worker := t.Attributes["vtworker"]
	minHealthyRdonlyTablets := t.Attributes["min_healthy_rdonly_tablets"]
	splitCmd := t.Attributes["split_cmd"]

	sourceKeyspaceShard := topoproto.KeyspaceShardString(keyspace, sourceShard)
	// Reset the vtworker to avoid error if vtworker command has been called elsewhere.
	// This is because vtworker class doesn't cleanup the environment after execution.
	if _, err := automation.ExecuteVtworker(ctx, worker, []string{"Reset"}); err != nil {
		return err
	}

	args := []string{splitCmd, "--min_healthy_rdonly_tablets=" + minHealthyRdonlyTablets, sourceKeyspaceShard}
	_, err := automation.ExecuteVtworker(hw.ctx, worker, args)
	return err
}

func (hw *horizontalReshardingWorkflow) runWaitForFilteredReplication(ctx context.Context, t *workflowpb.Task) error {
	keyspace := t.Attributes["keyspace"]
	destShard := t.Attributes["destination_shard"]
	return hw.wr.WaitForFilteredReplication(ctx, keyspace, destShard, wrangler.DefaultWaitForFilteredReplicationMaxDelay)
}

func (hw *horizontalReshardingWorkflow) runSplitDiff(ctx context.Context, t *workflowpb.Task) error {
	keyspace := t.Attributes["keyspace"]
	destShard := t.Attributes["destination_shard"]
	destinationTabletType := t.Attributes["dest_tablet_type"]
	worker := t.Attributes["vtworker"]

	if _, err := automation.ExecuteVtworker(hw.ctx, worker, []string{"Reset"}); err != nil {
		return err
	}
	args := []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", "--dest_tablet_type=" + destinationTabletType, topoproto.KeyspaceShardString(keyspace, destShard)}
	_, err := automation.ExecuteVtworker(ctx, worker, args)
	return err
}

func (hw *horizontalReshardingWorkflow) runMigrate(ctx context.Context, t *workflowpb.Task) error {
	keyspace := t.Attributes["keyspace"]
	sourceShard := t.Attributes["source_shard"]
	servedTypeStr := t.Attributes["served_type"]

	servedType, err := topoproto.ParseTabletType(servedTypeStr)
	if err != nil {
		return fmt.Errorf("unknown tablet type: %v", servedTypeStr)
	}

	if servedType != topodatapb.TabletType_RDONLY &&
		servedType != topodatapb.TabletType_REPLICA &&
		servedType != topodatapb.TabletType_MASTER {
		return fmt.Errorf("wrong served type to be migrated: %v", servedTypeStr)
	}

	return hw.wr.MigrateServedTypes(ctx, keyspace, sourceShard, servedType, false /* reverse */, false /* skipReFreshState */, wrangler.DefaultFilteredReplicationWaitTime, false /* reverseReplication */)
}
