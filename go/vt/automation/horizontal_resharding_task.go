// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"strings"

	automationpb "github.com/youtube/vitess/go/vt/proto/automation"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
)

// HorizontalReshardingTask is a cluster operation which allows to increase the number of shards.
type HorizontalReshardingTask struct {
}

// Run is part of the Task interface.
func (t *HorizontalReshardingTask) Run(parameters map[string]string) ([]*automationpb.TaskContainer, string, error) {
	// Required parameters.
	// Example: test_keyspace
	keyspace := parameters["keyspace"]
	// Example: 10-20
	sourceShards := strings.Split(parameters["source_shard_list"], ",")
	// Example: 10-18,18-20
	destShards := strings.Split(parameters["dest_shard_list"], ",")
	// Example: localhost:15000
	vtctldEndpoint := parameters["vtctld_endpoint"]
	// Example: localhost:15001
	vtworkerEndpoint := parameters["vtworker_endpoint"]

	// Optional parameters.
	// Example: unrelated1,unrelated2
	excludeTables := parameters["exclude_tables"]
	// Example: 1
	minHealthyRdonlyTablets := parameters["min_healthy_rdonly_tablets"]

	var newTasks []*automationpb.TaskContainer
	copySchemaTasks := NewTaskContainer()
	for _, destShard := range destShards {
		AddTask(copySchemaTasks, "CopySchemaShardTask", map[string]string{
			"source_keyspace_and_shard": topoproto.KeyspaceShardString(keyspace, sourceShards[0]),
			"dest_keyspace_and_shard":   topoproto.KeyspaceShardString(keyspace, destShard),
			"exclude_tables":            excludeTables,
			"vtctld_endpoint":           vtctldEndpoint,
		})
	}
	newTasks = append(newTasks, copySchemaTasks)

	splitCloneTasks := NewTaskContainer()
	for _, sourceShard := range sourceShards {
		// TODO(mberlin): Add a semaphore as argument to limit the parallism.
		AddTask(splitCloneTasks, "SplitCloneTask", map[string]string{
			"keyspace":                   keyspace,
			"source_shard":               sourceShard,
			"vtworker_endpoint":          vtworkerEndpoint,
			"exclude_tables":             excludeTables,
			"min_healthy_rdonly_tablets": minHealthyRdonlyTablets,
		})
	}
	newTasks = append(newTasks, splitCloneTasks)

	// TODO(mberlin): When the framework supports nesting tasks, these wait tasks should be run before each SplitDiff.
	waitTasks := NewTaskContainer()
	for _, destShard := range destShards {
		AddTask(waitTasks, "WaitForFilteredReplicationTask", map[string]string{
			"keyspace":        keyspace,
			"shard":           destShard,
			"max_delay":       "30s",
			"vtctld_endpoint": vtctldEndpoint,
		})
	}
	newTasks = append(newTasks, waitTasks)

	// TODO(mberlin): Run all SplitDiffTasks in parallel which do not use overlapping source shards for the comparison.
	for _, destShard := range destShards {
		splitDiffTask := NewTaskContainer()
		AddTask(splitDiffTask, "SplitDiffTask", map[string]string{
			"keyspace":                   keyspace,
			"dest_shard":                 destShard,
			"vtworker_endpoint":          vtworkerEndpoint,
			"exclude_tables":             excludeTables,
			"min_healthy_rdonly_tablets": minHealthyRdonlyTablets,
		})
		newTasks = append(newTasks, splitDiffTask)
	}

	for _, servedType := range []string{"rdonly", "replica", "master"} {
		migrateServedTypesTasks := NewTaskContainer()
		for _, sourceShard := range sourceShards {
			AddTask(migrateServedTypesTasks, "MigrateServedTypesTask", map[string]string{
				"keyspace":        keyspace,
				"source_shard":    sourceShard,
				"type":            servedType,
				"vtctld_endpoint": vtctldEndpoint,
			})
		}
		newTasks = append(newTasks, migrateServedTypesTasks)
	}

	return newTasks, "", nil
}

// RequiredParameters is part of the Task interface.
func (t *HorizontalReshardingTask) RequiredParameters() []string {
	return []string{"keyspace", "source_shard_list", "dest_shard_list",
		"vtctld_endpoint", "vtworker_endpoint"}
}

// OptionalParameters is part of the Task interface.
func (t *HorizontalReshardingTask) OptionalParameters() []string {
	return []string{"exclude_tables", "min_healthy_rdonly_tablets"}
}
