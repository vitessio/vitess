// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"strings"

	automationpb "github.com/youtube/vitess/go/vt/proto/automation"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
)

// VerticalSplitTask is a cluster operation to split out specific tables of one
// keyspace to a different keyspace.
type VerticalSplitTask struct {
}

// Run is part of the Task interface.
func (t *VerticalSplitTask) Run(parameters map[string]string) ([]*automationpb.TaskContainer, string, error) {
	// Required parameters.
	// Example: source_keyspace
	sourceKeyspace := parameters["source_keyspace"]
	// Example: destination_keyspace
	destKeyspace := parameters["dest_keyspace"]
	// Example: 10-20
	shards := strings.Split(parameters["shard_list"], ",")
	// Example: table1,table2
	tables := parameters["tables"]
	// Example: localhost:15000
	vtctldEndpoint := parameters["vtctld_endpoint"]
	// Example: localhost:15001
	vtworkerEndpoint := parameters["vtworker_endpoint"]

	// Optional parameters.
	// Example: 1
	minHealthyRdonlyTablets := parameters["min_healthy_rdonly_tablets"]

	var newTasks []*automationpb.TaskContainer
	copySchemaTasks := NewTaskContainer()
	for _, shard := range shards {
		AddTask(copySchemaTasks, "CopySchemaShardTask", map[string]string{
			"source_keyspace_and_shard": topoproto.KeyspaceShardString(sourceKeyspace, shard),
			"dest_keyspace_and_shard":   topoproto.KeyspaceShardString(destKeyspace, shard),
			"vtctld_endpoint":           vtctldEndpoint,
			"tables":                    tables,
		})
	}
	newTasks = append(newTasks, copySchemaTasks)

	vSplitCloneTasks := NewTaskContainer()
	for _, shard := range shards {
		// TODO(mberlin): Add a semaphore as argument to limit the parallism.
		AddTask(vSplitCloneTasks, "VerticalSplitCloneTask", map[string]string{
			"dest_keyspace":              destKeyspace,
			"shard":                      shard,
			"tables":                     tables,
			"vtworker_endpoint":          vtworkerEndpoint,
			"min_healthy_rdonly_tablets": minHealthyRdonlyTablets,
		})
	}
	newTasks = append(newTasks, vSplitCloneTasks)

	// TODO(mberlin): When the framework supports nesting tasks, these wait tasks should be run before each SplitDiff.
	waitTasks := NewTaskContainer()
	for _, shard := range shards {
		AddTask(waitTasks, "WaitForFilteredReplicationTask", map[string]string{
			"keyspace":        destKeyspace,
			"shard":           shard,
			"max_delay":       "30s",
			"vtctld_endpoint": vtctldEndpoint,
		})
	}
	newTasks = append(newTasks, waitTasks)

	// TODO(mberlin): Run all SplitDiffTasks in parallel which do not use overlapping source shards for the comparison.
	for _, shard := range shards {
		vSplitDiffTask := NewTaskContainer()
		AddTask(vSplitDiffTask, "VerticalSplitDiffTask", map[string]string{
			"dest_keyspace":              destKeyspace,
			"shard":                      shard,
			"vtworker_endpoint":          vtworkerEndpoint,
			"min_healthy_rdonly_tablets": minHealthyRdonlyTablets,
		})
		newTasks = append(newTasks, vSplitDiffTask)
	}

	for _, servedType := range []string{"rdonly", "replica", "master"} {
		migrateServedTypesTasks := NewTaskContainer()
		for _, shard := range shards {
			AddTask(migrateServedTypesTasks, "MigrateServedFromTask", map[string]string{
				"dest_keyspace":   destKeyspace,
				"shard":           shard,
				"type":            servedType,
				"vtctld_endpoint": vtctldEndpoint,
			})
		}
		newTasks = append(newTasks, migrateServedTypesTasks)
	}

	return newTasks, "", nil
}

// RequiredParameters is part of the Task interface.
func (t *VerticalSplitTask) RequiredParameters() []string {
	return []string{"source_keyspace", "dest_keyspace", "shard_list",
		"tables", "vtctld_endpoint", "vtworker_endpoint"}
}

// OptionalParameters is part of the Task interface.
func (t *VerticalSplitTask) OptionalParameters() []string {
	return []string{"min_healthy_rdonly_tablets"}
}
