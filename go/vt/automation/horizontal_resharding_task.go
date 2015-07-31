// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"strings"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// HorizontalReshardingTask is a cluster operation which allows to increase the number of shards.
type HorizontalReshardingTask struct {
}

// Run is part of the Task interface.
func (t *HorizontalReshardingTask) Run(parameters map[string]string) ([]*pb.TaskContainer, string, error) {
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

	var newTasks []*pb.TaskContainer
	copySchemaTasks := NewTaskContainer()
	for _, destShard := range destShards {
		AddTask(copySchemaTasks, "CopySchemaShardTask", map[string]string{
			"keyspace":        keyspace,
			"source_shard":    sourceShards[0],
			"dest_shard":      destShard,
			"vtctld_endpoint": vtctldEndpoint,
		})
	}
	newTasks = append(newTasks, copySchemaTasks)

	splitCloneTasks := NewTaskContainer()
	for _, sourceShard := range sourceShards {
		// TODO(mberlin): Add a semaphore as argument to limit the parallism.
		AddTask(splitCloneTasks, "SplitCloneTask", map[string]string{
			"keyspace":          keyspace,
			"source_shard":      sourceShard,
			"vtworker_endpoint": vtworkerEndpoint,
		})
	}
	newTasks = append(newTasks, splitCloneTasks)

	for _, destShard := range destShards {
		splitDiffTask := NewTaskContainer()
		AddTask(splitDiffTask, "SplitDiffTask", map[string]string{
			"keyspace":          keyspace,
			"dest_shard":        destShard,
			"vtworker_endpoint": vtworkerEndpoint,
		})
		newTasks = append(newTasks, splitDiffTask)
	}

	// TODO(mberlin): Implement "MigrateServedTypes" task and uncomment this.
	//	for _, servedType := range []string{"rdonly", "replica", "master"} {
	//		migrateServedTypesTasks := NewTaskContainer()
	//		for _, sourceShard := range sourceShards {
	//			AddTask(migrateServedTypesTasks, "MigrateServedTypes", map[string]string{
	//				"keyspace":    keyspace,
	//				"shard":       sourceShard,
	//				"served_type": servedType,
	//			})
	//		}
	//		newTasks = append(newTasks, migrateServedTypesTasks)
	//	}

	return newTasks, "", nil
}

// RequiredParameters is part of the Task interface.
func (t *HorizontalReshardingTask) RequiredParameters() []string {
	return []string{"keyspace", "source_shard_list", "dest_shard_list",
		"vtctld_endpoint", "vtworker_endpoint"}
}
