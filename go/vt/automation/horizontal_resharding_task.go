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

func (t *HorizontalReshardingTask) run(parameters map[string]string) ([]*pb.TaskContainer, string, error) {
	// Example: test_keyspace
	keyspace := parameters["keyspace"]
	// Example: 10-20
	sourceShards := strings.Split(parameters["source_shard_list"], ",")
	// Example: 10-18,18-20
	destShards := strings.Split(parameters["dest_shard_list"], ",")
	// Example: cell1-0000062352
	sourceRdonlyTablets := strings.Split(parameters["source_shard_rdonly_list"], ",")
	// Example: cell1-0000062349,cell1-0000062352
	destRdonlyTablets := strings.Split(parameters["dest_shard_rdonly_list"], ",")
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

	// TODO(mberlin): Remove this once SplitClone does this on its own.
	changeSlaveTypeTasks := NewTaskContainer()
	for _, sourceRdonlyTablet := range sourceRdonlyTablets {
		AddTask(changeSlaveTypeTasks, "ChangeSlaveTypeTask", map[string]string{
			"tablet_alias":    sourceRdonlyTablet,
			"tablet_type":     "rdonly",
			"vtctld_endpoint": vtctldEndpoint,
		})
	}
	newTasks = append(newTasks, changeSlaveTypeTasks)

	// TODO(mberlin): Simplify this code when the ChangeSlaveTypeTask can be removed.
	//                Since the open-source automation does not support nested tasks,
	//                we have to run the diff for each dest shard and then return
	//                the taken out source and dest rdonly tablets.
	for i := 0; i < len(destShards); i++ {
		splitDiffTask := NewTaskContainer()
		AddTask(splitDiffTask, "SplitDiffTask", map[string]string{
			"keyspace":          keyspace,
			"dest_shard":        destShards[i],
			"vtworker_endpoint": vtworkerEndpoint,
		})
		newTasks = append(newTasks, splitDiffTask)

		rdonlyTablets := append(sourceRdonlyTablets, destRdonlyTablets...)
		changeSlaveTypeTasks := NewTaskContainer()
		for _, rdonlyTablet := range rdonlyTablets {
			AddTask(changeSlaveTypeTasks, "ChangeSlaveTypeTask", map[string]string{
				"tablet_alias":    rdonlyTablet,
				"tablet_type":     "rdonly",
				"vtctld_endpoint": vtctldEndpoint,
			})
		}
		newTasks = append(newTasks, changeSlaveTypeTasks)
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

func (t *HorizontalReshardingTask) requiredParameters() []string {
	return []string{"keyspace", "source_shard_list", "dest_shard_list",
		"vtctld_endpoint", "vtworker_endpoint",
		"source_shard_rdonly_list", "dest_shard_rdonly_list"}
}
