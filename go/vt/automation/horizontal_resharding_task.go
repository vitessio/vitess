// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"fmt"
	"strings"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// HorizontalReshardingTask is a cluster operation which allows to increase the number of shards.
type HorizontalReshardingTask struct {
}

// TODO(mberlin): Uncomment/remove when "ForceReparent" and "CopySchemaShard" will be implemented.
//func selectAnyTabletFromShardByType(shard string, tabletType string) string {
//	return ""
//}

func (t *HorizontalReshardingTask) run(parameters map[string]string) ([]*pb.TaskContainer, string, error) {
	// Example: test_keyspace
	keyspace := parameters["keyspace"]
	// Example: 10-20
	sourceShards := strings.Split(parameters["source_shard_list"], ",")
	// Example: 10-18,18-20
	destShards := strings.Split(parameters["dest_shard_list"], ",")
	// Example: cell1-0000062352
	sourceRdonlyTablets := strings.Split(parameters["source_shard_rdonly_list"], ",")

	var newTasks []*pb.TaskContainer
	// TODO(mberlin): Implement "ForceParent" task and uncomment this.
	//	reparentTasks := NewTaskContainer()
	//	for _, destShard := range destShards {
	//		newMaster := selectAnyTabletFromShardByType(destShard, "master")
	//		AddTask(reparentTasks, "ForceReparent", map[string]string{
	//			"shard":  destShard,
	//			"master": newMaster,
	//		})
	//	}
	//	newTasks = append(newTasks, reparentTasks)

	// TODO(mberlin): Implement "CopySchemaShard" task and uncomment this.
	//	copySchemaTasks := NewTaskContainer()
	//	sourceRdonlyTablet := selectAnyTabletFromShardByType(sourceShards[0], "rdonly")
	//	for _, destShard := range destShards {
	//		AddTask(copySchemaTasks, "CopySchemaShard", map[string]string{
	//			"shard":                destShard,
	//			"source_rdonly_tablet": sourceRdonlyTablet,
	//		})
	//	}
	//	newTasks = append(newTasks, copySchemaTasks)

	splitCloneTasks := NewTaskContainer()
	for _, sourceShard := range sourceShards {
		// TODO(mberlin): Add a semaphore as argument to limit the parallism.
		AddTask(splitCloneTasks, "vtworker", map[string]string{
			"command":           "SplitClone",
			"keyspace":          keyspace,
			"shard":             sourceShard,
			"vtworker_endpoint": parameters["vtworker_endpoint"],
		})
	}
	newTasks = append(newTasks, splitCloneTasks)

	// TODO(mberlin): Remove this once SplitClone does this on its own.
	restoreTypeTasks := NewTaskContainer()
	for _, sourceRdonlyTablet := range sourceRdonlyTablets {
		AddTask(restoreTypeTasks, "vtctl", map[string]string{
			"command": fmt.Sprintf("ChangeSlaveType %v rdonly", sourceRdonlyTablet),
		})
	}
	newTasks = append(newTasks, restoreTypeTasks)

	splitDiffTasks := NewTaskContainer()
	for _, destShard := range destShards {
		AddTask(splitDiffTasks, "vtworker", map[string]string{
			"command":           "SplitDiff",
			"keyspace":          keyspace,
			"shard":             destShard,
			"vtworker_endpoint": parameters["vtworker_endpoint"],
		})
	}
	newTasks = append(newTasks, splitDiffTasks)

	// TODO(mberlin): Implement "CopySchemaShard" task and uncomment this.
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
	return []string{"keyspace", "source_shard_list", "source_shard_rdonly_list", "dest_shard_list"}
}
