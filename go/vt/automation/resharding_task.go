// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"fmt"
	"strings"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
	"github.com/youtube/vitess/go/vt/topo"
)

// ReshardingTask is a cluster operation which allows to increase the number of shards.
type ReshardingTask struct {
}

func splitShardListIntoKeyspaceAndShards(keyspaceAndShards []string) (string, []string, error) {
	var firstKeyspace string
	shards := make([]string, 0, len(keyspaceAndShards))

	for i, keyspaceAndShard := range keyspaceAndShards {
		keyspace, shard, err := topo.ParseKeyspaceShardString(keyspaceAndShard)
		if err != nil {
			return "", nil, err
		}

		if i == 0 {
			firstKeyspace = keyspace
		} else {
			if firstKeyspace != keyspace {
				return "", nil, fmt.Errorf("All shards must have the same keyspace. First seen keyspace: %v Wrong shard: %v", firstKeyspace, keyspaceAndShard)
			}
		}
		shards = append(shards, shard)
	}

	return firstKeyspace, shards, nil
}

func selectAnyTabletFromShardByType(shard string, tabletType string) string {
	return ""
}

func (t *ReshardingTask) run(parameters map[string]string) ([]*pb.TaskContainer, string, error) {
	sourceShardsWithKeyspace := strings.Split(parameters["source_shard_list"], ",")
	sourceRdonlyTablets := strings.Split(parameters["source_shard_rdonly_list"], ",")

	keyspace, sourceShards, err := splitShardListIntoKeyspaceAndShards(sourceShardsWithKeyspace)
	if err != nil {
		return nil, "", err
	}
	destShardsWithKeyspace := strings.Split(parameters["dest_shard_list"], ",")
	keyspaceDest, destShards, err := splitShardListIntoKeyspaceAndShards(destShardsWithKeyspace)
	if keyspace != keyspaceDest {
		return nil, "", fmt.Errorf("Source and Destination keyspace are not equal. Source: %v Dest: %v", keyspace, keyspaceDest)
	}

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
	//				"shard":       sourceShard,
	//				"served_type": servedType,
	//			})
	//		}
	//		newTasks = append(newTasks, migrateServedTypesTasks)
	//	}

	return newTasks, "", nil
}

func (t *ReshardingTask) requiredParameters() []string {
	return []string{"source_shard_list", "source_shard_rdonly_list", "dest_shard_list"}
}
