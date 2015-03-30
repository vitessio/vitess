// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"strconv"
	"strings"
)

// ReshardingTask is a cluster operation which allows to increase the number of shards.
type ReshardingTask struct {
}

func extractKeyspaceFromShard(shard string) string {
	return ""
}

func generateShardList(keyspace string, shardCount int) []string {
	return nil
}

func selectAnyTabletFromShardByType(shard string, tabletType string) string {
	return ""
}

func (t *ReshardingTask) run(parameters map[string]string) (*TaskNode, error) {
	sourceShards := strings.Split(parameters["source_shard_list"], ",")
	// TODO(mberlin): Check for error.
	shardCount, _ := strconv.Atoi(parameters["shard_count"])

	keyspace := extractKeyspaceFromShard(sourceShards[0])
	destShards := generateShardList(keyspace, shardCount)

	newTasks := &TaskNode{}

	for _, destShard := range destShards {
		newMaster := selectAnyTabletFromShardByType(destShard, "master")
		newTasks.AddParallelTask("ForceReparent", map[string]string{
			"shard":  destShard,
			"master": newMaster,
		})
	}

	newTasks.AddEmptySequentialTask()
	sourceRdonlyTablet := selectAnyTabletFromShardByType(sourceShards[0], "rdonly")
	for _, destShard := range destShards {
		newTasks.AddParallelTask("ForceReparent", map[string]string{
			"shard":                destShard,
			"source_rdonly_tablet": sourceRdonlyTablet,
		})
	}

	newTasks.AddEmptySequentialTask()
	for _, sourceShard := range sourceShards {
		// TODO(mberlin): Add a semaphore as argument to limit the parallism.
		newTasks.AddParallelTask("SplitClone", map[string]string{
			"shard":                sourceShard,
			"source_rdonly_tablet": sourceRdonlyTablet,
		})
	}

	newTasks.AddEmptySequentialTask()
	for _, destShard := range destShards {
		newTasks.AddParallelTask("SplitDiff", map[string]string{
			"shard": destShard,
		})
	}

	for _, servedType := range []string{"rdonly", "replica", "master"} {
		newTasks.AddEmptySequentialTask()
		for _, sourceShard := range sourceShards {
			newTasks.AddParallelTask("MigrateServedTypes", map[string]string{
				"shard":       sourceShard,
				"served_type": servedType,
			})
		}
	}

	return newTasks, nil
}
