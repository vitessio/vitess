// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"testing"

	"github.com/golang/protobuf/proto"
)

func TestReshardingTaskEmittedTasks(t *testing.T) {
	reshardingTask := ReshardingTask{}

	newTaskContainers, _, _ := reshardingTask.run(map[string]string{
		"source_shard_rdonly_list": "cell1-0000062352",
		"source_shard_list":        "test_keyspace/0",
		"vtworker_endpoint":        "localhost:12345",
		"dest_shard_list":          "test_keyspace/-80,test_keyspace/80"})

	// TODO(mberlin): Check emitted tasks against expected output.
	for _, tc := range newTaskContainers {
		t.Logf("new tasks: %v", proto.MarshalTextString(tc))
	}
}
