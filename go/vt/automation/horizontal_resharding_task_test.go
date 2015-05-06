// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"testing"

	"github.com/golang/protobuf/proto"
)

func TestHorizontalReshardingTaskEmittedTasks(t *testing.T) {
	reshardingTask := &HorizontalReshardingTask{}

	parameters := map[string]string{
		"source_shard_rdonly_list": "cell1-0000062352",
		"keyspace":                 "test_keyspace",
		"source_shard_list":        "10-20",
		"dest_shard_list":          "10-18,18-20",
		"vtworker_endpoint":        "localhost:12345",
	}

	err := checkRequiredParameters(reshardingTask, parameters)
	if err != nil {
		t.Fatalf("Not all required parameters were specified: %v", err)
	}

	newTaskContainers, _, _ := reshardingTask.run(parameters)

	// TODO(mberlin): Check emitted tasks against expected output.
	for _, tc := range newTaskContainers {
		t.Logf("new tasks: %v", proto.MarshalTextString(tc))
	}
}
