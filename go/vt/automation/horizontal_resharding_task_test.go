/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package automation

import (
	"testing"

	"github.com/gogo/protobuf/proto"
)

func TestHorizontalReshardingTaskEmittedTasks(t *testing.T) {
	reshardingTask := &HorizontalReshardingTask{}

	parameters := map[string]string{
		// Required parameters.
		"keyspace":          "test_keyspace",
		"source_shard_list": "10-20",
		"dest_shard_list":   "10-18,18-20",
		"vtctld_endpoint":   "localhost:15000",
		"vtworker_endpoint": "localhost:15001",
		// Optional parameters.
		"exclude_tables":             "unrelated1,unrelated2",
		"min_healthy_rdonly_tablets": "1",
	}

	err := validateParameters(reshardingTask, parameters)
	if err != nil {
		t.Fatalf("Not all required parameters were specified: %v", err)
	}

	newTaskContainers, _, _ := reshardingTask.Run(parameters)

	// TODO(mberlin): Check emitted tasks against expected output.
	for _, tc := range newTaskContainers {
		t.Logf("new tasks: %v", proto.MarshalTextString(tc))
	}
}
