// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"flag"
	"testing"

	"github.com/youtube/vitess/go/vt/worker/fakevtworkerclient"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
)

func TestSplitCloneTask(t *testing.T) {
	fake := fakevtworkerclient.NewFakeVtworkerClient()
	vtworkerclient.RegisterFactory("fake", fake.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")
	flag.Set("vtworker_client_protocol", "fake")
	fake.RegisterResult([]string{"SplitClone", "--exclude_tables=unrelated1", "--min_healthy_rdonly_endpoints=1", "test_keyspace/0"},
		"",  // No output.
		nil) // No error.

	task := &SplitCloneTask{}
	parameters := map[string]string{
		"keyspace":                     "test_keyspace",
		"source_shard":                 "0",
		"vtworker_endpoint":            "localhost:15001",
		"exclude_tables":               "unrelated1",
		"min_healthy_rdonly_endpoints": "1",
	}

	err := validateParameters(task, parameters)
	if err != nil {
		t.Fatalf("Not all required parameters were specified: %v", err)
	}

	newTasks, _ /* output */, err := task.Run(parameters)
	if newTasks != nil {
		t.Errorf("Task should not emit new tasks: %v", newTasks)
	}
	if err != nil {
		t.Errorf("Task should not fail: %v", err)
	}
}
