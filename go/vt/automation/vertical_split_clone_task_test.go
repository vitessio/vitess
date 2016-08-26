// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"flag"
	"testing"

	"github.com/youtube/vitess/go/vt/worker/fakevtworkerclient"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
)

func TestVerticalSplitCloneTask(t *testing.T) {
	fake := fakevtworkerclient.NewFakeVtworkerClient()
	vtworkerclient.RegisterFactory("fake", fake.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")
	flag.Set("vtworker_client_protocol", "fake")
	fake.RegisterResult([]string{"VerticalSplitClone", "--tables=moving1", "--online=false", "--offline=true", "--chunk_count=2", "--min_rows_per_chunk=4", "--write_query_max_rows=1", "--write_query_max_size=1024", "--min_healthy_rdonly_tablets=1", "--max_tps=100", "--max_replication_lag=5", "dest_keyspace/0"},
		"",  // No output.
		nil) // No error.

	task := &VerticalSplitCloneTask{}
	parameters := map[string]string{
		"dest_keyspace":              "dest_keyspace",
		"shard":                      "0",
		"tables":                     "moving1",
		"vtworker_endpoint":          "localhost:15001",
		"online":                     "false",
		"offline":                    "true",
		"chunk_count":                "2",
		"min_rows_per_chunk":         "4",
		"write_query_max_rows":       "1",
		"write_query_max_size":       "1024",
		"min_healthy_rdonly_tablets": "1",
		"max_tps":                    "100",
		"max_replication_lag":        "5",
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
