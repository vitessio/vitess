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
	fake.RegisterResult([]string{"SplitClone", "--online=false", "--offline=true", "--exclude_tables=unrelated1", "--chunk_count=2", "--min_rows_per_chunk=4", "--write_query_max_rows=1", "--write_query_max_size=1024", "--min_healthy_rdonly_tablets=1", "--max_tps=100", "--max_replication_lag=5", "test_keyspace/0"},
		"",  // No output.
		nil) // No error.

	task := &SplitCloneTask{}
	parameters := map[string]string{
		"keyspace":                   "test_keyspace",
		"source_shard":               "0",
		"vtworker_endpoint":          "localhost:15001",
		"online":                     "false",
		"offline":                    "true",
		"exclude_tables":             "unrelated1",
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
