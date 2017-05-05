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
