// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"flag"
	"testing"

	"github.com/youtube/vitess/go/vt/vtctl/fakevtctlclient"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
)

func TestCopySchemaShardTask(t *testing.T) {
	fake := fakevtctlclient.NewFakeVtctlClient()
	vtctlclient.RegisterFactory("fake", fake.FakeVtctlClientFactory)
	flag.Set("vtctl_client_protocol", "fake")
	fake.RegisterResult([]string{"CopySchemaShard", "test_keyspace/0", "test_keyspace/2"},
		"",  // No output.
		nil) // No error.

	task := &CopySchemaShardTask{}
	parameters := map[string]string{
		"keyspace":        "test_keyspace",
		"source_shard":    "0",
		"dest_shard":      "2",
		"vtctld_endpoint": "localhost:15000",
	}

	err := checkRequiredParameters(task, parameters)
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
