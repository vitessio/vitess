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

func TestMigrateServedTypesTask(t *testing.T) {
	fake := fakevtctlclient.NewFakeVtctlClient()
	vtctlclient.RegisterFactory("fake", fake.FakeVtctlClientFactory)
	defer vtctlclient.UnregisterFactoryForTest("fake")
	flag.Set("vtctl_client_protocol", "fake")
	task := &MigrateServedTypesTask{}

	fake.RegisterResult([]string{"MigrateServedTypes", "test_keyspace/0", "rdonly"},
		"",  // No output.
		nil) // No error.
	parameters := map[string]string{
		"keyspace":        "test_keyspace",
		"source_shard":    "0",
		"type":            "rdonly",
		"vtctld_endpoint": "localhost:15000",
	}
	testTask(t, "MigrateServedTypes", task, parameters)

	fake.RegisterResult([]string{"MigrateServedTypes", "--cell=cell1", "--reverse=true", "test_keyspace/0", "rdonly"},
		"",  // No output.
		nil) // No error.
	parameters["cell"] = "cell1"
	parameters["reverse"] = "true"
	testTask(t, "MigrateServedTypes", task, parameters)
}

func testTask(t *testing.T, test string, task Task, parameters map[string]string) {
	err := validateParameters(task, parameters)
	if err != nil {
		t.Fatalf("%s: Not all required parameters were specified: %v", test, err)
	}

	newTasks, _ /* output */, err := task.Run(parameters)
	if newTasks != nil {
		t.Errorf("%s: Task should not emit new tasks: %v", test, newTasks)
	}
	if err != nil {
		t.Errorf("%s: Task should not fail: %v", err, test)
	}
}
