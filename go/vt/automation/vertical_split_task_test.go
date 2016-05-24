// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"flag"
	"testing"

	"golang.org/x/net/context"

	automationpb "github.com/youtube/vitess/go/vt/proto/automation"
	"github.com/youtube/vitess/go/vt/vtctl/fakevtctlclient"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
	"github.com/youtube/vitess/go/vt/worker/fakevtworkerclient"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
)

// TestVerticalSplitTask tests the vertical split cluster operation
// using mocked out vtctld and vtworker responses.
func TestVerticalSplitTask(t *testing.T) {
	vtctld := fakevtctlclient.NewFakeVtctlClient()
	vtctlclient.RegisterFactory("fake", vtctld.FakeVtctlClientFactory)
	defer vtctlclient.UnregisterFactoryForTest("fake")
	flag.Set("vtctl_client_protocol", "fake")

	vtworker := fakevtworkerclient.NewFakeVtworkerClient()
	vtworkerclient.RegisterFactory("fake", vtworker.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")
	flag.Set("vtworker_client_protocol", "fake")

	vtctld.RegisterResult([]string{"CopySchemaShard", "--tables=table1,table2", "source_keyspace/0", "destination_keyspace/0"},
		"",  // No output.
		nil) // No error.
	vtworker.RegisterResult([]string{"VerticalSplitClone", "--tables=table1,table2", "--min_healthy_rdonly_tablets=1", "destination_keyspace/0"}, "", nil)
	vtctld.RegisterResult([]string{"WaitForFilteredReplication", "-max_delay", "30s", "destination_keyspace/0"}, "", nil)
	vtworker.RegisterResult([]string{"VerticalSplitDiff", "--min_healthy_rdonly_tablets=1", "destination_keyspace/0"}, "", nil)
	vtctld.RegisterResult([]string{"MigrateServedFrom", "destination_keyspace/0", "rdonly"}, "", nil)
	vtctld.RegisterResult([]string{"MigrateServedFrom", "destination_keyspace/0", "replica"}, "", nil)
	vtctld.RegisterResult([]string{"MigrateServedFrom", "destination_keyspace/0", "master"},
		"ALL_DONE",
		nil)

	scheduler, err := NewScheduler()
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	defer scheduler.ShutdownAndWait()

	scheduler.Run()

	enqueueRequest := &automationpb.EnqueueClusterOperationRequest{
		Name: "VerticalSplitTask",
		Parameters: map[string]string{
			"source_keyspace":            "source_keyspace",
			"dest_keyspace":              "destination_keyspace",
			"shard_list":                 "0",
			"tables":                     "table1,table2",
			"vtctld_endpoint":            "localhost:15000",
			"vtworker_endpoint":          "localhost:15001",
			"min_healthy_rdonly_tablets": "1",
		},
	}
	enqueueResponse, err := scheduler.EnqueueClusterOperation(context.Background(), enqueueRequest)
	if err != nil {
		t.Fatalf("Failed to start cluster operation. Request: %v Error: %v", enqueueRequest, err)
	}

	waitForClusterOperation(t, scheduler, enqueueResponse.Id,
		"ALL_DONE\n",
		"" /* expected error */)
}
