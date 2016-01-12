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
	defer vtctlclient.UnregisterFactoryForTest("fake")
	flag.Set("vtctl_client_protocol", "fake")
	fake.RegisterResult([]string{"CopySchemaShard", "test_keyspace/0", "test_keyspace/2"},
		"",  // No output.
		nil) // No error.

	task := &CopySchemaShardTask{}
	parameters := map[string]string{
		"source_keyspace_and_shard": "test_keyspace/0",
		"dest_keyspace_and_shard":   "test_keyspace/2",
		"vtctld_endpoint":           "localhost:15000",
		"exclude_tables":            "",
	}
	testTask(t, "CopySchemaShard", task, parameters, fake)

	fake.RegisterResult([]string{"CopySchemaShard", "--exclude_tables=excluded_table1", "test_keyspace/0", "test_keyspace/2"},
		"",  // No output.
		nil) // No error.
	parameters["exclude_tables"] = "excluded_table1"
	testTask(t, "CopySchemaShard", task, parameters, fake)
}
