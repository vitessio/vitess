/*
Copyright 2019 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/vtctl/fakevtctlclient"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"
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
