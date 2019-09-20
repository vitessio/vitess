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
	testTask(t, "MigrateServedTypes", task, parameters, fake)

	fake.RegisterResult([]string{"MigrateServedTypes", "--cells=cell1", "--reverse=true", "test_keyspace/0", "rdonly"},
		"",  // No output.
		nil) // No error.
	parameters["cells"] = "cell1"
	parameters["reverse"] = "true"
	testTask(t, "MigrateServedTypes", task, parameters, fake)
}
