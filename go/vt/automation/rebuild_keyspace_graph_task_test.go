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

	"github.com/youtube/vitess/go/vt/vtctl/fakevtctlclient"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
)

func TestRebuildKeyspaceGraphTask(t *testing.T) {
	fake := fakevtctlclient.NewFakeVtctlClient()
	vtctlclient.RegisterFactory("fake", fake.FakeVtctlClientFactory)
	defer vtctlclient.UnregisterFactoryForTest("fake")
	flag.Set("vtctl_client_protocol", "fake")
	task := &RebuildKeyspaceGraphTask{}

	fake.RegisterResult([]string{"RebuildKeyspaceGraph", "test_keyspace"},
		"",  // No output.
		nil) // No error.
	parameters := map[string]string{
		"keyspace":        "test_keyspace",
		"vtctld_endpoint": "localhost:15000",
	}
	testTask(t, "RebuildKeyspaceGraph", task, parameters, fake)

	fake.RegisterResult([]string{"RebuildKeyspaceGraph", "--cells=cell1", "test_keyspace"},
		"",  // No output.
		nil) // No error.
	parameters["cells"] = "cell1"
	testTask(t, "RebuildKeyspaceGraph", task, parameters, fake)
}
