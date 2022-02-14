package automation

import (
	"flag"
	"testing"

	"vitess.io/vitess/go/vt/vtctl/fakevtctlclient"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"
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
