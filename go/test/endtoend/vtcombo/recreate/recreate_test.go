/*
Copyright 2021 The Vitess Authors.

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

package recreate

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vttest"

	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var (
	localCluster *vttest.LocalCluster
	grpcAddress  string
	vtctldAddr   string
	ks1          = "test_keyspace"
	redirected   = "redirected"
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {

		topology := new(vttestpb.VTTestTopology)
		topology.Keyspaces = []*vttestpb.Keyspace{
			{
				Name: ks1,
				Shards: []*vttestpb.Shard{
					{Name: "-80"},
					{Name: "80-"},
				},
				RdonlyCount:  1,
				ReplicaCount: 2,
			},
			{
				Name:       redirected,
				ServedFrom: ks1,
			},
		}

		var cfg vttest.Config
		cfg.Topology = topology
		cfg.SchemaDir = os.Getenv("VTROOT") + "/test/vttest_schema"
		cfg.DefaultSchemaDir = os.Getenv("VTROOT") + "/test/vttest_schema/default"
		cfg.PersistentMode = true

		localCluster = &vttest.LocalCluster{
			Config: cfg,
		}

		err := localCluster.Setup()
		defer localCluster.TearDown()
		if err != nil {
			return 1, err
		}

		grpcAddress = fmt.Sprintf("localhost:%d", localCluster.Env.PortForProtocol("vtcombo", "grpc"))
		vtctldAddr = fmt.Sprintf("localhost:%d", localCluster.Env.PortForProtocol("vtcombo", "port"))

		return m.Run(), nil
	}()
	if err != nil {
		log.Errorf("top level error: %v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

func TestDropAndRecreateWithSameShards(t *testing.T) {
	ctx := context.Background()
	conn, err := vtgateconn.Dial(ctx, grpcAddress)
	require.Nil(t, err)
	defer conn.Close()

	cur := conn.Session(ks1+"@primary", nil)

	_, err = cur.Execute(ctx, "DROP DATABASE "+ks1, nil)
	require.Nil(t, err)

	_, err = cur.Execute(ctx, "CREATE DATABASE "+ks1, nil)
	require.Nil(t, err)

	assertTabletsPresent(t)
}

func assertTabletsPresent(t *testing.T) {
	tmpCmd := exec.Command("vtctlclient", "--vtctl_client_protocol", "grpc", "--server", grpcAddress, "--stderrthreshold", "0", "ListAllTablets", "--", "test")

	log.Infof("Running vtctlclient with command: %v", tmpCmd.Args)

	output, err := tmpCmd.CombinedOutput()
	require.Nil(t, err)

	numPrimary, numReplica, numRdonly, numDash80, num80Dash := 0, 0, 0, 0, 0
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if !strings.HasPrefix(line, "test-") {
			continue
		}
		parts := strings.Split(line, " ")
		assert.Equal(t, "test_keyspace", parts[1])

		switch parts[3] {
		case "primary":
			numPrimary++
		case "replica":
			numReplica++
		case "rdonly":
			numRdonly++
		default:
			t.Logf("invalid tablet type %s", parts[3])
		}

		switch parts[2] {
		case "-80":
			numDash80++
		case "80-":
			num80Dash++
		default:
			t.Logf("invalid shard %s", parts[2])
		}

	}

	assert.Equal(t, 2, numPrimary)
	assert.Equal(t, 2, numReplica)
	assert.Equal(t, 2, numRdonly)
	assert.Equal(t, 3, numDash80)
	assert.Equal(t, 3, num80Dash)
}
