/*
Copyright 2023 The Vitess Authors.

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

package command_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cmd/vtctldclient/command"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/localvtctldclient"

	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

type emptyLocalServer struct {
	vtctlservicepb.UnimplementedVtctldServer
}

func TestRoot(t *testing.T) {
	t.Run("error on unknown subcommand", func(t *testing.T) {
		args := append([]string{}, os.Args...)
		protocol := command.VtctldClientProtocol
		localvtctldclient.SetServer(&emptyLocalServer{})

		t.Cleanup(func() {
			os.Args = append([]string{}, args...)
			command.VtctldClientProtocol = protocol
		})

		os.Args = []string{"vtctldclient", "this-is-bunk"}
		command.VtctldClientProtocol = "local"

		err := command.Root.Execute()
		require.Error(t, err, "root command should error on unknown command")
		assert.Contains(t, err.Error(), "unknown command")
	})
}

// TestRootWithInternalVtctld tests that the internal VtctldServer
// implementation -- used with --server=internal -- works for
// commands as expected.
func TestRootWithInternalVtctld(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cell := "zone1"
	ts, factory := memorytopo.NewServerAndFactory(ctx, cell)
	topo.RegisterFactory("test", factory)
	origProtocol := command.VtctldClientProtocol
	command.VtctldClientProtocol = "local"
	baseArgs := []string{"vtctldclient", "--server", "internal", "--topo-implementation", "test"}

	args := append([]string{}, os.Args...)
	t.Cleanup(func() {
		ts.Close()
		os.Args = append([]string{}, args...)
		command.VtctldClientProtocol = origProtocol
	})

	// Create a keyspace for UpdateGossipConfig test.
	_, err := ts.GetOrCreateShard(ctx, "test_ks", "0")
	require.NoError(t, err)

	testCases := []struct {
		command   string
		args      []string
		expectErr string
	}{
		{
			command:   "AddCellInfo",
			args:      []string{"--root", "/vitess/" + cell, "--server-address", "", cell},
			expectErr: "node already exists", // Cell already exists
		},
		{
			command: "GetTablets",
		},
		{
			command:   "NoCommandDrJones",
			expectErr: "unknown command", // Invalid command
		},
		{
			command: "UpdateGossipConfig",
			args:    []string{"--enable", "--phi-threshold", "5", "--ping-interval", "2s", "--max-update-age", "10s", "test_ks"},
		},
		{
			command:   "UpdateGossipConfig",
			args:      []string{"--enable", "--disable", "test_ks"},
			expectErr: "mutually exclusive",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.command, func(t *testing.T) {
			defer func() {
				// Reset the OS args.
				os.Args = append([]string{}, args...)
			}()

			os.Args = append(baseArgs, tc.command)
			os.Args = append(os.Args, tc.args...)

			err := command.Root.Execute()
			if tc.expectErr != "" {
				require.ErrorContains(t, err, tc.expectErr)
			} else {
				require.NoError(t, err, "unexpected error: %v", err)
			}
		})
	}
}

func TestUpdateGossipConfigOmitsPhiThresholdUnlessSpecified(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cell := "zone1"
	ts, factory := memorytopo.NewServerAndFactory(ctx, cell)
	const topoImplementation = "test-update-gossip-config"
	topo.RegisterFactory(topoImplementation, factory)

	origProtocol := command.VtctldClientProtocol
	command.VtctldClientProtocol = "local"
	baseArgs := []string{"vtctldclient", "--server", "internal", "--topo-implementation", topoImplementation}

	args := append([]string{}, os.Args...)
	t.Cleanup(func() {
		ts.Close()
		os.Args = append([]string{}, args...)
		command.VtctldClientProtocol = origProtocol
	})

	resetUpdateGossipConfigFlags := func() {
		t.Helper()

		for name, value := range map[string]string{
			"disable":        "false",
			"enable":         "false",
			"max-update-age": "",
			"phi-threshold":  "0",
			"ping-interval":  "",
		} {
			flag := command.UpdateGossipConfig.Flags().Lookup(name)
			require.NotNil(t, flag)
			require.NoError(t, command.UpdateGossipConfig.Flags().Set(name, value))
			flag.Changed = false
		}
	}

	_, err := ts.GetOrCreateShard(ctx, "test_ks_1", "0")
	require.NoError(t, err)
	_, err = ts.GetOrCreateShard(ctx, "test_ks_2", "0")
	require.NoError(t, err)

	resetUpdateGossipConfigFlags()
	os.Args = append(append([]string{}, baseArgs...), "UpdateGossipConfig", "--enable", "--phi-threshold", "7", "test_ks_1")
	require.NoError(t, command.Root.Execute())

	resetUpdateGossipConfigFlags()
	os.Args = append(append([]string{}, baseArgs...), "UpdateGossipConfig", "--enable", "test_ks_2")
	require.NoError(t, command.Root.Execute())

	ki, err := ts.GetKeyspace(ctx, "test_ks_2")
	require.NoError(t, err)
	require.NotNil(t, ki.GossipConfig)
	assert.Equal(t, float64(4), ki.GossipConfig.PhiThreshold)
}
