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
	"fmt"
	"os"
	"strings"
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
	command.VtctldClientProtocol = "local"
	baseArgs := []string{"vtctldclient", "--server", "internal", "--topo-implementation", "test"}

	args := append([]string{}, os.Args...)
	protocol := command.VtctldClientProtocol
	t.Cleanup(func() {
		ts.Close()
		os.Args = append([]string{}, args...)
		command.VtctldClientProtocol = protocol
	})

	testCases := []struct {
		command   string
		args      []string
		expectErr string
	}{
		{
			command:   "AddCellInfo",
			args:      []string{"--root", fmt.Sprintf("/vitess/%s", cell), "--server-address", "", cell},
			expectErr: "node already exists", // Cell already exists
		},
		{
			command: "GetTablets",
		},
		{
			command:   "NoCommandDrJones",
			expectErr: "unknown command", // Invalid command
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
				if !strings.Contains(err.Error(), tc.expectErr) {
					t.Errorf(fmt.Sprintf("%s error = %v, expectErr = %v", tc.command, err, tc.expectErr))
				}
			} else {
				require.NoError(t, err, "unexpected error: %v", err)
			}
		})
	}
}
