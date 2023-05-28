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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cmd/vtctldclient/command"
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
