/*
Copyright 2026 The Vitess Authors.

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

package command

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSubcommandsAcceptConnectionFlags checks that every subcommand accepts
// --server and the zk2topo connection flags, which are defined on the root
// command.
func TestSubcommandsAcceptConnectionFlags(t *testing.T) {
	for _, sub := range Root.Commands() {
		if sub.Name() == "help" || sub.Name() == "completion" {
			continue
		}
		t.Run(sub.Name(), func(t *testing.T) {
			args := []string{
				"--server", "zk1:2181,zk2:2181",
				"--topo-zk-tls-cert", "/tmp/cert.pem",
				"--topo-zk-tls-key", "/tmp/key.pem",
				"--topo-zk-tls-ca", "/tmp/ca.pem",
				"--topo-zk-auth-file", "/tmp/auth",
				"/zk",
			}
			cmd, rest, err := Root.Find(append([]string{sub.Name()}, args...))
			require.NoError(t, err)
			require.Equal(t, sub, cmd)

			require.NoError(t, cmd.ParseFlags(rest))
			assert.Equal(t, "zk1:2181,zk2:2181", server)
			for name, want := range map[string]string{
				"topo-zk-tls-cert":  "/tmp/cert.pem",
				"topo-zk-tls-key":   "/tmp/key.pem",
				"topo-zk-tls-ca":    "/tmp/ca.pem",
				"topo-zk-auth-file": "/tmp/auth",
			} {
				f := cmd.Flags().Lookup(name)
				require.NotNil(t, f, name)
				assert.Equal(t, want, f.Value.String(), name)
			}
			assert.Equal(t, []string{"/zk"}, cmd.Flags().Args())
		})
	}
}
