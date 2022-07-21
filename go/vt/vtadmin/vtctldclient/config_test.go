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

package vtctldclient

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/backoff"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtadmin/cluster/resolver"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

func withTempFile(t *testing.T, tmpdir string, name string, f func(*testing.T, *os.File)) {
	tmpfile, err := os.CreateTemp(tmpdir, name)
	require.NoError(t, err, "TempFile(%s, %s)", tmpdir, name)
	defer os.Remove(tmpfile.Name())

	f(t, tmpfile)
}

func TestParse(t *testing.T) {
	t.Parallel()

	t.Run("no credentials provided", func(t *testing.T) {
		t.Parallel()

		cfg, err := Parse(&vtadminpb.Cluster{}, nil, []string{})
		require.NoError(t, err)

		expected := &Config{
			Cluster:         &vtadminpb.Cluster{},
			Credentials:     nil,
			CredentialsPath: "",
			ResolverOptions: &resolver.Options{
				DiscoveryTimeout:     100 * time.Millisecond,
				MinDiscoveryInterval: time.Second * 30,
				BackoffConfig:        backoff.DefaultConfig,
			},
		}
		assert.Equal(t, expected, cfg)
	})

	t.Run("credential loading", func(t *testing.T) {
		t.Parallel()

		withTempFile(t, "", "vtctldclient.config_test.testcluster.*", func(t *testing.T, credsfile *os.File) {
			creds := &grpcclient.StaticAuthClientCreds{
				Username: "admin",
				Password: "hunter2",
			}

			data, err := json.Marshal(creds)
			require.NoError(t, err, "cannot marshal credentials %+v into credsfile", creds)

			_, err = credsfile.Write(data)
			require.NoError(t, err, "cannot write credentials to file")

			credsdir := filepath.Dir(credsfile.Name())
			baseParts := strings.Split(filepath.Base(credsfile.Name()), ".")
			tmplParts := append(baseParts[:2], "{{ .Cluster.Name }}", baseParts[3])

			args := []string{
				fmt.Sprintf("--credentials-path-tmpl=%s", filepath.Join(credsdir, strings.Join(tmplParts, "."))),
			}

			cfg, err := Parse(&vtadminpb.Cluster{Name: "testcluster"}, nil, args)
			require.NoError(t, err)

			expected := &Config{
				Cluster: &vtadminpb.Cluster{
					Name: "testcluster",
				},
				Credentials:     creds,
				CredentialsPath: credsfile.Name(),
				ResolverOptions: &resolver.Options{
					DiscoveryTimeout:     100 * time.Millisecond,
					MinDiscoveryInterval: time.Second * 30,
					BackoffConfig:        backoff.DefaultConfig,
				},
			}

			assert.Equal(t, expected, cfg)
		})
	})
}
