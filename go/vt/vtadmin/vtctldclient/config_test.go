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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/grpcclient"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

func withTempFile(t *testing.T, tmpdir string, name string, f func(*testing.T, *os.File)) {
	tmpfile, err := ioutil.TempFile(tmpdir, name)
	require.NoError(t, err, "TempFile(%s, %s)", tmpdir, name)
	defer os.Remove(tmpfile.Name())

	f(t, tmpfile)
}

func TestParse(t *testing.T) {
	t.Parallel()

	t.Run("no credentials provided", func(t *testing.T) {
		t.Parallel()

		cfg, err := Parse(nil, nil, []string{})
		require.NoError(t, err)

		expected := &Config{
			Cluster:         nil,
			Discovery:       nil,
			Credentials:     nil,
			CredentialsPath: "",
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
				Discovery:       nil,
				Credentials:     creds,
				CredentialsPath: credsfile.Name(),
			}

			assert.Equal(t, expected, cfg)
		})
	})
}
