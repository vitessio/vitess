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

package credentials

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/grpcclient"
)

func Test_loadCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		contents  []byte
		expected  grpcclient.StaticAuthClientCreds
		shouldErr bool
	}{
		{
			name: "success",
			contents: []byte(`{
	"Username": "vtadmin",
	"Password": "hunter2"
}`),
			expected: grpcclient.StaticAuthClientCreds{
				Username: "vtadmin",
				Password: "hunter2",
			},
			shouldErr: false,
		},
		{
			name:      "not found",
			contents:  nil,
			expected:  grpcclient.StaticAuthClientCreds{},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := ""

			if len(tt.contents) > 0 {
				f, err := ioutil.TempFile("", "vtsql-credentials-test-*")
				require.NoError(t, err)
				_, err = f.Write(tt.contents)
				require.NoError(t, err)

				path = f.Name()
				f.Close()
				defer os.Remove(path)
			}

			creds, err := loadCredentials(path)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, *creds)
		})
	}
}
