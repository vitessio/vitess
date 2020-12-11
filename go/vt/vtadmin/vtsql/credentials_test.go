package vtsql

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/grpcclient"
)

func Test_loadCredentials(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			path := ""

			if len(tt.contents) > 0 {
				f, err := ioutil.TempFile("", "vtsql-credentials-test-*")
				require.NoError(t, err)
				_, err = f.Write(tt.contents)
				require.NoError(t, err)

				path = f.Name()
				f.Close()
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
