package credentials

import (
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
				f, err := os.CreateTemp("", "vtsql-credentials-test-*")
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
