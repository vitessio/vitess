package dynamic

import (
	"context"
	"encoding/base32"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterFromString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		s          string
		encoder    func(b []byte) string
		expectedID string
		shouldErr  bool
	}{
		{
			name: "ok",
			s: `{
				"id": "dynamic_cluster",
				"discovery": "dynamic",
				"discovery-dynamic-discovery": "{\"vtctlds\": [ { \"host\": { \"fqdn\": \"localhost:15000\", \"hostname\": \"localhost:15999\" } } ], \"vtgates\": [ { \"host\": {\"hostname\": \"localhost:15991\" } } ] }"
			}`,
			expectedID: "dynamic_cluster",
		},
		{
			name: "true id",
			s: `{
				"id": true,
				"discovery": "dynamic",
				"discovery-dynamic-discovery": "{\"vtctlds\": [ { \"host\": { \"fqdn\": \"vtctld.invalid:15000\", \"hostname\": \"vtctld.invalid:15999\" } } ], \"vtgates\": [ { \"host\": {\"hostname\": \"vtgate.invalid:15991\" } } ] }"
			}`,
			expectedID: "true",
		},
		{
			name: "false id",
			s: `{
				"id": false,
				"discovery": "dynamic",
				"discovery-dynamic-discovery": "{\"vtctlds\": [ { \"host\": { \"fqdn\": \"vtctld.invalid:15000\", \"hostname\": \"vtctld.invalid:15999\" } } ], \"vtgates\": [ { \"host\": {\"hostname\": \"vtgate.invalid:15991\" } } ] }"
			}`,
			expectedID: "false",
		},
		{
			name:      "empty id",
			s:         `{"id": ""}`,
			shouldErr: true,
		},
		{
			name:      "no id",
			s:         `{"vtctlds": []}`,
			shouldErr: true,
		},
		{
			name:      "bad encoding",
			s:         "…∆ø†h¬®çå®øç", // this junk (when base32 hex'd) breaks base64 std decoding
			encoder:   base32.HexEncoding.EncodeToString,
			shouldErr: true,
		},
		{
			name:      "invalid json",
			s:         `{`,
			shouldErr: true,
		},
		{
			name:       "null discovery config",
			s:          `{"id": "dynamic_cluster", "discovery": "dynamic", "discovery-dynamic-discovery": "null"}`,
			expectedID: "dynamic_cluster",
			shouldErr:  true,
		},
		{
			name:       "null vtgate",
			s:          `{"id": "dynamic_cluster", "discovery": "dynamic", "discovery-dynamic-discovery": "{\"vtgates\": [null]}"}`,
			expectedID: "dynamic_cluster",
			shouldErr:  true,
		},
		{
			name:       "vtgate without host",
			s:          `{"id": "dynamic_cluster", "discovery": "dynamic", "discovery-dynamic-discovery": "{\"vtgates\": [{}]}"}`,
			expectedID: "dynamic_cluster",
			shouldErr:  true,
		},
		{
			name:       "null vtctld",
			s:          `{"id": "dynamic_cluster", "discovery": "dynamic", "discovery-dynamic-discovery": "{\"vtctlds\": [null]}"}`,
			expectedID: "dynamic_cluster",
			shouldErr:  true,
		},
		{
			name:       "vtctld without host",
			s:          `{"id": "dynamic_cluster", "discovery": "dynamic", "discovery-dynamic-discovery": "{\"vtctlds\": [{}]}"}`,
			expectedID: "dynamic_cluster",
			shouldErr:  true,
		},
		{
			name: "unknown vtctld backoff strategy",
			s: `{
				"id": "dynamic_cluster",
				"discovery": "dynamic",
				"discovery-dynamic-discovery": "{\"vtctlds\": [ { \"host\": { \"hostname\": \"vtctld.invalid:15999\" } } ], \"vtgates\": [ { \"host\": {\"hostname\": \"vtgate.invalid:15991\" } } ] }",
				"vtctld-backoff-strategy": "bogus"
			}`,
			expectedID: "dynamic_cluster",
			shouldErr:  true,
		},
		{
			name: "unknown vtsql backoff strategy",
			s: `{
				"id": "dynamic_cluster",
				"discovery": "dynamic",
				"discovery-dynamic-discovery": "{\"vtctlds\": [ { \"host\": { \"hostname\": \"vtctld.invalid:15999\" } } ], \"vtgates\": [ { \"host\": {\"hostname\": \"vtgate.invalid:15991\" } } ] }",
				"vtsql-backoff-strategy": "bogus"
			}`,
			expectedID: "dynamic_cluster",
			shouldErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.encoder == nil {
				tt.encoder = base64.StdEncoding.EncodeToString
			}

			enc := tt.encoder([]byte(tt.s))

			c, id, err := ClusterFromString(context.Background(), enc)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Nil(t, c, "when err != nil, cluster must be nil")
				assert.Equal(t, tt.expectedID, id)
				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, id, "when err == nil, id must be non-empty")

			assert.Equal(t, tt.expectedID, id)
			require.NotNil(t, c, "when err == nil, cluster should not be nil")
			t.Cleanup(func() { _ = c.Close() })
			assert.Equal(t, id, c.ID, "the cluster's ID must match the id it is registered under")
		})
	}
}
