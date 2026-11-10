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

package dynamic

import (
	"context"
	"encoding/base32"
	"encoding/base64"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/vtadmin/cluster"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
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

			c, id, err := ClusterFromString(t.Context(), enc)
			if tt.shouldErr {
				require.Error(t, err)
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

type fakeAPI struct {
	vtadminpb.UnimplementedVTAdminServer
}

func (api *fakeAPI) WithCluster(c *cluster.Cluster, id string) (API, error) { return api, nil }
func (api *fakeAPI) Handler() http.Handler                                  { return nil }

func TestUnaryServerInterceptorLogs(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)

	const secret = "vtadmin-password-sentinel"
	interceptor := UnaryServerInterceptor(&fakeAPI{})
	info := &grpc.UnaryServerInfo{FullMethod: "/vtadmin.VTAdmin/GetClusters"}
	var encoded []string
	call := func(spec string) {
		enc := base64.StdEncoding.EncodeToString([]byte(spec))
		encoded = append(encoded, enc)
		ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("cluster", enc))
		_, _ = interceptor(ctx, &vtadminpb.GetClustersRequest{}, info, func(ctx context.Context, req any) (any, error) {
			return nil, nil
		})
	}

	call(`{"vtsql-credentials-password": "` + secret + `"`)
	record := logs.Record(t, "failed to unmarshal dynamic cluster spec from incoming context metadata; falling back to static API")
	assert.Equal(t, "WARN", record["level"])
	assert.NotEmpty(t, record["error"])

	call(`{"id": "dyn1", "discovery": "no-such-impl", "vtsql-credentials-password": "` + secret + `"}`)
	record = logs.Record(t, "failed to extract valid cluster from incoming metadata; attempting to use existing cluster")
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "dyn1", record["cluster_id"])
	assert.Contains(t, record["error"], "no-such-impl")

	assert.NotContains(t, logs.String(), secret)
	for _, enc := range encoded {
		assert.NotContains(t, logs.String(), enc)
	}
}
