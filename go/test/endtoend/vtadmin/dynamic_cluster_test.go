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

package vtadmin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// TestDynamicClusterHeader sends gRPC requests with a cluster header through
// the vtadmin binary's full interceptor chain. Whatever the header holds,
// vtadmin must answer the request and keep running.
func TestDynamicClusterHeader(t *testing.T) {
	vp := &clusterInstance.VtadminProcess
	vp.MakeAPICallRetry(t, "api/keyspaces")

	conn, err := grpc.NewClient(vp.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := vtadminpb.NewVTAdminClient(conn)

	withSpec := func(t *testing.T, spec string) context.Context {
		return metadata.AppendToOutgoingContext(t.Context(), "cluster", base64.StdEncoding.EncodeToString([]byte(spec)))
	}

	t.Run("health check", func(t *testing.T) {
		resp, err := healthpb.NewHealthClient(conn).Check(withSpec(t, `{"id": "unknown"}`), &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
	})

	t.Run("health watch", func(t *testing.T) {
		stream, err := healthpb.NewHealthClient(conn).Watch(withSpec(t, `{"id": "unknown"}`), &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		resp, err := stream.Recv()
		require.NoError(t, err)
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
	})

	t.Run("unknown cluster", func(t *testing.T) {
		_, err := client.GetClusters(withSpec(t, `{"id": "unknown"}`), &vtadminpb.GetClustersRequest{})
		assert.Equal(t, codes.NotFound, status.Code(err), "%v", err)
	})

	t.Run("malformed spec", func(t *testing.T) {
		spec := `{"id": "malformed", "discovery": "dynamic", "discovery-dynamic-discovery": "null"}`
		_, err := client.GetClusters(withSpec(t, spec), &vtadminpb.GetClustersRequest{})
		assert.Equal(t, codes.NotFound, status.Code(err), "%v", err)
	})

	t.Run("valid spec", func(t *testing.T) {
		host := func(fqdnPort, hostnamePort int) map[string]any {
			return map[string]any{"host": map[string]any{
				"fqdn":     fmt.Sprintf("localhost:%d", fqdnPort),
				"hostname": fmt.Sprintf("localhost:%d", hostnamePort),
			}}
		}
		discovery, err := json.Marshal(map[string]any{
			"vtctlds": []any{host(vp.VtctldWebPort, vp.VtctldGrpcPort)},
			"vtgates": []any{host(vp.VtGateWebPort, vp.VtGateGrpcPort)},
		})
		require.NoError(t, err)
		spec, err := json.Marshal(map[string]string{
			"id":                          "dynamic_test",
			"name":                        "dynamic_test",
			"discovery":                   "dynamic",
			"discovery-dynamic-discovery": string(discovery),
		})
		require.NoError(t, err)

		// The second request finds the cluster that the first one registered.
		// The cluster stays registered, so later tests in this package see it
		// next to the static cluster.
		for range 2 {
			resp, err := client.GetKeyspaces(withSpec(t, string(spec)), &vtadminpb.GetKeyspacesRequest{ClusterIds: []string{"dynamic_test"}})
			require.NoError(t, err)
			require.Len(t, resp.GetKeyspaces(), 1)
			assert.Equal(t, "dynamic_test", resp.GetKeyspaces()[0].GetCluster().GetId())
			assert.Equal(t, uks, resp.GetKeyspaces()[0].GetKeyspace().GetName())
		}
	})

	t.Run("unknown backoff strategy", func(t *testing.T) {
		spec := `{"id": "bogus_backoff", "discovery": "dynamic", "discovery-dynamic-discovery": "{\"vtgates\":[{\"host\":{\"hostname\":\"localhost:1\"}}]}", "vtctld-backoff-strategy": "bogus"}`
		_, err := client.GetClusters(withSpec(t, spec), &vtadminpb.GetClustersRequest{})
		assert.Equal(t, codes.NotFound, status.Code(err), "%v", err)
	})

	t.Run("vtadmin is still running", func(t *testing.T) {
		code, resp, err := vp.MakeAPICall("api/keyspaces")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, code)
		assert.Contains(t, resp, uks)
	})
}
