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

package dynamic_test

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/vtadmin"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/cluster/dynamic"
	"vitess.io/vitess/go/vt/vtenv"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

const (
	// validSpec builds a cluster whose hosts don't resolve, so its connections
	// never succeed even when a local Vitess cluster is running.
	validSpec = `{
		"id": "dynamic_cluster",
		"name": "dynamic_cluster",
		"discovery": "dynamic",
		"discovery-dynamic-discovery": "{\"vtctlds\": [ { \"host\": { \"fqdn\": \"vtctld.invalid:15000\", \"hostname\": \"vtctld.invalid:15999\" } } ], \"vtgates\": [ { \"host\": {\"hostname\": \"vtgate.invalid:15991\" } } ] }"
	}`
	// idOnlySpec has an id but cannot be built into a cluster.
	idOnlySpec = `{"id": "dynamic_cluster"}`
)

func newDynamicAPI(t *testing.T) *vtadmin.API {
	t.Helper()

	api := vtadmin.NewAPI(vtenv.NewTestEnv(), nil, vtadmin.Options{EnableDynamicClusters: true})
	t.Cleanup(func() { _ = api.Close() })

	return api
}

func clusterContext(ctx context.Context, spec string) context.Context {
	return metadata.NewIncomingContext(ctx, metadata.Pairs("cluster", base64.StdEncoding.EncodeToString([]byte(spec))))
}

func getClustersInfo() *grpc.UnaryServerInfo {
	return &grpc.UnaryServerInfo{FullMethod: "/vtadmin.VTAdmin/GetClusters"}
}

func staticHandler(t *testing.T) grpc.UnaryHandler {
	return func(ctx context.Context, req any) (any, error) {
		t.Errorf("static handler should not be called for a VTAdmin method with a cluster header")
		return nil, nil
	}
}

func clusterIDs(t *testing.T, resp any) []string {
	t.Helper()

	require.IsType(t, &vtadminpb.GetClustersResponse{}, resp)

	var ids []string
	for _, c := range resp.(*vtadminpb.GetClustersResponse).Clusters {
		ids = append(ids, c.Id)
	}

	return ids
}

func TestUnaryServerInterceptorNonVTAdminMethod(t *testing.T) {
	t.Parallel()

	api := newDynamicAPI(t)
	interceptor := dynamic.UnaryServerInterceptor(api)

	healthServer := health.NewServer()
	info := &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}
	handler := func(ctx context.Context, req any) (any, error) {
		return healthServer.Check(ctx, req.(*healthpb.HealthCheckRequest))
	}

	resp, err := interceptor(clusterContext(t.Context(), idOnlySpec), &healthpb.HealthCheckRequest{}, info, handler)
	require.NoError(t, err)
	require.IsType(t, &healthpb.HealthCheckResponse{}, resp)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.(*healthpb.HealthCheckResponse).Status)
}

// Not parallel, because it checks for leaked goroutines.
func TestUnaryServerInterceptorNonVTAdminMethodDoesNotBuildCluster(t *testing.T) {
	interceptor := dynamic.UnaryServerInterceptor(newDynamicAPI(t))

	healthServer := health.NewServer()
	info := &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}
	handler := func(ctx context.Context, req any) (any, error) {
		return healthServer.Check(ctx, req.(*healthpb.HealthCheckRequest))
	}

	existingGoroutines := goleak.IgnoreCurrent()
	for range 10 {
		_, err := interceptor(clusterContext(t.Context(), validSpec), &healthpb.HealthCheckRequest{}, info, handler)
		require.NoError(t, err)
	}

	goleak.VerifyNone(t, existingGoroutines)
}

// Not parallel, because it checks for leaked goroutines.
func TestUnaryServerInterceptorRepeatedSpecDoesNotLeakCluster(t *testing.T) {
	interceptor := dynamic.UnaryServerInterceptor(newDynamicAPI(t))

	_, err := interceptor(clusterContext(t.Context(), validSpec), &vtadminpb.GetClustersRequest{}, getClustersInfo(), staticHandler(t))
	require.NoError(t, err)

	existingGoroutines := goleak.IgnoreCurrent()
	for range 10 {
		resp, err := interceptor(clusterContext(t.Context(), validSpec), &vtadminpb.GetClustersRequest{}, getClustersInfo(), staticHandler(t))
		require.NoError(t, err)
		assert.Equal(t, []string{"dynamic_cluster"}, clusterIDs(t, resp))
	}

	// The registered cluster keeps redialing its vtctld, which doesn't resolve.
	goleak.VerifyNone(t, existingGoroutines, goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransportAndUnlock"))
}

// panickingAPI is a dynamic API whose GetClusters panics.
type panickingAPI struct {
	vtadminpb.UnimplementedVTAdminServer
}

func (api *panickingAPI) WithCluster(c *cluster.Cluster, id string) (dynamic.API, error) {
	if c != nil {
		_ = c.Close()
	}

	return api, nil
}

func (api *panickingAPI) Handler() http.Handler { return nil }

func (api *panickingAPI) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	panic("GetClusters failed")
}

// Not parallel, because it captures the process-wide logger.
func TestUnaryServerInterceptorRecoversPanic(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelError)
	interceptor := dynamic.UnaryServerInterceptor(&panickingAPI{})

	resp, err := interceptor(clusterContext(t.Context(), validSpec), &vtadminpb.GetClustersRequest{}, getClustersInfo(), staticHandler(t))
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err), "unexpected error: %v", err)
	assert.Nil(t, resp)

	record := logs.Record(t, "panic in dynamic cluster interceptor")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "/vtadmin.VTAdmin/GetClusters", record["method"])
	assert.Equal(t, "GetClusters failed", record["panic"])
	assert.Contains(t, record["stack"], "(*panickingAPI).GetClusters")
}

// panickingWithClusterAPI is a dynamic API whose WithCluster panics.
type panickingWithClusterAPI struct {
	panickingAPI
}

func (api *panickingWithClusterAPI) WithCluster(c *cluster.Cluster, id string) (dynamic.API, error) {
	if c != nil {
		_ = c.Close()
	}

	panic("WithCluster failed")
}

// Not parallel, because it captures the process-wide logger.
func TestUnaryServerInterceptorRecoversWithClusterPanic(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelError)
	interceptor := dynamic.UnaryServerInterceptor(&panickingWithClusterAPI{})

	resp, err := interceptor(clusterContext(t.Context(), validSpec), &vtadminpb.GetClustersRequest{}, getClustersInfo(), staticHandler(t))
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err), "unexpected error: %v", err)
	assert.Nil(t, resp)

	record := logs.Record(t, "panic in dynamic cluster interceptor")
	assert.Equal(t, "WithCluster failed", record["panic"])
}

// Not parallel, because it checks for leaked goroutines.
func TestUnaryServerInterceptorInvalidTabletFQDNTemplateDoesNotLeak(t *testing.T) {
	interceptor := dynamic.UnaryServerInterceptor(newDynamicAPI(t))
	spec := `{
		"id": "dynamic_cluster",
		"discovery": "dynamic",
		"discovery-dynamic-discovery": "{\"vtctlds\": [ { \"host\": { \"fqdn\": \"vtctld.invalid:15000\", \"hostname\": \"vtctld.invalid:15999\" } } ], \"vtgates\": [ { \"host\": {\"hostname\": \"vtgate.invalid:15991\" } } ] }",
		"tablet-fqdn-tmpl": "{{"
	}`

	existingGoroutines := goleak.IgnoreCurrent()
	for range 5 {
		resp, err := interceptor(clusterContext(t.Context(), spec), &vtadminpb.GetClustersRequest{}, getClustersInfo(), staticHandler(t))
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err), "unexpected error: %v", err)
		assert.Nil(t, resp)
	}

	goleak.VerifyNone(t, existingGoroutines)
}

func TestUnaryServerInterceptorUnknownCluster(t *testing.T) {
	t.Parallel()

	interceptor := dynamic.UnaryServerInterceptor(newDynamicAPI(t))

	resp, err := interceptor(clusterContext(t.Context(), idOnlySpec), &vtadminpb.GetClustersRequest{}, getClustersInfo(), staticHandler(t))
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err), "unexpected error: %v", err)
	assert.Nil(t, resp)
}

type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *fakeServerStream) Context() context.Context { return s.ctx }

func TestStreamServerInterceptorNonVTAdminMethod(t *testing.T) {
	t.Parallel()

	api := newDynamicAPI(t)
	interceptor := dynamic.StreamServerInterceptor(api)

	errFromHandler := errors.New("health watch handler")
	info := &grpc.StreamServerInfo{FullMethod: "/grpc.health.v1.Health/Watch", IsServerStream: true}
	handler := func(srv any, ss grpc.ServerStream) error {
		return errFromHandler
	}

	err := interceptor(nil, &fakeServerStream{ctx: clusterContext(t.Context(), validSpec)}, info, handler)
	require.ErrorIs(t, err, errFromHandler, "the original stream handler should be called")

	clusters, err := api.GetClusters(t.Context(), &vtadminpb.GetClustersRequest{})
	require.NoError(t, err)
	assert.Empty(t, clusters.Clusters, "a non-VTAdmin method must not add a dynamic cluster")
}
