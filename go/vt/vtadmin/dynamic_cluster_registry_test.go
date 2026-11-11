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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	"vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/rbac"
	"vitess.io/vitess/go/vt/vtadmin/testutil"
	"vitess.io/vitess/go/vt/vtadmin/vtctldclient/fakevtctldclient"
	"vitess.io/vitess/go/vt/vtenv"
)

// Placeholder values for the operator credentials file. They are not a real
// secret; the tests only check whether this file is opened and whether its
// username is offered to a client-supplied address.
const (
	placeholderUsername = "placeholder-user"
	placeholderPassword = "placeholder-secret"

	dynamicClusterActorHeader = "x-vtadmin-actor"
	dynamicClusterAuthnName   = "vtadmin.test.dynamic-cluster-header"
)

func init() {
	rbac.RegisterAuthenticator(dynamicClusterAuthnName, func() rbac.Authenticator {
		return headerAuthenticator{}
	})
}

// headerAuthenticator maps a test header or gRPC metadata value to an actor
// whose only role is that value.
type headerAuthenticator struct{}

func (headerAuthenticator) Authenticate(ctx context.Context) (*rbac.Actor, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("missing actor")
	}
	values := md.Get(dynamicClusterActorHeader)
	if len(values) != 1 || values[0] == "" {
		return nil, fmt.Errorf("missing actor")
	}
	return &rbac.Actor{Name: values[0], Roles: []string{values[0]}}, nil
}

func (headerAuthenticator) AuthenticateHTTP(r *http.Request) (*rbac.Actor, error) {
	role := r.Header.Get(dynamicClusterActorHeader)
	if role == "" {
		return nil, fmt.Errorf("missing actor")
	}
	return &rbac.Actor{Name: role, Roles: []string{role}}, nil
}

// credentialSink is a vtgate/vtctld stand-in. It records whether any RPC
// arrived with per-RPC credential metadata, and nothing else.
type credentialSink struct {
	mu       sync.Mutex
	calls    int
	sawCreds bool
}

func (s *credentialSink) sawCredentials() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sawCreds
}

func (s *credentialSink) handle(_ any, stream grpc.ServerStream) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	s.mu.Lock()
	s.calls++
	if len(md.Get("username")) > 0 || len(md.Get("password")) > 0 {
		s.sawCreds = true
	}
	s.mu.Unlock()
	return status.Error(codes.Unimplemented, "sink")
}

func startCredentialSink(t *testing.T) (*credentialSink, string) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	sink := &credentialSink{}
	server := grpc.NewServer(grpc.UnknownServiceHandler(sink.handle))
	go server.Serve(lis)
	t.Cleanup(func() {
		server.Stop()
	})
	return sink, lis.Addr().String()
}

// dynamicClusterFixture is one VTAdmin process with a single static cluster
// whose vtgate and vtctld passwords are loaded from a local credentials file.
type dynamicClusterFixture struct {
	api      *API
	id       string
	original *cluster.Cluster
	creds    string
	sink     *credentialSink
	addr     string
}

func newDynamicClusterFixture(t *testing.T, withRBAC bool) *dynamicClusterFixture {
	t.Helper()

	sink, addr := startCredentialSink(t)
	creds := writePlaceholderCredentials(t)

	id := clusterID(t)
	original := testutil.BuildCluster(t, testutil.TestClusterConfig{
		Cluster:      &vtadminpb.Cluster{Id: id, Name: id},
		VtctldClient: &fakevtctldclient.VtctldClient{},
		Tablets: []*vtadminpb.Tablet{{
			Tablet: &topodata.Tablet{
				Alias:    &topodata.TabletAlias{Cell: "zone1", Uid: 100},
				Keyspace: "ks",
				Shard:    "-",
			},
			State: vtadminpb.Tablet_SERVING,
		}},
		Config: &cluster.Config{
			VtSQLFlags:  map[string]string{"credentials-path-tmpl": creds},
			VtctldFlags: map[string]string{"credentials-path-tmpl": creds},
		},
	})

	var rbacCfg *rbac.Config
	if withRBAC {
		rbacCfg = &rbac.Config{
			Authenticator: dynamicClusterAuthnName,
			Rules: []*struct {
				Resource string
				Actions  []string
				Subjects []string
				Clusters []string
			}{
				{
					Resource: "*",
					Actions:  []string{"*"},
					Subjects: []string{"role:admin"},
					Clusters: []string{"*"},
				},
				{
					Resource: string(rbac.TabletResource),
					Actions:  []string{string(rbac.GetAction)},
					Subjects: []string{"role:reader"},
					Clusters: []string{"*"},
				},
				{
					Resource: string(rbac.ClusterResource),
					Actions:  []string{string(rbac.GetAction)},
					Subjects: []string{"role:reader"},
					Clusters: []string{"*"},
				},
			},
		}
		require.NoError(t, rbacCfg.Reify())
	}

	api := NewAPI(vtenv.NewTestEnv(), []*cluster.Cluster{original}, Options{
		EnableDynamicClusters: true,
		RBAC:                  rbacCfg,
	})
	t.Cleanup(func() { _ = api.Close() })

	return &dynamicClusterFixture{
		api:      api,
		id:       id,
		original: original,
		creds:    creds,
		sink:     sink,
		addr:     addr,
	}
}

func writePlaceholderCredentials(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "vtadmin-credentials.json")
	payload, err := json.Marshal(map[string]string{
		"Username": placeholderUsername,
		"Password": placeholderPassword,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, payload, 0o600))
	return path
}

var dynamicClusterIDs atomic.Uint64

// clusterID is a short lowercase id. The vtadmin resolver uses it as a gRPC
// URL scheme, which is matched case-sensitively and must stay a valid scheme.
func clusterID(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("p%d", dynamicClusterIDs.Add(1))
}

// spec reuses id, points discovery at the sink, and selects the operator
// credentials file for both vtgate and vtctld.
func (f *dynamicClusterFixture) spec(id string) string {
	discovery := fmt.Sprintf(
		`{"vtctlds":[{"host":{"hostname":%q,"fqdn":%q}}],"vtgates":[{"host":{"hostname":%q}}]}`,
		f.addr, f.addr, f.addr,
	)
	return fmt.Sprintf(
		`{"id":%q,"name":%q,"discovery":"dynamic","discovery-dynamic-discovery":%q,"vtsql-credentials-path-tmpl":%q,"vtctld-credentials-path-tmpl":%q}`,
		id, id, discovery, f.creds, f.creds,
	)
}

func (f *dynamicClusterFixture) cookie() *http.Cookie {
	encoded := url.QueryEscape(base64.StdEncoding.EncodeToString([]byte(f.spec(f.id))))
	return &http.Cookie{Name: "cluster", Value: encoded}
}

func (f *dynamicClusterFixture) serve(t *testing.T, method, target, actor string, body io.Reader, cookie *http.Cookie) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, target, body)
	if actor != "" {
		req.Header.Set(dynamicClusterActorHeader, actor)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}

	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.api.serv.Router().ServeHTTP(w, req)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request did not finish")
	}
	return w
}

// openedDuring reports whether path was opened while fn ran.
func openedDuring(t *testing.T, path string, fn func()) bool {
	t.Helper()

	fd, err := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	require.NoError(t, err)
	defer unix.Close(fd)

	_, err = unix.InotifyAddWatch(fd, path, unix.IN_OPEN)
	require.NoError(t, err)
	drainInotify(fd)

	fn()
	return drainInotify(fd)
}

func drainInotify(fd int) bool {
	var buf [4096]byte
	opened := false
	for {
		n, err := unix.Read(fd, buf[:])
		if err != nil {
			return opened
		}
		if n > 0 {
			opened = true
		}
	}
}

func (f *dynamicClusterFixture) assertStaticClusterUntouched(t *testing.T) {
	t.Helper()

	assert.True(t, f.api.clusterMap[f.id] == f.original, "a client cluster spec must not replace the static cluster")

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = rbac.NewContext(ctx, &rbac.Actor{Name: "admin", Roles: []string{"admin"}})

	resp, err := f.api.GetTablets(ctx, &vtadminpb.GetTabletsRequest{})
	assert.False(t, f.sink.sawCredentials(), "a client-supplied address must not be dialed with the operator credentials")
	require.NoError(t, err)
	require.Len(t, resp.GetTablets(), 1)
	assert.Equal(t, uint32(100), resp.GetTablets()[0].GetTablet().GetAlias().GetUid())
}

func TestDynamicClusterSpecDoesNotReplaceStaticCluster(t *testing.T) {
	t.Run("http/apply-schema", func(t *testing.T) {
		f := newDynamicClusterFixture(t, true)
		opened := openedDuring(t, f.creds, func() {
			w := f.serve(t, http.MethodPost, "/api/migration/"+f.id+"/ks", "reader", strings.NewReader(`{}`), f.cookie())
			assert.NotEqual(t, http.StatusOK, w.Code)
		})
		assert.False(t, opened, "the credentials file must not be opened before the request is authorized")
		f.assertStaticClusterUntouched(t)
	})

	t.Run("http/emergency-failover", func(t *testing.T) {
		f := newDynamicClusterFixture(t, true)
		opened := openedDuring(t, f.creds, func() {
			f.serve(t, http.MethodPost, "/api/shard/"+f.id+"/ks/-/emergency_failover", "reader", strings.NewReader(`{}`), f.cookie())
		})
		assert.False(t, opened, "the credentials file must not be opened before the request is authorized")
		f.assertStaticClusterUntouched(t)
	})

	t.Run("http/get-tablets", func(t *testing.T) {
		// The reader is allowed to list tablets. That authorization is for the
		// static cluster, not for installing the cookie's addresses.
		f := newDynamicClusterFixture(t, true)
		var w *httptest.ResponseRecorder
		opened := openedDuring(t, f.creds, func() {
			w = f.serve(t, http.MethodGet, "/api/tablets", "reader", nil, f.cookie())
		})
		assert.False(t, opened, "the credentials file must not be opened for a client-supplied path")
		assert.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
		assert.Contains(t, w.Body.String(), `"uid":100`)
		f.assertStaticClusterUntouched(t)
	})

	t.Run("http/no-rbac", func(t *testing.T) {
		f := newDynamicClusterFixture(t, false)
		var w *httptest.ResponseRecorder
		opened := openedDuring(t, f.creds, func() {
			w = f.serve(t, http.MethodGet, "/api/tablets", "", nil, f.cookie())
		})
		assert.False(t, opened, "the credentials file must not be opened for a client-supplied path")
		assert.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
		assert.Contains(t, w.Body.String(), `"uid":100`)
		f.assertStaticClusterUntouched(t)
	})

	t.Run("http/new-id", func(t *testing.T) {
		// A spec whose id is not the static cluster still must not publish a
		// process-wide cluster that dials the client address with the operator
		// credentials file.
		f := newDynamicClusterFixture(t, true)
		encoded := url.QueryEscape(base64.StdEncoding.EncodeToString([]byte(f.spec("attacker"))))
		cookie := &http.Cookie{Name: "cluster", Value: encoded}
		opened := openedDuring(t, f.creds, func() {
			f.serve(t, http.MethodGet, "/api/tablets", "reader", nil, cookie)
		})
		assert.False(t, opened, "a client-supplied credentials path must not be opened")
		_, added := f.api.clusterMap["attacker"]
		assert.False(t, added, "a client spec must not be stored in the process-wide cluster map")
		f.assertStaticClusterUntouched(t)
	})

	t.Run("grpc/apply-schema", func(t *testing.T) {
		f := newDynamicClusterFixture(t, true)
		client := startVTAdminClient(t, f.api)
		opened := openedDuring(t, f.creds, func() {
			ctx := f.grpcContext(t, "reader", f.spec(f.id))
			_, err := client.ApplySchema(ctx, &vtadminpb.ApplySchemaRequest{
				ClusterId: f.id,
				Sql:       "alter table t add column c int",
				Request:   &vtctldata.ApplySchemaRequest{},
			})
			assert.Error(t, err)
		})
		assert.False(t, opened, "the credentials file must not be opened before the request is authorized")
		f.assertStaticClusterUntouched(t)
	})

	t.Run("grpc/emergency-failover", func(t *testing.T) {
		f := newDynamicClusterFixture(t, true)
		client := startVTAdminClient(t, f.api)
		opened := openedDuring(t, f.creds, func() {
			ctx := f.grpcContext(t, "reader", f.spec(f.id))
			_, _ = client.EmergencyFailoverShard(ctx, &vtadminpb.EmergencyFailoverShardRequest{
				ClusterId: f.id,
				Options:   &vtctldata.EmergencyReparentShardRequest{Keyspace: "ks", Shard: "-"},
			})
		})
		assert.False(t, opened, "the credentials file must not be opened before the request is authorized")
		f.assertStaticClusterUntouched(t)
	})

	t.Run("grpc/get-tablets", func(t *testing.T) {
		f := newDynamicClusterFixture(t, true)
		client := startVTAdminClient(t, f.api)
		var resp *vtadminpb.GetTabletsResponse
		opened := openedDuring(t, f.creds, func() {
			ctx := f.grpcContext(t, "reader", f.spec(f.id))
			var err error
			resp, err = client.GetTablets(ctx, &vtadminpb.GetTabletsRequest{})
			assert.False(t, f.sink.sawCredentials(), "a client-supplied address must not be dialed with the operator credentials")
			require.NoError(t, err)
		})
		assert.False(t, opened, "the credentials file must not be opened for a client-supplied path")
		require.Len(t, resp.GetTablets(), 1)
		assert.Equal(t, uint32(100), resp.GetTablets()[0].GetTablet().GetAlias().GetUid())
		f.assertStaticClusterUntouched(t)
	})

	t.Run("grpc/no-rbac", func(t *testing.T) {
		f := newDynamicClusterFixture(t, false)
		client := startVTAdminClient(t, f.api)
		opened := openedDuring(t, f.creds, func() {
			ctx := f.grpcContext(t, "", f.spec(f.id))
			_, err := client.GetTablets(ctx, &vtadminpb.GetTabletsRequest{})
			assert.False(t, f.sink.sawCredentials(), "a client-supplied address must not be dialed with the operator credentials")
			require.NoError(t, err)
		})
		assert.False(t, opened, "the credentials file must not be opened for a client-supplied path")
		f.assertStaticClusterUntouched(t)
	})

	t.Run("cache-eviction", func(t *testing.T) {
		f := newDynamicClusterFixture(t, true)
		opened := openedDuring(t, f.creds, func() {
			f.serve(t, http.MethodPost, "/api/migration/"+f.id+"/ks", "reader", strings.NewReader(`{}`), f.cookie())
		})
		// Eviction is what the 24h cache callback runs. Delete invokes that
		// callback immediately; see go-cache Cache.Delete.
		f.api.clusterCache.Delete(f.id)
		assert.False(t, opened, "the credentials file must not be opened before the request is authorized")
		f.assertStaticClusterUntouched(t)
	})
}

func (f *dynamicClusterFixture) grpcContext(t *testing.T, actor, spec string) context.Context {
	t.Helper()

	pairs := []string{"cluster", base64.StdEncoding.EncodeToString([]byte(spec))}
	if actor != "" {
		pairs = append(pairs, dynamicClusterActorHeader, actor)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(pairs...))
}

func startVTAdminClient(t *testing.T, api *API) vtadminpb.VTAdminClient {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go api.serv.GRPCServer().Serve(lis)
	t.Cleanup(api.serv.GRPCServer().Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return vtadminpb.NewVTAdminClient(conn)
}
