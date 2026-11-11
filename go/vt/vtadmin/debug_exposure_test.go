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
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/http/debug"
	"vitess.io/vitess/go/vt/vtadmin/rbac"
	vtadmintestutil "vitess.io/vitess/go/vt/vtadmin/testutil"
	"vitess.io/vitess/go/vt/vtadmin/vtctldclient/fakevtctldclient"
	"vitess.io/vitess/go/vt/vtenv"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// Placeholders stand in for the vtgate password, the Consul ACL token, and an
// environment value. They are not real credentials.
const (
	debugExposureAuthnName = "vtadmin-debug-exposure-test"

	debugExposureActor = "allowed"

	vtsqlPasswordC0 = "placeholder-vtsql-password-c0"
	consulTokenC0   = "placeholder-consul-token-c0"
	vtsqlPasswordC1 = "placeholder-vtsql-password-c1"
	consulTokenC1   = "placeholder-consul-token-c1"
	envSentinel     = "placeholder-env-value"

	envSentinelName = "VTADMIN_HTTP_DEBUG_SENTINEL"
)

func init() {
	rbac.RegisterAuthenticator(debugExposureAuthnName, func() rbac.Authenticator {
		return headerAuthenticator{}
	})
}

// headerAuthenticator is the same shape as an RBAC authenticator vtadmin loads
// from --rbac-config: missing credentials fail the request, and a named actor
// is attached when they are present.
type headerAuthenticator struct{}

func (headerAuthenticator) Authenticate(context.Context) (*rbac.Actor, error) {
	return nil, http.ErrNoCookie
}

func (headerAuthenticator) AuthenticateHTTP(r *http.Request) (*rbac.Actor, error) {
	if r.Header.Get("X-Vtadmin-Test-Actor") == "" {
		return nil, http.ErrNoCookie
	}
	return &rbac.Actor{Name: r.Header.Get("X-Vtadmin-Test-Actor")}, nil
}

// TestDebugRoutesRequireRBACAndRedactSecrets checks the HTTP routes mounted
// on the root router (the handler ListenAndServe passes to http.Serve).
//
// RBAC is enabled and permits actor "allowed" to act only on cluster c0.
// http-no-debug is left at its default, so the zero HTTP options are used.
//
// An unauthenticated request must be rejected with 401 or 403, or the route
// must be absent (the standard mux 404). A 200 from a debug handler, including
// a pprof 404 such as "Unknown profile", is the vulnerability.
//
// An authenticated request must not receive c1 at all, and must not receive
// c0's vtgate password, Consul token, or the environment sentinel, including
// when the response is 200. Those values are secrets even for an authorized
// caller.
func TestDebugRoutesRequireRBACAndRedactSecrets(t *testing.T) {
	prevOmit, prevSanitize := debug.OmitEnv, debug.SanitizeEnv
	debug.OmitEnv = flagutil.StringSetFlag{}
	debug.SanitizeEnv = flagutil.StringSetFlag{}
	t.Cleanup(func() {
		debug.OmitEnv = prevOmit
		debug.SanitizeEnv = prevSanitize
	})
	t.Setenv(envSentinelName, envSentinel)

	c0 := buildClusterWithSecrets(t, "c0", "cluster0", vtsqlPasswordC0, consulTokenC0)
	c1 := buildClusterWithSecrets(t, "c1", "cluster1", vtsqlPasswordC1, consulTokenC1)

	rbacCfg := &rbac.Config{
		Authenticator: debugExposureAuthnName,
		Rules: []*struct {
			Resource string
			Actions  []string
			Subjects []string
			Clusters []string
		}{{
			Resource: "*",
			Actions:  []string{"*"},
			Subjects: []string{"user:" + debugExposureActor},
			Clusters: []string{"c0"},
		}},
	}
	require.NoError(t, rbacCfg.Reify())

	api := NewAPI(vtenv.NewTestEnv(), []*cluster.Cluster{c0, c1}, Options{
		RBAC: rbacCfg,
		// HTTPOpts is the zero value: DisableDebug is false, which is the
		// default of --http-no-debug.
	})
	t.Cleanup(func() {
		_ = api.Close()
	})

	srv := httptest.NewServer(api.serv.Router())
	t.Cleanup(srv.Close)
	client := &http.Client{Timeout: 15 * time.Second}

	// The same authenticator is installed on /api. This request has to be
	// rejected, or the rest of the test would not be exercising RBAC.
	unauthAPI := getDebug(t, client, srv.URL+"/api/clusters", "")
	require.Equal(t, http.StatusUnauthorized, unauthAPI.status, "unauthenticated /api/clusters status %d", unauthAPI.status)
	requireNoSecrets(t, "/api/clusters", unauthAPI.body)

	authAPI := getDebug(t, client, srv.URL+"/api/clusters", debugExposureActor)
	require.Equal(t, http.StatusOK, authAPI.status, "authenticated /api/clusters status %d", authAPI.status)
	require.Contains(t, string(authAPI.body), "cluster0")
	require.NotContains(t, string(authAPI.body), "cluster1")
	requireNoSecrets(t, "/api/clusters", authAPI.body)

	routes := []string{
		"/debug/clusters",
		"/debug/cluster/c0",
		"/debug/cluster/c1",
		"/debug/env",
		"/debug/pprof/cmdline",
		"/debug/pprof/symbol",
		"/debug/pprof/heap",
		"/debug/pprof/goroutine?debug=2",
		"/debug/pprof/",
		"/debug/pprof/profile?seconds=1",
	}

	for _, path := range routes {
		t.Run("unauthenticated "+path, func(t *testing.T) {
			resp := getDebug(t, client, srv.URL+path, "")
			requireNoSecrets(t, path, resp.body)
			if path == "/debug/pprof/cmdline" && bytes.Equal(resp.body, []byte(strings.Join(os.Args, "\x00"))) {
				t.Fatalf("unauthenticated %s served the process command line", path)
			}
			if !debugRouteClosed(resp.status, resp.body) {
				t.Fatalf("unauthenticated %s returned status %d (%d bytes); debug handlers are reachable without RBAC", path, resp.status, len(resp.body))
			}
		})

		t.Run("authenticated "+path, func(t *testing.T) {
			resp := getDebug(t, client, srv.URL+path, debugExposureActor)
			requireNoSecrets(t, path, resp.body)
			if resp.status == http.StatusOK && bytes.Contains(resp.body, []byte("cluster1")) {
				t.Fatalf("authenticated %s included cluster1, which this actor is not allowed to access", path)
			}
		})
	}
}

func buildClusterWithSecrets(t *testing.T, id, name, vtsqlPassword, consulToken string) *cluster.Cluster {
	t.Helper()

	cfg := &cluster.Config{}
	require.NoError(t, cfg.Set("id="+id+
		",name="+name+
		",discovery=consul"+
		",discovery-consul-token="+consulToken+
		",vtsql-credentials-username=placeholder-user"+
		",vtsql-credentials-password="+vtsqlPassword))

	return vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
		Cluster:      &vtadminpb.Cluster{Id: id, Name: name},
		VtctldClient: &fakevtctldclient.VtctldClient{},
		Config:       cfg,
	})
}

type debugHTTPResult struct {
	status int
	body   []byte
}

func getDebug(t *testing.T, client *http.Client, url, actor string) debugHTTPResult {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	if actor != "" {
		req.Header.Set("X-Vtadmin-Test-Actor", actor)
	}

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	require.NoError(t, err)
	return debugHTTPResult{status: resp.StatusCode, body: body}
}

func requireNoSecrets(t *testing.T, path string, body []byte) {
	t.Helper()

	var disclosed []string
	for _, secret := range []struct {
		name  string
		value string
	}{
		{name: "vtsql-credentials-password for c0", value: vtsqlPasswordC0},
		{name: "discovery-consul-token for c0", value: consulTokenC0},
		{name: "vtsql-credentials-password for c1", value: vtsqlPasswordC1},
		{name: "discovery-consul-token for c1", value: consulTokenC1},
		{name: envSentinelName, value: envSentinel},
	} {
		if bytes.Contains(body, []byte(secret.value)) {
			disclosed = append(disclosed, secret.name)
		}
	}
	if len(disclosed) > 0 {
		t.Fatalf("%s disclosed %s (response is %d bytes)", path, strings.Join(disclosed, ", "), len(body))
	}
}

// debugRouteClosed reports whether an unauthenticated caller was kept out of a
// debug handler. The only 404 that counts is the router's own not-found page.
// A pprof handler that serves "Unknown profile" has already run.
func debugRouteClosed(status int, body []byte) bool {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return true
	case http.StatusNotFound:
		return bytes.Equal(body, []byte("404 page not found\n"))
	default:
		return false
	}
}
