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

package vtgate

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/stats/opentsdb"
	"vitess.io/vitess/go/stats/prometheusbackend"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtgate/balancer"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	// Register /throttlerz and /throttlerlogz the same way vttablet does.
	_ "vitess.io/vitess/go/vt/throttler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestDenyAllSecurityPolicyRejectsDebugHandlers locks --security-policy=deny-all
// and requests every debug handler that is registered without the ACL check
// /debug/status performs. Each request must be refused before the handler
// writes its body or runs its side effect.
//
// Run this test by itself. The policy is process-wide and is fixed on the
// first acl.CheckAccessHTTP call:
//
//	go test -count=1 -timeout 120s -run '^TestDenyAllSecurityPolicyRejectsDebugHandlers$' ./go/vt/vtgate
//
// Left untested, because they are not registered on this mux unless a
// different server is constructed:
//   - vttablet /debug/status_details (tabletserver/status.go) and /healthz
//   - vtadmin /debug/pprof/* on its own router (vtadmin/api.go)
func TestDenyAllSecurityPolicyRejectsDebugHandlers(t *testing.T) {
	fs := servenv.GetFlagSetFor("vtgate")
	acl.RegisterFlags(fs)
	require.NoError(t, fs.Parse([]string{
		"--security-policy=deny-all",
		"--pprof-http",
		"--opentsdb-uri=http://127.0.0.1:9",
	}))

	servenv.Init()

	stats.NewCounter("ACLProbeSecretMetric", "acl probe").Add(1)
	prometheusbackend.Init("vtgate")
	_, err := opentsdb.InitWithoutServenv("vtgate")
	require.NoError(t, err)

	var flushed atomic.Bool
	logutil.OnFlush(func() { flushed.Store(true) })

	apiTablet := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "aclprobecell", Uid: 1},
		Hostname:      "tablet-acl-probe.internal",
		MysqlHostname: "mysql-acl-probe.internal",
		Keyspace:      "aclprobe",
		Shard:         "0",
		Type:          topodatapb.TabletType_REPLICA,
		PortMap:       map[string]int32{"vt": 1},
	}
	hc := discovery.NewFakeHealthCheck(nil)
	hc.AddTablet(apiTablet)
	initAPI(hc)

	bal, err := balancer.NewTabletBalancer(balancer.ModeRandom, "acl-probe-cell", []string{"acl-probe-cell"})
	require.NoError(t, err)
	(&VTGate{gw: &TabletGateway{balancer: bal}}).registerDebugBalancerHandler()

	gwTablet := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "aclprobecell", Uid: 2},
		Hostname:      "tablet-gateway-probe.internal",
		MysqlHostname: "mysql-gateway-probe.internal",
		Keyspace:      "aclprobe",
		Shard:         "0",
		Type:          topodatapb.TabletType_REPLICA,
		PortMap:       map[string]int32{"grpc": 1, "vt": 1},
	}
	// GetDialer exits the process when nothing is registered for the grpc
	// protocol. The dial result is not the data this request returns.
	tabletconn.RegisterDialer("grpc", func(context.Context, *topodatapb.Tablet, grpcclient.FailFast) (queryservice.QueryService, error) {
		return nil, errors.New("acl probe dial")
	})

	ts := memorytopo.NewServer(t.Context(), "aclprobecell")
	ghc := discovery.NewHealthCheck(t.Context(), time.Hour, time.Hour, ts, "aclprobecell", "aclprobecell", nil)
	t.Cleanup(func() { _ = ghc.Close() })
	ghc.AddTablet(gwTablet)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() { _ = servenv.HTTPServe(ln) }()

	client := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	base := "http://" + ln.Addr().String()

	var statusCode int
	var statusBody string
	require.Eventually(t, func() bool {
		var getErr error
		statusCode, statusBody, getErr = debugGet(client, base+"/debug/status")
		return getErr == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.Equalf(t, http.StatusForbidden, statusCode, "deny-all did not apply to /debug/status: %s", preview(statusBody))
	require.Contains(t, statusBody, "Access denied")
	require.NotContains(t, statusBody, "Status for")

	// Liveness stays a body-less 200. Anything else means the handler started returning data.
	liveCode, liveBody, err := debugGet(client, base+"/debug/liveness")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, liveCode)
	require.Empty(t, liveBody)

	cases := []struct {
		path   string
		absent []string
	}{
		{path: "/debug/vars", absent: []string{"ACLProbeSecretMetric", "memstats", os.Args[0]}},
		{path: "/api/health-check/", absent: []string{"mysql-acl-probe.internal", "tablet-acl-probe.internal"}},
		{path: "/api/health-check/cell/aclprobecell", absent: []string{"mysql-acl-probe.internal"}},
		{path: "/api/health-check/keyspace/aclprobe", absent: []string{"mysql-acl-probe.internal"}},
		{path: "/api/health-check/tablet/mysql-acl-probe.internal", absent: []string{"tablet-acl-probe.internal"}},
		{path: "/debug/balancer", absent: []string{"acl-probe-cell"}},
		{path: "/debug/gateway", absent: []string{"mysql-gateway-probe.internal", "tablet-gateway-probe.internal"}},
		{path: "/debug/pprof/", absent: []string{"Types of profiles available"}},
		{path: "/debug/pprof/cmdline", absent: []string{os.Args[0]}},
		{path: "/debug/pprof/heap?debug=1", absent: []string{"heap profile:"}},
		{path: "/debug/pprof/allocs?debug=1", absent: []string{"heap profile:"}},
		{path: "/debug/pprof/goroutine?debug=1", absent: []string{"goroutine profile:"}},
		{path: "/debug/pprof/mutex?debug=1", absent: []string{"--- mutex:"}},
		{path: "/debug/pprof/block?debug=1", absent: []string{"--- contention:"}},
		{path: "/debug/pprof/threadcreate?debug=1", absent: []string{"threadcreate profile:"}},
		{path: "/debug/pprof/symbol", absent: []string{"num_symbols:"}},
		{path: "/debug/pprof/trace?seconds=1", absent: []string{"trace\x00"}},
		{path: "/debug/pprof/profile?seconds=1", absent: []string{"\x1f\x8b"}},
		{path: "/metrics", absent: []string{"go_info_ext"}},
		{path: "/debug/opentsdb", absent: []string{"aclprobesecretmetric"}},
		{path: "/throttlerz/", absent: []string{"Active Throttler"}},
		{path: "/throttlerlogz/", absent: []string{"/throttlerz"}},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			code, body, err := debugGet(client, base+tc.path)
			require.NoError(t, err)
			assertDenied(t, tc.path, code, body, tc.absent...)
		})
	}

	flushCode, flushBody, err := debugGet(client, base+"/debug/flushlogs")
	require.NoError(t, err)
	assert.Falsef(t, flushed.Load(), "log flush ran for a request that must be refused")
	assertDenied(t, "/debug/flushlogs", flushCode, flushBody, "flushed")
}

func debugGet(client *http.Client, url string) (int, string, error) {
	resp, err := client.Get(url)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(body), nil
}

func assertDenied(t *testing.T, path string, code int, body string, absent ...string) {
	t.Helper()
	if code != http.StatusForbidden || !strings.Contains(body, "Access denied") {
		t.Errorf("%s: got status %d with denial=%v, body prefix %q", path, code, strings.Contains(body, "Access denied"), preview(body))
	}
	for _, marker := range absent {
		if strings.Contains(body, marker) {
			t.Errorf("%s: response contains %q", path, marker)
		}
	}
}

func preview(body string) string {
	if len(body) > 180 {
		return body[:180]
	}
	return body
}
