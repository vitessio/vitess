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

package grpcvtctldserver

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// TestGetVersionReturnsWhenContextIsCanceled checks that GetVersion stops
// waiting for a tablet's /debug/vars response once the RPC's context ends. The
// fake tablet accepts the request and then never answers, as a hung vttablet
// would. It is not parallel because it sets the package-level version func back
// to the real /debug/vars reader, which the parallel ValidateVersion tests
// replace with a mock.
func TestGetVersionReturnsWhenContextIsCanceled(t *testing.T) {
	SetVersionFunc(getVersionFromTabletDebugVars)
	reached := make(chan string, 1)
	release := make(chan struct{})
	tabletHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case reached <- r.URL.Path:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(tabletHTTP.Close)
	t.Cleanup(func() { close(release) })

	host, portText, err := net.SplitHostPort(tabletHTTP.Listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.ParseInt(portText, 10, 32)
	require.NoError(t, err)

	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(ts.Close)
	alias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
	testutil.AddTablet(ctx, t, ts, &topodatapb.Tablet{
		Alias:    alias,
		Hostname: host,
		PortMap:  map[string]int32{"vt": int32(port)},
		Keyspace: "ks",
		Shard:    "-",
		Type:     topodatapb.TabletType_REPLICA,
	}, nil)
	vtctld := NewTestVtctldServer(ts, &testutil.TabletManagerClient{})

	rpcCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() {
		_, err := vtctld.GetVersion(rpcCtx, &vtctldatapb.GetVersionRequest{TabletAlias: alias})
		done <- err
	}()

	require.Eventually(t, func() bool { return len(reached) == 1 },
		30*time.Second, 10*time.Millisecond, "GetVersion never sent a request to the tablet")
	require.Equal(t, "/debug/vars", <-reached)

	// The caller gives up, as when a gRPC client cancels or its deadline passes.
	cancel()
	require.Eventually(t, func() bool { return len(done) == 1 },
		30*time.Second, 10*time.Millisecond,
		"GetVersion still waited on the tablet's /debug/vars 30s after its context was canceled")
	require.ErrorContains(t, <-done, context.Canceled.Error())
}
