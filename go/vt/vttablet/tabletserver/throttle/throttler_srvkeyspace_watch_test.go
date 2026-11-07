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

package throttle

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestWatchSrvKeyspaceCallbackAfterContextCancelled covers a Throttler that is closed after
// Open() started the goroutine that reads the throttler config, but before that goroutine starts
// watching SrvKeyspace. Close() cancels the context the watch uses. srvtopo does not start a
// watch for a cancelled context, so it calls the listener back with neither a SrvKeyspace nor an
// error, and WatchSrvKeyspaceCallback must tolerate that.
func TestWatchSrvKeyspaceCallbackAfterContextCancelled(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()
	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "cell1", "ks", &topodatapb.SrvKeyspace{}))
	srvTopoServer := srvtopo.NewResilientServer(ctx, ts, stats.NewCountersWithSingleLabel("", "", "Operation"))
	throttler := &Throttler{}

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()

	assert.NotPanics(t, func() {
		srvTopoServer.WatchSrvKeyspace(cancelledCtx, "cell1", "ks", throttler.WatchSrvKeyspaceCallback)
	})
}
