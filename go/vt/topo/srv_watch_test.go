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

package topo_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// TestWatchSrvUnreachableGlobalTopo tests that setting up a SrvKeyspace or
// SrvVSchema watch fails within the remote operation timeout when the global
// topo server does not answer, even though the watch context has no deadline.
func TestWatchSrvUnreachableGlobalTopo(t *testing.T) {
	originalTimeout := topo.RemoteOperationTimeout
	topo.RemoteOperationTimeout = 100 * time.Millisecond
	t.Cleanup(func() { topo.RemoteOperationTimeout = originalTimeout })

	_, factory := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	// A server whose global cell connection blocks until its context ends.
	ts, err := topo.NewWithFactory(factory, memorytopo.UnreachableServerAddr, "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)

	tests := []struct {
		name  string
		watch func(ctx context.Context) error
	}{
		{
			name: "SrvKeyspace",
			watch: func(ctx context.Context) error {
				current, _, err := ts.WatchSrvKeyspace(ctx, "zone1", "ks")
				if err != nil {
					return err
				}
				return current.Err
			},
		},
		{
			name: "SrvVSchema",
			watch: func(ctx context.Context) error {
				_, _, err := ts.WatchSrvVSchema(ctx, "zone1")
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() {
				done <- tt.watch(t.Context())
			}()

			var err error
			assert.Eventually(t, func() bool {
				select {
				case err = <-done:
					return true
				default:
					return false
				}
			}, 30*time.Second, 10*time.Millisecond)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})
	}
}
