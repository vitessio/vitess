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

package cli

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

// TestDBDDLSerializesCreateAndDrop checks that concurrent CREATE and DROP calls
// cannot overlap their callbacks, with each callback released through a channel.
func TestDBDDLSerializesCreateAndDrop(t *testing.T) {
	oldCreate, oldDrop := globalCreateDb, globalDropDb
	t.Cleanup(func() { globalCreateDb, globalDropDb = oldCreate, oldDrop })
	var active atomic.Int32
	var overlapping atomic.Bool
	entered := make(chan chan struct{})
	operation := func() error {
		if active.Add(1) != 1 {
			overlapping.Store(true)
		}
		release := make(chan struct{})
		entered <- release
		<-release
		active.Add(-1)
		return nil
	}
	globalCreateDb = func(context.Context, *vttestpb.Keyspace) error { return operation() }
	globalDropDb = func(context.Context, string) error { return operation() }
	plugin := &DBDDL{}
	ctx := t.Context()
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			<-start
			for range 8 {
				assert.NoError(t, plugin.CreateDatabase(ctx, "example"))
				assert.NoError(t, plugin.DropDatabase(ctx, "example"))
			}
		})
	}
	close(start)
	for range 16 * 8 * 2 {
		close(<-entered)
	}
	workers.Wait()
	assert.False(t, overlapping.Load(), "database lifecycle callbacks ran concurrently")
}

// TestDBDDLDropSkipsCallbackWhenContextDone verifies that cancelled and expired
// requests cannot start tablet shutdown, and that a subsequent DROP can proceed.
func TestDBDDLDropSkipsCallbackWhenContextDone(t *testing.T) {
	for _, tc := range []struct {
		name     string
		context  func(context.Context) (context.Context, context.CancelFunc)
		wantErr  error
		wantCode vtrpcpb.Code
	}{
		{
			name: "cancelled",
			context: func(parent context.Context) (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(parent)
				cancel()
				return ctx, cancel
			},
			wantErr:  context.Canceled,
			wantCode: vtrpcpb.Code_CANCELED,
		},
		{
			name: "deadline exceeded",
			context: func(parent context.Context) (context.Context, context.CancelFunc) {
				return context.WithDeadline(parent, time.Unix(0, 0))
			},
			wantErr:  context.DeadlineExceeded,
			wantCode: vtrpcpb.Code_DEADLINE_EXCEEDED,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldDrop := globalDropDb
			t.Cleanup(func() { globalDropDb = oldDrop })
			calls := 0
			globalDropDb = func(context.Context, string) error {
				calls++
				return nil
			}
			ctx, cancel := tc.context(t.Context())
			t.Cleanup(cancel)
			plugin := &DBDDL{}

			err := plugin.DropDatabase(ctx, "example")
			require.ErrorIs(t, vterrors.UnwrapAll(err), tc.wantErr)
			assert.Equal(t, tc.wantCode, vterrors.Code(err))
			assert.Zero(t, calls, "a cancelled DROP must not start tablet shutdown")

			require.NoError(t, plugin.DropDatabase(t.Context(), "example"))
			assert.Equal(t, 1, calls, "a subsequent DROP with a live context must proceed")
		})
	}
}
