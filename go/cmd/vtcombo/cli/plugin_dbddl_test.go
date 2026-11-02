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

	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

func TestDBDDLSerializesCreateAndDrop(t *testing.T) {
	oldCreate, oldDrop := globalCreateDb, globalDropDb
	t.Cleanup(func() { globalCreateDb, globalDropDb = oldCreate, oldDrop })
	var active atomic.Int32
	var overlapping atomic.Bool
	operation := func() error {
		if active.Add(1) != 1 {
			overlapping.Store(true)
		}
		time.Sleep(time.Millisecond)
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
	workers.Wait()
	assert.False(t, overlapping.Load(), "database lifecycle callbacks ran concurrently")
}
