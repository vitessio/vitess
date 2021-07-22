/*
Copyright 2021 The Vitess Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topotests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/protoutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestWatchOnEmptyTopo(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")

	current, _, cancel := ts.WatchTopoEvents(ctx)
	require.NoError(t, current.Err)
	require.Len(t, current.ActiveEvents, 0)

	cancel()
}

func TestDuplicateEvents(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")

	ev := &topodatapb.TopoEvent{
		Uuid:      uuid.New().String(),
		StartedAt: protoutil.TimeToProto(time.Now()),
	}

	err := ts.AppendTopoEvent(ctx, ev)
	require.NoError(t, err)

	err = ts.AppendTopoEvent(ctx, ev)
	require.Error(t, err)
}

func TestWatchNewEvents(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")

	current, watchChan, watchCancel := ts.WatchTopoEvents(ctx)
	require.NoError(t, current.Err)
	require.Len(t, current.ActiveEvents, 0)

	var wg sync.WaitGroup
	var done = make(chan struct{})
	var logEntries = make(map[string]*topodatapb.TopoEvent)

	go func() {
		defer close(done)

		for data := range watchChan {
			if data.Err != nil {
				return
			}

			for _, ev := range data.ActiveEvents {
				logEntries[ev.Uuid] = ev
			}
		}
	}()

	for i := 0; i < 4; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for i := 0; i < 128; i++ {
				ev := &topodatapb.TopoEvent{
					Uuid:      uuid.New().String(),
					StartedAt: protoutil.TimeToProto(time.Now()),
				}
				if err := ts.AppendTopoEvent(ctx, ev); err != nil {
					t.Errorf("failed to AppendTopoEvent: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()
	watchCancel()
	<-done

	require.Len(t, logEntries, 128*4)
}
