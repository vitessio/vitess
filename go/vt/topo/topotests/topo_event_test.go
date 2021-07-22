/*
Copyright 2019 The Vitess Authors

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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"vitess.io/vitess/go/protoutil"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestWatchOnEmptyTopo(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")

	current, _, cancel := ts.WatchTopoEventLog(ctx)
	require.NoError(t, current.Err)
	require.Len(t, current.NewLogEntries, 0)

	cancel()
}

func TestWatchNewEvents(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")

	current, watchChan, watchCancel := ts.WatchTopoEventLog(ctx)
	require.NoError(t, current.Err)
	require.Len(t, current.NewLogEntries, 0)

	var wg sync.WaitGroup
	var done = make(chan struct{})
	var logEntries []*topodatapb.TopoEvent

	go func() {
		defer close(done)

		for ev := range watchChan {
			if ev.Err != nil {
				return
			}

			logEntries = append(logEntries, ev.NewLogEntries...)

			if !sort.SliceIsSorted(ev.FullLog, func(i, j int) bool {
				a := protoutil.TimeFromProto(ev.FullLog[i].StartedAt)
				b := protoutil.TimeFromProto(ev.FullLog[j].StartedAt)
				return a.Before(b)
			}) {
				t.Errorf("FullLog is not sorted")
			}
		}
	}()

	for i := 0; i < 3; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for i := 0; i < 512; i++ {
				ev := &topodatapb.TopoEvent{
					Uuid:      uuid.New().String(),
					StartedAt: protoutil.TimeToProto(time.Now()),
				}
				if err := ts.UpdateTopoEventLog(ctx, ev); err != nil {
					t.Fatalf("failed to UpdateTopoEventLog: %v", err)
				}
			}
		}()
	}

	wg.Wait()
	watchCancel()
	<-done

	require.Len(t, logEntries, 512*3)
}
