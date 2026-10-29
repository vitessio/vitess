/*
Copyright 2025 The Vitess Authors.

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

package logic

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

func TestDiscoveryQueue(t *testing.T) {
	q := NewDiscoveryQueue()
	require.Zero(t, q.QueueLen())

	tabletAlias := &topodatapb.TabletAlias{
		Cell: "zone1",
		Uid:  1,
	}
	tabletAliasString := topoproto.TabletAliasString(tabletAlias)

	// Push
	q.Push(tabletAlias)
	require.Equal(t, 1, q.QueueLen())
	_, found := q.enqueued[tabletAliasString]
	require.True(t, found)

	// Push duplicate
	q.Push(tabletAlias)
	require.Equal(t, 1, q.QueueLen())

	// Consume
	require.Equal(t, tabletAlias, q.Consume())
	require.Equal(t, 1, q.QueueLen())
	_, found = q.enqueued[tabletAliasString]
	require.True(t, found)

	// Release
	q.Release(tabletAlias)
	require.Zero(t, q.QueueLen())
	_, found = q.enqueued[tabletAliasString]
	require.False(t, found)
}

func TestDiscoveryQueueLogsLongWait(t *testing.T) {
	q := NewDiscoveryQueue()
	tabletAlias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 1}
	q.queue <- queueItem{TabletAlias: tabletAlias, PushedAt: time.Now().Add(-time.Hour)}

	logs := logtest.Capture(t, slog.LevelInfo)
	require.Equal(t, tabletAlias, q.Consume())

	record := logs.Record(t, "tablet spent too long waiting on a discovery queue")
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "zone1-0000000001", record["tablet"])
	// A slog.Duration is logged as integer nanoseconds.
	assert.GreaterOrEqual(t, record["duration"], float64(time.Hour))
}

type testDiscoveryQueue interface {
	QueueLen() int
	Push(*topodatapb.TabletAlias)
	Consume() *topodatapb.TabletAlias
	Release(*topodatapb.TabletAlias)
}

func BenchmarkDiscoveryQueues(b *testing.B) {
	tests := []struct {
		name  string
		queue testDiscoveryQueue
	}{
		{"Current", NewDiscoveryQueue()},
	}
	for _, test := range tests {
		q := test.queue
		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for i := range 1000 {
					q.Push(&topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  uint32(i),
					})
				}
				q.QueueLen()
				for range 1000 {
					q.Release(q.Consume())
				}
			}
		})
	}
}
