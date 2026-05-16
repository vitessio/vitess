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

package vreplication

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestRelayLogSendFetch(t *testing.T) {
	ctx := t.Context()
	rl := newRelayLog(ctx, 5, 10)

	event := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "t1",
			RowChanges: []*binlogdatapb.RowChange{{
				After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}},
			}},
		},
	}

	require.NoError(t, rl.Send([]*binlogdatapb.VEvent{event}))

	items, err := rl.Fetch()
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Len(t, items[0], 1)
	assert.Equal(t, binlogdatapb.VEventType_ROW, items[0][0].Type)
}

func TestRelayLogSendTimeout(t *testing.T) {
	ctx := t.Context()
	oldDeadline := vplayerProgressDeadline
	vplayerProgressDeadline = 100 * time.Millisecond
	t.Cleanup(func() {
		vplayerProgressDeadline = oldDeadline
	})

	rl := newRelayLog(ctx, 1, 1)

	event := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "t1",
			RowChanges: []*binlogdatapb.RowChange{{
				After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}},
			}},
		},
	}

	require.NoError(t, rl.Send([]*binlogdatapb.VEvent{event}))

	errCh := make(chan error, 1)
	go func() {
		errCh <- rl.Send([]*binlogdatapb.VEvent{event})
	}()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.ErrorContains(t, err, relayLogIOStalledMsg)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for send")
	}
}

func TestRelayLogFetchTimeout(t *testing.T) {
	ctx := t.Context()
	oldIdle := idleTimeout
	idleTimeout = 100 * time.Millisecond
	t.Cleanup(func() {
		idleTimeout = oldIdle
	})

	rl := newRelayLog(ctx, 1, 1)

	items, err := rl.Fetch()
	require.NoError(t, err)
	assert.Len(t, items, 0)
}

func TestRelayLogDoneReturnsEOF(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rl := newRelayLog(ctx, 1, 1)

	items, err := rl.Fetch()
	assert.ErrorIs(t, err, io.EOF)
	assert.Nil(t, items)
}

func TestRelayLogEventsSize(t *testing.T) {
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "t1",
			RowChanges: []*binlogdatapb.RowChange{
				{Before: &querypb.Row{Values: []byte("ab"), Lengths: []int64{2}}},
				{After: &querypb.Row{Values: []byte("cde"), Lengths: []int64{3}}},
			},
		},
	}
	otherEvent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_COMMIT}

	size := eventsSize([]*binlogdatapb.VEvent{rowEvent, otherEvent})
	assert.Equal(t, 5, size)
}
