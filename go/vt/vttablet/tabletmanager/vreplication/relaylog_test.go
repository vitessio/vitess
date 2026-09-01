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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestRelayLogSendFetch(t *testing.T) {
	ctx := t.Context()
	rl := newRelayLog(ctx, 5, 10, nil)

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

	rl := newRelayLog(ctx, 1, 1, nil)

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
		require.ErrorContains(t, err, relayLogIOStalledMsg)
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

	rl := newRelayLog(ctx, 1, 1, nil)

	items, err := rl.Fetch()
	require.NoError(t, err)
	assert.Empty(t, items)
}

func TestRelayLogDoneReturnsEOF(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rl := newRelayLog(ctx, 1, 1, nil)

	items, err := rl.Fetch()
	require.ErrorIs(t, err, io.EOF)
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

// TestRelayLogSendStallDeferredWhileThrottled guards against the vplayer
// stall detector firing while the applier is throttled
// (https://github.com/vitessio/vitess/issues/20922). While the vplayer is
// denied by the throttler it does not drain the relay log, so a full relay
// log is expected backpressure, not a stall: the stall verdict must be
// deferred until a full vplayerProgressDeadline has elapsed without the
// applier being throttled. Once the throttling ends, a genuinely stuck
// applier must still produce the stall error (the behavior TestPlayerStalls
// pins end to end).
func TestRelayLogSendStallDeferredWhileThrottled(t *testing.T) {
	oldProgressDeadline := vplayerProgressDeadline
	defer func() { vplayerProgressDeadline = oldProgressDeadline }()
	vplayerProgressDeadline = 2 * time.Second

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var lastThrottledNano atomic.Int64
	rl := newRelayLog(ctx, 1, 10, &lastThrottledNano)

	events := []*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_GTID}}
	// Fill the log: with maxItems=1 the next Send blocks until a Fetch,
	// which -- as for a throttle-denied vplayer -- never comes.
	require.NoError(t, rl.Send(events))

	// Simulate an applier that is continuously denied by the throttler: a
	// denial offset that stays fresh for the whole phase. A far-future
	// offset is used instead of a timestamp-updater goroutine, which a
	// paused CI runner could starve into recording a false stall.
	lastThrottledNano.Store(int64(time.Since(vplayerThrottleEpoch) + time.Hour))

	sendResult := make(chan error, 1)
	go func() {
		sendResult <- rl.Send(events)
	}()

	// The stall deadline must not fire while the applier is throttled:
	// well after several deadlines' worth of wall time, Send must still be
	// waiting.
	select {
	case err := <-sendResult:
		require.Failf(t, "premature Send return",
			"Send returned (err: %v) while the applier was throttled; the stall deadline should have been deferred", err)
	case <-time.After(3 * vplayerProgressDeadline):
	}

	// The throttling ends: the denial timestamp goes stale. The applier
	// still isn't draining the log, so the stall must now fire once a full
	// deadline of un-throttled time has passed.
	lastThrottledNano.Store(int64(time.Since(vplayerThrottleEpoch)))
	var sendErr error
	require.Eventually(t, func() bool {
		select {
		case sendErr = <-sendResult:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond)
	// vterrors.Wrap does not support errors.Is chain traversal, so match by
	// message, as the rest of the stall handling does.
	require.ErrorContains(t, sendErr, errVPlayerStalled.Error())
	require.ErrorContains(t, sendErr, relayLogIOStalledMsg)
}
