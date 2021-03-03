/*
Copyright 2019 The Vitess Authors.

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

package messager

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/test/utils"

	"context"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	testFields = []*querypb.Field{{
		Name: "id",
		Type: sqltypes.VarBinary,
	}, {
		Name: "message",
		Type: sqltypes.VarBinary,
	}}

	testDBFields = []*querypb.Field{
		{Type: sqltypes.Int64},
		{Type: sqltypes.Int64},
		{Type: sqltypes.Int64},
		{Type: sqltypes.Int64},
		{Type: sqltypes.Int64},
		{Type: sqltypes.VarBinary},
	}
)

func newMMTable() *schema.Table {
	return &schema.Table{
		Name: sqlparser.NewTableIdent("foo"),
		Type: schema.Message,
		MessageInfo: &schema.MessageInfo{
			Fields:             testFields,
			AckWaitDuration:    1 * time.Second,
			PurgeAfterDuration: 3 * time.Second,
			MinBackoff:         1 * time.Second,
			BatchSize:          1,
			CacheSize:          10,
			PollInterval:       1 * time.Second,
		},
	}
}

func newMMTableWithBackoff() *schema.Table {
	return &schema.Table{
		Name: sqlparser.NewTableIdent("foo"),
		Type: schema.Message,
		MessageInfo: &schema.MessageInfo{
			Fields:             testFields,
			AckWaitDuration:    10 * time.Second,
			PurgeAfterDuration: 3 * time.Second,
			MinBackoff:         1 * time.Second,
			MaxBackoff:         4 * time.Second,
			BatchSize:          1,
			CacheSize:          10,
			PollInterval:       1 * time.Second,
		},
	}
}

func newMMRow(id int64) *querypb.Row {
	return sqltypes.RowToProto3([]sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(0),
		sqltypes.NULL,
		sqltypes.NewInt64(id),
		sqltypes.NewVarBinary(fmt.Sprintf("%v", id)),
	})
}

type testReceiver struct {
	rcv   func(*sqltypes.Result) error
	count sync2.AtomicInt64
	ch    chan *sqltypes.Result
}

func newTestReceiver(size int) *testReceiver {
	tr := &testReceiver{
		ch: make(chan *sqltypes.Result, size),
	}
	tr.rcv = func(qr *sqltypes.Result) error {
		tr.count.Add(1)
		tr.ch <- qr
		return nil
	}
	return tr
}

func (tr *testReceiver) WaitForCount(n int) {
	for {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
		if tr.count.Get() == int64(n) {
			return
		}
	}
}

func TestReceiverCancel(t *testing.T) {
	mm := newMessageManager(newFakeTabletServer(), newFakeVStreamer(), newMMTable(), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()

	r1 := newTestReceiver(0)
	ctx, cancel := context.WithCancel(context.Background())
	go cancel()
	_ = mm.Subscribe(ctx, r1.rcv)

	// r1 should eventually be unsubscribed.
	for i := 0; i < 10; i++ {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
		if mm.receiverCount() != 0 {
			continue
		}
		return
	}
	t.Errorf("receivers were not cleared: %d", len(mm.receivers))
}

func TestMessageManagerState(t *testing.T) {
	mm := newMessageManager(newFakeTabletServer(), newFakeVStreamer(), newMMTable(), sync2.NewSemaphore(1, 0))
	// Do it twice
	for i := 0; i < 2; i++ {
		mm.Open()
		// Idempotence.
		mm.Open()
		// The yield is for making sure runSend starts up
		// and waits on cond.
		runtime.Gosched()
		mm.Close()
		// Idempotence.
		mm.Close()
	}
}

func TestMessageManagerAdd(t *testing.T) {
	ti := newMMTable()
	ti.MessageInfo.CacheSize = 1
	mm := newMessageManager(newFakeTabletServer(), newFakeVStreamer(), ti, sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()

	row1 := &MessageRow{
		Row: []sqltypes.Value{sqltypes.NewVarBinary("1")},
	}
	if mm.Add(row1) {
		t.Error("Add(no receivers): true, want false")
	}

	r1 := newTestReceiver(0)
	go func() { <-r1.ch }()
	mm.Subscribe(context.Background(), r1.rcv)

	if !mm.Add(row1) {
		t.Error("Add(1 receiver): false, want true")
	}
	// Make sure message is enqueued.
	r1.WaitForCount(2)
	// This will fill up the cache.
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("2")}})

	// The third add has to fail.
	if mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("3")}}) {
		t.Error("Add(cache full): true, want false")
	}
}

func TestMessageManagerSend(t *testing.T) {
	tsv := newFakeTabletServer()
	mm := newMessageManager(tsv, newFakeVStreamer(), newMMTable(), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()

	r1 := newTestReceiver(1)
	mm.Subscribe(context.Background(), r1.rcv)

	want := &sqltypes.Result{
		Fields: testFields,
	}
	if got := <-r1.ch; !reflect.DeepEqual(got, want) {
		t.Errorf("Received: %v, want %v", got, want)
	}
	// Set the channel to verify call to Postpone.
	// Make it buffered so the thread doesn't block on repeated calls.
	ch := make(chan string, 20)
	tsv.SetChannel(ch)
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("1"), sqltypes.NULL}})
	want = &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("1"),
			sqltypes.NULL,
		}},
	}
	if got := <-r1.ch; !reflect.DeepEqual(got, want) {
		t.Errorf("Received: %v, want %v", got, want)
	}

	// Ensure Postpone got called.
	if got, want := <-ch, "postpone"; got != want {
		t.Errorf("Postpone: %s, want %v", got, want)
	}

	// Verify item has been removed from cache.
	// Need to obtain lock to prevent data race.
	// It may take some time for this to happen.
	inQueue := true
	inFlight := true
	for i := 0; i < 10; i++ {
		mm.cache.mu.Lock()
		if _, ok := mm.cache.inQueue["1"]; !ok {
			inQueue = false
		}
		if _, ok := mm.cache.inFlight["1"]; !ok {
			inFlight = false
		}
		mm.cache.mu.Unlock()
		if inQueue || inFlight {
			runtime.Gosched()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		break
	}
	assert.False(t, inQueue)
	assert.False(t, inFlight)

	// Test that mm stops sending to a canceled receiver.
	r2 := newTestReceiver(1)
	ctx, cancel := context.WithCancel(context.Background())
	mm.Subscribe(ctx, r2.rcv)
	<-r2.ch

	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("2")}})
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("3")}})
	// Send should be round-robin.
	<-r1.ch
	<-r2.ch

	// Cancel and wait for it to take effect.
	cancel()
	for i := 0; i < 10; i++ {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
		mm.mu.Lock()
		if len(mm.receivers) != 1 {
			mm.mu.Unlock()
			continue
		}
		mm.mu.Unlock()
		break
	}

	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("4")}})
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("5")}})
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("6")}})
	// Only r1 should be receiving.
	<-r1.ch
	<-r1.ch
	<-r1.ch
}

func TestMessageManagerPostponeThrottle(t *testing.T) {
	tsv := newFakeTabletServer()
	mm := newMessageManager(tsv, newFakeVStreamer(), newMMTable(), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()

	r1 := newTestReceiver(1)
	mm.Subscribe(context.Background(), r1.rcv)
	<-r1.ch

	// Set the channel to verify call to Postpone.
	ch := make(chan string)
	tsv.SetChannel(ch)
	tsv.postponeCount.Set(0)

	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("1"), sqltypes.NULL}})
	// Once we receive, mm will obtain the single semaphore and call postpone.
	// Postpone will wait on the unbuffered ch.
	<-r1.ch

	// Set up a second subsriber, add a message.
	r2 := newTestReceiver(1)
	mm.Subscribe(context.Background(), r2.rcv)
	<-r2.ch

	// Wait.
	for i := 0; i < 2; i++ {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	// postponeCount should be 1. Verify for two iterations.
	if got, want := tsv.postponeCount.Get(), int64(1); got != want {
		t.Errorf("tsv.postponeCount: %d, want %d", got, want)
	}

	// Receive on this channel will allow the next postpone to go through.
	<-ch
	// Wait.
	for i := 0; i < 2; i++ {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	if got, want := tsv.postponeCount.Get(), int64(1); got != want {
		t.Errorf("tsv.postponeCount: %d, want %d", got, want)
	}
	<-ch
}

func TestMessageManagerSendError(t *testing.T) {
	tsv := newFakeTabletServer()
	mm := newMessageManager(tsv, newFakeVStreamer(), newMMTable(), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()
	ctx := context.Background()

	ch := make(chan *sqltypes.Result)
	go func() { <-ch }()
	fieldSent := false
	mm.Subscribe(ctx, func(qr *sqltypes.Result) error {
		ch <- qr
		if !fieldSent {
			fieldSent = true
			return nil
		}
		return errors.New("intentional error")
	})

	postponech := make(chan string, 20)
	tsv.SetChannel(postponech)
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("1"), sqltypes.NULL}})
	<-ch

	// Ensure Postpone got called.
	if got, want := <-postponech, "postpone"; got != want {
		t.Errorf("Postpone: %s, want %v", got, want)
	}
}

func TestMessageManagerFieldSendError(t *testing.T) {
	mm := newMessageManager(newFakeTabletServer(), newFakeVStreamer(), newMMTable(), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()
	ctx := context.Background()

	ch := make(chan *sqltypes.Result)
	go func() { <-ch }()
	done := mm.Subscribe(ctx, func(qr *sqltypes.Result) error {
		ch <- qr
		return errors.New("non-eof")
	})

	// This should not hang because a field send error must terminate
	// subscription.
	<-done
}

func TestMessageManagerBatchSend(t *testing.T) {
	ti := newMMTable()
	ti.MessageInfo.BatchSize = 2
	mm := newMessageManager(newFakeTabletServer(), newFakeVStreamer(), ti, sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()

	r1 := newTestReceiver(1)
	mm.Subscribe(context.Background(), r1.rcv)
	<-r1.ch

	row1 := &MessageRow{
		Row: []sqltypes.Value{sqltypes.NewVarBinary("1"), sqltypes.NULL},
	}
	mm.Add(row1)
	want := &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("1"),
			sqltypes.NULL,
		}},
	}
	if got := <-r1.ch; !reflect.DeepEqual(got, want) {
		t.Errorf("Received: %v, want %v", got, row1)
	}
	mm.mu.Lock()
	mm.cache.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("2"), sqltypes.NULL}})
	mm.cache.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("3"), sqltypes.NULL}})
	mm.cond.Broadcast()
	mm.mu.Unlock()
	want = &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("2"),
			sqltypes.NULL,
		}, {
			sqltypes.NewVarBinary("3"),
			sqltypes.NULL,
		}},
	}
	if got := <-r1.ch; !reflect.DeepEqual(got, want) {
		t.Errorf("Received: %+v, want %+v", got, row1)
	}
}

func TestMessageManagerStreamerSimple(t *testing.T) {
	fvs := newFakeVStreamer()
	fvs.setStreamerResponse([][]*binlogdatapb.VEvent{{{
		// Event set 1.
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/33333333-3333-3333-3333-333333333333:1-100",
	}, {
		Type: binlogdatapb.VEventType_OTHER,
	}}, {{
		// Event set 2.
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: "foo",
			Fields:    testDBFields,
		},
	}}, {{
		// Event set 3.
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "foo",
			RowChanges: []*binlogdatapb.RowChange{{
				After: newMMRow(1),
			}},
		},
	}, {
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/33333333-3333-3333-3333-333333333333:1-101",
	}, {
		Type: binlogdatapb.VEventType_COMMIT,
	}}})
	mm := newMessageManager(newFakeTabletServer(), fvs, newMMTable(), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()

	r1 := newTestReceiver(1)
	mm.Subscribe(context.Background(), r1.rcv)
	<-r1.ch

	want := &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewVarBinary("1"),
		}},
	}
	if got := <-r1.ch; !reflect.DeepEqual(got, want) {
		t.Errorf("Received: %v, want %v", got, want)
	}
}

func TestMessageManagerStreamerAndPoller(t *testing.T) {
	fvs := newFakeVStreamer()
	fvs.setPollerResponse([]*binlogdatapb.VStreamResultsResponse{{
		Fields: testDBFields,
		Gtid:   "MySQL56/33333333-3333-3333-3333-333333333333:1-100",
	}})
	mm := newMessageManager(newFakeTabletServer(), fvs, newMMTable(), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()

	r1 := newTestReceiver(1)
	mm.Subscribe(context.Background(), r1.rcv)
	<-r1.ch

	for {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
		mm.streamMu.Lock()
		pos := mm.lastPollPosition
		mm.streamMu.Unlock()
		if pos != nil {
			break
		}
	}

	fvs.setStreamerResponse([][]*binlogdatapb.VEvent{{{
		// Event set 1: field info.
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: "foo",
			Fields:    testDBFields,
		},
	}}, {{
		// Event set 2: GTID won't be known till the first GTID event.
		// Row will not be added.
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "foo",
			RowChanges: []*binlogdatapb.RowChange{{
				After: newMMRow(1),
			}},
		},
	}, {
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/33333333-3333-3333-3333-333333333333:1-99",
	}, {
		Type: binlogdatapb.VEventType_COMMIT,
	}}, {{
		// Event set 3: GTID will be known, but <= last poll.
		// Row will not be added.
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "foo",
			RowChanges: []*binlogdatapb.RowChange{{
				After: newMMRow(2),
			}},
		},
	}, {
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/33333333-3333-3333-3333-333333333333:1-100",
	}, {
		Type: binlogdatapb.VEventType_COMMIT,
	}}, {{
		// Event set 3: GTID will be > last poll.
		// Row will be added.
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "foo",
			RowChanges: []*binlogdatapb.RowChange{{
				After: newMMRow(3),
			}},
		},
	}, {
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/33333333-3333-3333-3333-333333333333:1-101",
	}, {
		Type: binlogdatapb.VEventType_COMMIT,
	}}})

	want := &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(3),
			sqltypes.NewVarBinary("3"),
		}},
	}
	if got := <-r1.ch; !reflect.DeepEqual(got, want) {
		t.Errorf("Received: %v, want %v", got, want)
	}
}

func TestMessageManagerPoller(t *testing.T) {
	ti := newMMTable()
	ti.MessageInfo.BatchSize = 2
	ti.MessageInfo.PollInterval = 20 * time.Second
	fvs := newFakeVStreamer()
	fvs.setPollerResponse([]*binlogdatapb.VStreamResultsResponse{{
		Fields: testDBFields,
		Gtid:   "MySQL56/33333333-3333-3333-3333-333333333333:1-100",
	}, {
		Rows: []*querypb.Row{
			newMMRow(1),
			newMMRow(2),
			newMMRow(3),
		},
	}})
	mm := newMessageManager(newFakeTabletServer(), fvs, ti, sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	r1 := newTestReceiver(1)
	mm.Subscribe(ctx, r1.rcv)
	<-r1.ch

	want := [][]sqltypes.Value{{
		sqltypes.NewInt64(1),
		sqltypes.NewVarBinary("1"),
	}, {
		sqltypes.NewInt64(2),
		sqltypes.NewVarBinary("2"),
	}, {
		sqltypes.NewInt64(3),
		sqltypes.NewVarBinary("3"),
	}}
	var got [][]sqltypes.Value
	// We should get it in 2 iterations.
	for i := 0; i < 2; i++ {
		qr := <-r1.ch
		got = append(got, qr.Rows...)
	}
	for _, gotrow := range got {
		found := false
		for _, wantrow := range want {
			if reflect.DeepEqual(gotrow, wantrow) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("row: %v not found in %v", gotrow, want)
		}
	}

	// If there are no receivers, nothing should fire.
	cancel()
	runtime.Gosched()
	select {
	case row := <-r1.ch:
		t.Errorf("Expecting no value, got: %v", row)
	default:
	}
}

// TestMessagesPending1 tests for the case where you can't
// add items because the cache is full.
func TestMessagesPending1(t *testing.T) {
	// Set a large polling interval.
	ti := newMMTable()
	ti.MessageInfo.CacheSize = 2
	ti.MessageInfo.PollInterval = 30 * time.Second
	fvs := newFakeVStreamer()
	mm := newMessageManager(newFakeTabletServer(), fvs, ti, sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()

	r1 := newTestReceiver(0)
	go func() { <-r1.ch }()
	mm.Subscribe(context.Background(), r1.rcv)

	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("1")}})
	// Make sure the first message is enqueued.
	r1.WaitForCount(2)
	// This will fill up the cache.
	assert.True(t, mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("2")}}))
	assert.True(t, mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("3")}}))
	// This will fail and messagesPending will be set to true.
	assert.False(t, mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("4")}}))

	fvs.setPollerResponse([]*binlogdatapb.VStreamResultsResponse{{
		Fields: testDBFields,
		Gtid:   "MySQL56/33333333-3333-3333-3333-333333333333:1-100",
	}, {
		Rows: []*querypb.Row{newMMRow(1)},
	}})

	// Now, let's pull more than 3 items. It should
	// trigger the poller, and there should be no wait.
	start := time.Now()
	for i := 0; i < 4; i++ {
		<-r1.ch
	}
	if d := time.Since(start); d > 15*time.Second {
		t.Errorf("pending work trigger did not happen. Duration: %v", d)
	}
}

// TestMessagesPending2 tests for the case where
// there are more pending items than the cache size.
func TestMessagesPending2(t *testing.T) {
	// Set a large polling interval.
	ti := newMMTable()
	ti.MessageInfo.CacheSize = 1
	ti.MessageInfo.PollInterval = 30 * time.Second
	fvs := newFakeVStreamer()
	fvs.setPollerResponse([]*binlogdatapb.VStreamResultsResponse{{
		Fields: testDBFields,
		Gtid:   "MySQL56/33333333-3333-3333-3333-333333333333:1-100",
	}, {
		Rows: []*querypb.Row{newMMRow(1)},
	}})
	mm := newMessageManager(newFakeTabletServer(), fvs, ti, sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()

	r1 := newTestReceiver(0)
	go func() { <-r1.ch }()
	mm.Subscribe(context.Background(), r1.rcv)

	// Now, let's pull more than 1 item. It should
	// trigger the poller every time cache gets empty.
	start := time.Now()
	for i := 0; i < 3; i++ {
		<-r1.ch
	}
	if d := time.Since(start); d > 15*time.Second {
		t.Errorf("pending work trigger did not happen. Duration: %v", d)
	}
}

func TestMessageManagerPurge(t *testing.T) {
	tsv := newFakeTabletServer()

	// Make a buffered channel so the thread doesn't block on repeated calls.
	ch := make(chan string, 20)
	tsv.SetChannel(ch)

	ti := newMMTable()
	ti.MessageInfo.PollInterval = 1 * time.Millisecond
	mm := newMessageManager(tsv, newFakeVStreamer(), ti, sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()
	// Ensure Purge got called.
	if got, want := <-ch, "purge"; got != want {
		t.Errorf("Purge: %s, want %v", got, want)
	}
}

func TestMMGenerate(t *testing.T) {
	mm := newMessageManager(newFakeTabletServer(), newFakeVStreamer(), newMMTable(), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()
	query, bv := mm.GenerateAckQuery([]string{"1", "2"})
	wantQuery := "update foo set time_acked = :time_acked, time_next = null where id in ::ids and time_acked is null"
	if query != wantQuery {
		t.Errorf("GenerateAckQuery query: %s, want %s", query, wantQuery)
	}
	bvv, _ := sqltypes.BindVariableToValue(bv["time_acked"])
	gotAcked, _ := evalengine.ToInt64(bvv)
	wantAcked := time.Now().UnixNano()
	if wantAcked-gotAcked > 10e9 {
		t.Errorf("gotAcked: %d, should be with 10s of %d", gotAcked, wantAcked)
	}
	gotids := bv["ids"]
	wantids := sqltypes.TestBindVariable([]interface{}{"1", "2"})
	utils.MustMatch(t, wantids, gotids, "did not match")

	query, bv = mm.GeneratePostponeQuery([]string{"1", "2"})
	wantQuery = "update foo set time_next = :time_now + :wait_time + IF(FLOOR((:min_backoff<<ifnull(epoch, 0)) * :jitter) < :min_backoff, :min_backoff, FLOOR((:min_backoff<<ifnull(epoch, 0)) * :jitter)), epoch = ifnull(epoch, 0)+1 where id in ::ids and time_acked is null"
	if query != wantQuery {
		t.Errorf("GeneratePostponeQuery query: %s, want %s", query, wantQuery)
	}
	if _, ok := bv["time_now"]; !ok {
		t.Errorf("time_now is absent in %v", bv)
	} else {
		// time_now cannot be compared.
		delete(bv, "time_now")
	}
	if _, ok := bv["jitter"]; !ok {
		t.Errorf("jitter is absent in %v", bv)
	} else {
		// jitter cannot be compared.
		delete(bv, "jitter")
	}
	wantbv := map[string]*querypb.BindVariable{
		"wait_time":   sqltypes.Int64BindVariable(1e9),
		"min_backoff": sqltypes.Int64BindVariable(1e9),
		"ids":         wantids,
	}
	utils.MustMatch(t, wantbv, bv, "did not match")

	query, bv = mm.GeneratePurgeQuery(3)
	wantQuery = "delete from foo where time_acked < :time_acked limit 500"
	if query != wantQuery {
		t.Errorf("GeneratePurgeQuery query: %s, want %s", query, wantQuery)
	}
	wantbv = map[string]*querypb.BindVariable{
		"time_acked": sqltypes.Int64BindVariable(3),
	}
	if !reflect.DeepEqual(bv, wantbv) {
		t.Errorf("gotid: %v, want %v", bv, wantbv)
	}
}

func TestMMGenerateWithBackoff(t *testing.T) {
	mm := newMessageManager(newFakeTabletServer(), newFakeVStreamer(), newMMTableWithBackoff(), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()

	wantids := sqltypes.TestBindVariable([]interface{}{"1", "2"})

	query, bv := mm.GeneratePostponeQuery([]string{"1", "2"})
	wantQuery := "update foo set time_next = :time_now + :wait_time + IF(FLOOR((:min_backoff<<ifnull(epoch, 0)) * :jitter) < :min_backoff, :min_backoff, IF(FLOOR((:min_backoff<<ifnull(epoch, 0)) * :jitter) > :max_backoff, :max_backoff, FLOOR((:min_backoff<<ifnull(epoch, 0)) * :jitter))), epoch = ifnull(epoch, 0)+1 where id in ::ids and time_acked is null"
	if query != wantQuery {
		t.Errorf("GeneratePostponeQuery query: %s, want %s", query, wantQuery)
	}
	if _, ok := bv["time_now"]; !ok {
		t.Errorf("time_now is absent in %v", bv)
	} else {
		// time_now cannot be compared.
		delete(bv, "time_now")
	}
	if _, ok := bv["jitter"]; !ok {
		t.Errorf("jitter is absent in %v", bv)
	} else {
		// jitter cannot be compared.
		delete(bv, "jitter")
	}
	wantbv := map[string]*querypb.BindVariable{
		"wait_time":   sqltypes.Int64BindVariable(1e10),
		"min_backoff": sqltypes.Int64BindVariable(1e9),
		"max_backoff": sqltypes.Int64BindVariable(4e9),
		"ids":         wantids,
	}
	if !reflect.DeepEqual(bv, wantbv) {
		t.Errorf("gotid: %v, want %v", bv, wantbv)
	}
}

type fakeTabletServer struct {
	tabletenv.Env
	postponeCount sync2.AtomicInt64
	purgeCount    sync2.AtomicInt64

	mu sync.Mutex
	ch chan string
}

func newFakeTabletServer() *fakeTabletServer {
	config := tabletenv.NewDefaultConfig()
	return &fakeTabletServer{
		Env: tabletenv.NewEnv(config, "MessagerTest"),
	}
}

func (fts *fakeTabletServer) CheckMySQL() {}

func (fts *fakeTabletServer) SetChannel(ch chan string) {
	fts.mu.Lock()
	fts.ch = ch
	fts.mu.Unlock()
}

func (fts *fakeTabletServer) PostponeMessages(ctx context.Context, target *querypb.Target, name string, ids []string) (count int64, err error) {
	fts.postponeCount.Add(1)
	fts.mu.Lock()
	ch := fts.ch
	fts.mu.Unlock()
	if ch != nil {
		ch <- "postpone"
	}
	return 0, nil
}

func (fts *fakeTabletServer) PurgeMessages(ctx context.Context, target *querypb.Target, name string, timeCutoff int64) (count int64, err error) {
	fts.purgeCount.Add(1)
	fts.mu.Lock()
	ch := fts.ch
	fts.mu.Unlock()
	if ch != nil {
		ch <- "purge"
	}
	return 0, nil
}

type fakeVStreamer struct {
	streamInvocations sync2.AtomicInt64
	mu                sync.Mutex
	streamerResponse  [][]*binlogdatapb.VEvent
	pollerResponse    []*binlogdatapb.VStreamResultsResponse
}

func newFakeVStreamer() *fakeVStreamer { return &fakeVStreamer{} }

func (fv *fakeVStreamer) setStreamerResponse(sr [][]*binlogdatapb.VEvent) {
	fv.mu.Lock()
	defer fv.mu.Unlock()
	fv.streamerResponse = sr
}

func (fv *fakeVStreamer) setPollerResponse(pr []*binlogdatapb.VStreamResultsResponse) {
	fv.mu.Lock()
	defer fv.mu.Unlock()
	fv.pollerResponse = pr
}

func (fv *fakeVStreamer) Stream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	fv.streamInvocations.Add(1)
	for {
		fv.mu.Lock()
		sr := fv.streamerResponse
		fv.streamerResponse = nil
		fv.mu.Unlock()
		for _, r := range sr {
			if err := send(r); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return io.EOF
		default:
		}
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
}

func (fv *fakeVStreamer) StreamResults(ctx context.Context, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	fv.mu.Lock()
	defer fv.mu.Unlock()
	for _, r := range fv.pollerResponse {
		if err := send(r); err != nil {
			return err
		}
	}
	return nil
}
