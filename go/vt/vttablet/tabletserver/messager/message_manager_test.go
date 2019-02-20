/*
Copyright 2017 Google Inc.

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
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	testFields = []*querypb.Field{{
		Name: "id",
		Type: sqltypes.VarBinary,
	}, {
		Name: "time_scheduled",
		Type: sqltypes.Int64,
	}, {
		Name: "message",
		Type: sqltypes.VarBinary,
	}}

	mmTable = &schema.Table{
		Name: sqlparser.NewTableIdent("foo"),
		Type: schema.Message,
		MessageInfo: &schema.MessageInfo{
			Fields:             testFields,
			AckWaitDuration:    1 * time.Second,
			PurgeAfterDuration: 3 * time.Second,
			BatchSize:          1,
			CacheSize:          10,
			PollInterval:       1 * time.Second,
		},
	}
)

// newMMTable is not a true copy, but enough to massage what we need.
func newMMTable() *schema.Table {
	ti := *mmTable
	msg := *ti.MessageInfo
	ti.MessageInfo = &msg
	return &ti
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
	db := fakesqldb.New(t)
	defer db.Close()
	mm := newMessageManager(newFakeTabletServer(), mmTable, newMMConnPool(db), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(0)
	ctx, cancel := context.WithCancel(context.Background())
	_ = mm.Subscribe(ctx, r1.rcv)
	cancel()
	// r1 should eventually be unsubscribed.
	for i := 0; i < 10; i++ {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
		mm.mu.Lock()
		if len(mm.receivers) != 0 {
			mm.mu.Unlock()
			continue
		}
		mm.mu.Unlock()
		return
	}
	t.Errorf("receivers were not cleared: %d", len(mm.receivers))
}

func TestMessageManagerState(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	mm := newMessageManager(newFakeTabletServer(), mmTable, newMMConnPool(db), sync2.NewSemaphore(1, 0))
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

	for i := 0; i < 2; i++ {
		mm.Open()
		r1 := newTestReceiver(1)
		mm.Subscribe(context.Background(), r1.rcv)
		// This time the wait is in a different code path.
		runtime.Gosched()
		mm.Close()
	}
}

func TestMessageManagerAdd(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	ti := newMMTable()
	ti.MessageInfo.CacheSize = 1
	mm := newMessageManager(newFakeTabletServer(), ti, newMMConnPool(db), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()

	row1 := &MessageRow{
		Row: []sqltypes.Value{sqltypes.NewVarBinary("1")},
	}
	if mm.Add(row1) {
		t.Error("Add(no receivers): true, want false")
	}

	r1 := newTestReceiver(0)
	mm.Subscribe(context.Background(), r1.rcv)
	<-r1.ch
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
	db := fakesqldb.New(t)
	defer db.Close()
	tsv := newFakeTabletServer()
	mm := newMessageManager(tsv, mmTable, newMMConnPool(db), sync2.NewSemaphore(1, 0))
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
	mm.cache.mu.Lock()
	if _, ok := mm.cache.inQueue["1"]; ok {
		t.Error("Message 1 is still present in inQueue cache")
	}
	if _, ok := mm.cache.inFlight["1"]; ok {
		t.Error("Message 1 is still present in inFlight cache")
	}
	mm.cache.mu.Unlock()

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
	db := fakesqldb.New(t)
	defer db.Close()
	tsv := newFakeTabletServer()
	mm := newMessageManager(tsv, mmTable, newMMConnPool(db), sync2.NewSemaphore(1, 0))
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

func TestMessageManagerSendEOF(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQueryPattern(
		"select time_next, epoch, time_created, id, time_scheduled, message from foo.*",
		&sqltypes.Result{
			Fields: []*querypb.Field{
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.VarBinary},
			},
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(0),
				sqltypes.NewInt64(2),
				sqltypes.NewInt64(10),
				sqltypes.NewVarBinary("a"),
			}},
		},
	)
	// Set a large polling interval.
	ti := newMMTable()
	ti.MessageInfo.CacheSize = 2
	ti.MessageInfo.PollInterval = 30 * time.Second
	mm := newMessageManager(newFakeTabletServer(), ti, newMMConnPool(db), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(0)
	ctx, cancel := context.WithCancel(context.Background())
	mm.Subscribe(ctx, r1.rcv)
	// Pull field info.
	<-r1.ch

	r2 := newTestReceiver(0)
	mm.Subscribe(context.Background(), r2.rcv)
	// Pull field info.
	<-r2.ch

	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("1"), sqltypes.NULL}})
	// Wait for send to enqueue.
	// After this is enquened, runSend will go into a wait state because
	// there's nothing more to send.
	r1.WaitForCount(2)

	// Now cancel, which will send an EOF to the sender.
	cancel()

	// The EOF should immediately trigger the poller, which will result
	// in a message being sent on r2. Verify that this happened in a timely fashion.
	start := time.Now()
	<-r2.ch
	if d := time.Since(start); d > 15*time.Second {
		t.Errorf("pending work trigger did not happen. Duration: %v", d)
	}
}

func TestMessageManagerSendError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	tsv := newFakeTabletServer()
	mm := newMessageManager(tsv, mmTable, newMMConnPool(db), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()
	ctx := context.Background()

	ch := make(chan *sqltypes.Result)
	fieldSent := false
	mm.Subscribe(ctx, func(qr *sqltypes.Result) error {
		ch <- qr
		if !fieldSent {
			fieldSent = true
			return nil
		}
		return errors.New("non-eof")
	})
	// Pull field info.
	<-ch

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
	db := fakesqldb.New(t)
	defer db.Close()
	tsv := newFakeTabletServer()
	mm := newMessageManager(tsv, mmTable, newMMConnPool(db), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()
	ctx := context.Background()

	ch := make(chan *sqltypes.Result)
	done := mm.Subscribe(ctx, func(qr *sqltypes.Result) error {
		ch <- qr
		return errors.New("non-eof")
	})
	// Pull field info.
	<-ch

	// This should not hang because a field send error must terminate
	// subscription.
	<-done
}

func TestMessageManagerBatchSend(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	ti := newMMTable()
	ti.MessageInfo.BatchSize = 2
	mm := newMessageManager(newFakeTabletServer(), ti, newMMConnPool(db), sync2.NewSemaphore(1, 0))
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

func TestMessageManagerPoller(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQueryPattern(
		"select time_next, epoch, time_created, id, time_scheduled, message from foo.*",
		&sqltypes.Result{
			Fields: []*querypb.Field{
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.VarBinary},
			},
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(0),
				sqltypes.NewInt64(0),
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(10),
				sqltypes.NewVarBinary("01"),
			}, {
				sqltypes.NewInt64(2),
				sqltypes.NewInt64(0),
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(2),
				sqltypes.NewInt64(20),
				sqltypes.NewVarBinary("02"),
			}, {
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(0),
				sqltypes.NewInt64(3),
				sqltypes.NewInt64(30),
				sqltypes.NewVarBinary("11"),
			}},
		},
	)
	ti := newMMTable()
	ti.MessageInfo.BatchSize = 2
	ti.MessageInfo.PollInterval = 20 * time.Second
	mm := newMessageManager(newFakeTabletServer(), ti, newMMConnPool(db), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(1)
	ctx, cancel := context.WithCancel(context.Background())
	mm.Subscribe(ctx, r1.rcv)
	<-r1.ch
	mm.pollerTicks.Trigger()
	want := [][]sqltypes.Value{{
		sqltypes.NewInt64(2),
		sqltypes.NewInt64(20),
		sqltypes.NewVarBinary("02"),
	}, {
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(10),
		sqltypes.NewVarBinary("01"),
	}, {
		sqltypes.NewInt64(3),
		sqltypes.NewInt64(30),
		sqltypes.NewVarBinary("11"),
	}}
	var got [][]sqltypes.Value
	// We should get it in 2 iterations.
	for i := 0; i < 2; i++ {
		qr := <-r1.ch
		got = append(got, qr.Rows...)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("rows:\n%+v, want\n%+v", got, want)
	}

	// If there are no receivers, nothing should fire.
	cancel()
	mm.pollerTicks.Trigger()
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
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQueryPattern(
		"select time_next, epoch, time_created, id, time_scheduled, message from foo.*",
		&sqltypes.Result{
			Fields: []*querypb.Field{
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.VarBinary},
			},
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(0),
				sqltypes.NewInt64(2),
				sqltypes.NewInt64(10),
				sqltypes.NewVarBinary("a"),
			}},
		},
	)
	// Set a large polling interval.
	ti := newMMTable()
	ti.MessageInfo.CacheSize = 2
	ti.MessageInfo.PollInterval = 30 * time.Second
	mm := newMessageManager(newFakeTabletServer(), ti, newMMConnPool(db), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(0)
	mm.Subscribe(context.Background(), r1.rcv)
	<-r1.ch

	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("1")}})
	// Make sure the first message is enqueued.
	r1.WaitForCount(2)
	// This will fill up the cache.
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("2")}})
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("3")}})

	// Trigger the poller. It should do nothing.
	mm.pollerTicks.Trigger()

	// Wait for pending flag to be turned on.
	for {
		runtime.Gosched()
		mm.mu.Lock()
		if mm.messagesPending {
			mm.mu.Unlock()
			break
		}
		mm.mu.Unlock()
	}

	// Now, let's pull more than 3 items. It should
	// trigger the poller, and there should be no wait.
	start := time.Now()
	for i := 0; i < 4; i++ {
		<-r1.ch
	}
	if d := time.Now().Sub(start); d > 15*time.Second {
		t.Errorf("pending work trigger did not happen. Duration: %v", d)
	}
}

// TestMessagesPending2 tests for the case where
// there are more pending items than the cache size.
func TestMessagesPending2(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQueryPattern(
		"select time_next, epoch, time_created, id, time_scheduled, message from foo.*",
		&sqltypes.Result{
			Fields: []*querypb.Field{
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.Int64},
				{Type: sqltypes.VarBinary},
			},
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(0),
				sqltypes.NewInt64(2),
				sqltypes.NewInt64(10),
				sqltypes.NewVarBinary("a"),
			}},
		},
	)
	// Set a large polling interval.
	ti := newMMTable()
	ti.MessageInfo.CacheSize = 1
	ti.MessageInfo.PollInterval = 30 * time.Second
	mm := newMessageManager(newFakeTabletServer(), ti, newMMConnPool(db), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(0)
	mm.Subscribe(context.Background(), r1.rcv)
	<-r1.ch

	// Trigger the poller.
	mm.pollerTicks.Trigger()

	// Now, let's pull more than 1 item. It should
	// trigger the poller every time cache gets empty.
	start := time.Now()
	for i := 0; i < 3; i++ {
		<-r1.ch
	}
	if d := time.Now().Sub(start); d > 15*time.Second {
		t.Errorf("pending work trigger did not happen. Duration: %v", d)
	}
}

func TestMessageManagerPurge(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	tsv := newFakeTabletServer()

	// Make a buffered channel so the thread doesn't block on repeated calls.
	ch := make(chan string, 20)
	tsv.SetChannel(ch)

	ti := newMMTable()
	ti.MessageInfo.PollInterval = 1 * time.Millisecond
	mm := newMessageManager(tsv, ti, newMMConnPool(db), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()
	// Ensure Purge got called.
	if got, want := <-ch, "purge"; got != want {
		t.Errorf("Purge: %s, want %v", got, want)
	}
}

func TestMMGenerate(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	mm := newMessageManager(newFakeTabletServer(), mmTable, newMMConnPool(db), sync2.NewSemaphore(1, 0))
	mm.Open()
	defer mm.Close()
	query, bv := mm.GenerateAckQuery([]string{"1", "2"})
	wantQuery := "update foo set time_acked = :time_acked, time_next = null where id in ::ids and time_acked is null"
	if query != wantQuery {
		t.Errorf("GenerateAckQuery query: %s, want %s", query, wantQuery)
	}
	bvv, _ := sqltypes.BindVariableToValue(bv["time_acked"])
	gotAcked, _ := sqltypes.ToInt64(bvv)
	wantAcked := time.Now().UnixNano()
	if wantAcked-gotAcked > 10e9 {
		t.Errorf("gotAcked: %d, should be with 10s of %d", gotAcked, wantAcked)
	}
	gotids := bv["ids"]
	wantids := sqltypes.TestBindVariable([]interface{}{"1", "2"})
	if !reflect.DeepEqual(gotids, wantids) {
		t.Errorf("gotid: %v, want %v", gotids, wantids)
	}

	query, bv = mm.GeneratePostponeQuery([]string{"1", "2"})
	wantQuery = "update foo set time_next = :time_now+(:wait_time<<epoch), epoch = epoch+1 where id in ::ids and time_acked is null"
	if query != wantQuery {
		t.Errorf("GeneratePostponeQuery query: %s, want %s", query, wantQuery)
	}
	if _, ok := bv["time_now"]; !ok {
		t.Errorf("time_now is absent in %v", bv)
	} else {
		// time_now cannot be compared.
		delete(bv, "time_now")
	}
	wantbv := map[string]*querypb.BindVariable{
		"wait_time": sqltypes.Int64BindVariable(1e9),
		"ids":       wantids,
	}
	if !reflect.DeepEqual(bv, wantbv) {
		t.Errorf("gotid: %v, want %v", bv, wantbv)
	}

	query, bv = mm.GeneratePurgeQuery(3)
	wantQuery = "delete from foo where time_scheduled < :time_scheduled and time_acked is not null limit 500"
	if query != wantQuery {
		t.Errorf("GeneratePurgeQuery query: %s, want %s", query, wantQuery)
	}
	wantbv = map[string]*querypb.BindVariable{
		"time_scheduled": sqltypes.Int64BindVariable(3),
	}
	if !reflect.DeepEqual(bv, wantbv) {
		t.Errorf("gotid: %v, want %v", bv, wantbv)
	}
}

type fakeTabletServer struct {
	postponeCount sync2.AtomicInt64
	purgeCount    sync2.AtomicInt64

	mu sync.Mutex
	ch chan string
}

func newFakeTabletServer() *fakeTabletServer { return &fakeTabletServer{} }

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

func newMMConnPool(db *fakesqldb.DB) *connpool.Pool {
	pool := connpool.New("", 20, time.Duration(10*time.Minute), 0, newFakeTabletServer())
	dbconfigs := dbconfigs.NewTestDBConfigs(*db.ConnParams(), *db.ConnParams(), "")
	pool.Open(dbconfigs.AppWithDB(), dbconfigs.DbaWithDB(), dbconfigs.AppDebugWithDB())
	return pool
}
