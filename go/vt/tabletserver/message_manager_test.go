// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
)

type testReceiver struct {
	ctx    context.Context
	cancel context.CancelFunc
	count  sync2.AtomicInt64
	name   string
	ch     chan *MessageRow
}

func newTestReceiver(size int) *testReceiver {
	ctx, cancel := context.WithCancel(context.Background())
	return &testReceiver{
		ctx:    ctx,
		cancel: cancel,
		ch:     make(chan *MessageRow, size),
	}
}

func (tr *testReceiver) Send(name string, mr *MessageRow) error {
	tr.count.Add(1)
	tr.name = name
	select {
	case tr.ch <- mr:
	case <-tr.ctx.Done():
	}
	return nil
}

func (tr *testReceiver) Cancel() {
	tr.cancel()
}

func (tr *testReceiver) WaitForCount(n int) {
	for {
		time.Sleep(10 * time.Nanosecond)
		if tr.count.Get() == int64(n) {
			return
		}
	}
}

func TestMessageManagerState(t *testing.T) {
	db := setUpTabletServerTest()
	mm := NewMessageManager("foo", 10, 1*time.Second, newMMConnPool(db))
	// Do it twice
	for i := 0; i < 2; i++ {
		mm.Open()
		// Idempotence.
		mm.Open()
		// The sleep is for making sure runSend starts up
		// and waits on cond.
		time.Sleep(10 * time.Millisecond)
		mm.Close()
		// Idempotence.
		mm.Close()
	}

	for i := 0; i < 2; i++ {
		mm.Open()
		r1 := newTestReceiver(1)
		mm.Subscribe(r1)
		// This time the wait is in a different code path.
		time.Sleep(10 * time.Millisecond)
		mm.Close()
		// This should not hang
		<-r1.ctx.Done()
	}
}

func TestMessageManagerAdd(t *testing.T) {
	db := setUpTabletServerTest()
	mm := NewMessageManager("foo", 1, 1*time.Second, newMMConnPool(db))
	mm.Open()
	defer mm.Close()

	row1 := &MessageRow{
		id: "1",
	}
	if mm.Add(row1) {
		t.Error("Add(no receivers): true, want false")
	}

	r1 := newTestReceiver(0)
	mm.Subscribe(r1)
	if !mm.Add(row1) {
		t.Error("Add(1 receiver): false, want true")
	}
	// Make sure message is enqueued.
	r1.WaitForCount(1)
	// This will fill up the cache.
	mm.Add(&MessageRow{id: "2"})

	// The third add has to fail.
	if mm.Add(&MessageRow{id: "3"}) {
		t.Error("Add(cache full): true, want false")
	}
}

func TestMessageManagerSend(t *testing.T) {
	db := setUpTabletServerTest()
	mm := NewMessageManager("foo", 10, 1*time.Second, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(1)
	mm.Subscribe(r1)
	row1 := &MessageRow{
		id: "1",
	}
	mm.Add(row1)
	if got := <-r1.ch; got != row1 {
		t.Errorf("Received: %v, want %v", got, row1)
	}
	r2 := newTestReceiver(1)
	mm.Subscribe(r2)
	// Subscribing twice is a no-op.
	mm.Subscribe(r1)
	// Unsubscribe of non-registered receiver should be no-op.
	mm.Unsubscribe(newTestReceiver(1))
	row2 := &MessageRow{
		id: "2",
	}
	mm.Add(row1)
	mm.Add(row2)
	// Send should be round-robin.
	<-r1.ch
	<-r2.ch
	mm.Unsubscribe(r1)
	mm.Add(row1)
	mm.Add(row2)
	// Only r2 should be receiving.
	<-r2.ch
	<-r2.ch
	mm.Unsubscribe(r2)
}

func TestMessageManagerPoller(t *testing.T) {
	db := setUpTabletServerTest()
	db.AddQueryPattern(
		"select time_next, epoch, id, message from foo.*",
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{{
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("0")),
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("01")),
			}, {
				sqltypes.MakeString([]byte("2")),
				sqltypes.MakeString([]byte("0")),
				sqltypes.MakeString([]byte("2")),
				sqltypes.MakeString([]byte("02")),
			}, {
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("3")),
				sqltypes.MakeString([]byte("11")),
			}},
		},
	)
	mm := NewMessageManager("foo", 10, 1*time.Second, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(1)
	mm.Subscribe(r1)
	mm.ticks.Trigger()
	want := []MessageRow{{
		TimeNext: 2,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("2")),
		Message:  sqltypes.MakeString([]byte("02")),
		id:       "2",
	}, {
		TimeNext: 1,
		Epoch:    0,
		ID:       sqltypes.MakeString([]byte("1")),
		Message:  sqltypes.MakeString([]byte("01")),
		id:       "1",
	}, {
		TimeNext: 1,
		Epoch:    1,
		ID:       sqltypes.MakeString([]byte("3")),
		Message:  sqltypes.MakeString([]byte("11")),
		id:       "3",
	}}
	var got []MessageRow
	for i := 0; i < 3; i++ {
		got = append(got, *(<-r1.ch))
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("rows:\n%+v, want\n%+v", got, want)
	}

	// If there are no receivers, nothing should fire.
	mm.Unsubscribe(r1)
	mm.ticks.Trigger()
	time.Sleep(10 * time.Microsecond)
	select {
	case row := <-r1.ch:
		t.Errorf("Expecting no value, got: %v", row)
	default:
	}
}

// TestMessagesPending1 tests for the case where you can't
// add items because the cache is full.
func TestMessagesPending1(t *testing.T) {
	db := setUpTabletServerTest()
	db.AddQueryPattern(
		"select time_next, epoch, id, message from foo.*",
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{{
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("0")),
				sqltypes.MakeString([]byte("a")),
				sqltypes.MakeString([]byte("a")),
			}},
		},
	)
	// Set a large polling interval.
	mm := NewMessageManager("foo", 2, 30*time.Second, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(0)
	mm.Subscribe(r1)

	mm.Add(&MessageRow{id: "1"})
	// Make sure the first message is enqueued.
	r1.WaitForCount(1)
	// This will fill up the cache.
	mm.Add(&MessageRow{id: "2"})
	mm.Add(&MessageRow{id: "3"})

	// Trigger the poller. It should do nothing.
	mm.ticks.Trigger()

	// Wait for pending flag to be turned on.
	for {
		time.Sleep(10 * time.Nanosecond)
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
	db := setUpTabletServerTest()
	db.AddQueryPattern(
		"select time_next, epoch, id, message from foo.*",
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{{
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte("0")),
				sqltypes.MakeString([]byte("a")),
				sqltypes.MakeString([]byte("a")),
			}},
		},
	)
	// Set a large polling interval.
	mm := NewMessageManager("foo", 1, 30*time.Second, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(0)
	mm.Subscribe(r1)

	// Trigger the poller.
	mm.ticks.Trigger()

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

func newMMConnPool(db *fakesqldb.DB) *ConnPool {
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	pool := NewConnPool(
		config.PoolNamePrefix+"MesasgeConnPool",
		20,
		time.Duration(config.IdleTimeout*1e9),
		config.EnablePublishStats,
		tsv.queryServiceStats,
		tsv,
	)
	pool.Open(&dbconfigs.App, &dbconfigs.Dba)
	return pool
}
