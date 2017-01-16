// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"io"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

type testReceiver struct {
	rcv   *messageReceiver
	done  chan struct{}
	count sync2.AtomicInt64
	name  string
	ch    chan []*MessageRow
}

func newTestReceiver(size int) *testReceiver {
	tr := &testReceiver{
		ch: make(chan []*MessageRow, size),
	}
	tr.rcv, tr.done = newMessageReceiver(func(name string, mrs []*MessageRow) error {
		select {
		case <-tr.done:
			return io.EOF
		default:
		}
		tr.count.Add(1)
		tr.name = name
		tr.ch <- mrs
		return nil
	})
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

func (tr *testReceiver) WaitForDone() {
	for {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
		select {
		case <-tr.done:
			return
		default:
		}
	}
}

func TestMessageManagerState(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	mm := NewMessageManager(tsv, "foo", 1*time.Second, 3*time.Second, 1, 10, 1*time.Second, newMMConnPool(db))
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
		mm.Subscribe(r1.rcv)
		// This time the wait is in a different code path.
		runtime.Gosched()
		mm.Close()
	}
}

func TestMessageManagerAdd(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	mm := NewMessageManager(tsv, "foo", 1*time.Second, 3*time.Second, 1, 1, 1*time.Second, newMMConnPool(db))
	mm.Open()
	defer mm.Close()

	row1 := &MessageRow{
		id: "1",
	}
	if mm.Add(row1) {
		t.Error("Add(no receivers): true, want false")
	}

	r1 := newTestReceiver(0)
	mm.Subscribe(r1.rcv)
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
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	mm := NewMessageManager(tsv, "foo", 1*time.Second, 3*time.Second, 1, 10, 1*time.Second, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(1)
	mm.Subscribe(r1.rcv)
	row1 := &MessageRow{
		id: "1",
	}
	mm.Add(row1)
	if got := <-r1.ch; !reflect.DeepEqual(got, []*MessageRow{row1}) {
		t.Errorf("Received: %v, want %v", got, row1)
	}
	r2 := newTestReceiver(1)
	mm.Subscribe(r2.rcv)
	mm.Add(&MessageRow{id: "2"})
	mm.Add(&MessageRow{id: "3"})
	// Send should be round-robin.
	<-r1.ch
	<-r2.ch
	r2.rcv.Cancel()
	r2.WaitForDone()
	// One of these messages will fail to send
	// because r1 will return EOF.
	mm.Add(&MessageRow{id: "4"})
	mm.Add(&MessageRow{id: "5"})
	mm.Add(&MessageRow{id: "6"})
	// Only r2 should be receiving.
	<-r1.ch
	<-r1.ch
}

func TestMessageManagerBatchSend(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	mm := NewMessageManager(tsv, "foo", 1*time.Second, 3*time.Second, 2, 10, 1*time.Second, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(1)
	mm.Subscribe(r1.rcv)
	row1 := &MessageRow{
		id: "1",
	}
	mm.Add(row1)
	want := []*MessageRow{row1}
	if got := <-r1.ch; !reflect.DeepEqual(got, want) {
		t.Errorf("Received: %v, want %v", got, row1)
	}
	mm.mu.Lock()
	mm.cache.Add(&MessageRow{id: "2"})
	mm.cache.Add(&MessageRow{id: "3"})
	mm.cond.Broadcast()
	mm.mu.Unlock()
	want = []*MessageRow{
		{id: "2"},
		{id: "3"},
	}
	if got := <-r1.ch; !reflect.DeepEqual(got, want) {
		t.Errorf("Received: %+v, want %+v", got, row1)
	}
}

func TestMessageManagerPoller(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
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
	mm := NewMessageManager(tsv, "foo", 1*time.Second, 3*time.Second, 2, 10, 20*time.Second, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(1)
	mm.Subscribe(r1.rcv)
	mm.pollerTicks.Trigger()
	want := []*MessageRow{{
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
	var got []*MessageRow
	// We should get it in 2 iterations.
	for i := 0; i < 2; i++ {
		mrs := <-r1.ch
		got = append(got, mrs...)
		fmt.Printf("got: %v\n", got)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("rows:\n%v, want\n%v", got, want)
	}

	// If there are no receivers, nothing should fire.
	r1.rcv.Cancel()
	r1.WaitForDone()
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
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
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
	mm := NewMessageManager(tsv, "foo", 1*time.Second, 3*time.Second, 1, 2, 30*time.Second, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(0)
	mm.Subscribe(r1.rcv)

	mm.Add(&MessageRow{id: "1"})
	// Make sure the first message is enqueued.
	r1.WaitForCount(1)
	// This will fill up the cache.
	mm.Add(&MessageRow{id: "2"})
	mm.Add(&MessageRow{id: "3"})

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
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
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
	mm := NewMessageManager(tsv, "foo", 1*time.Second, 3*time.Second, 1, 1, 30*time.Second, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(0)
	mm.Subscribe(r1.rcv)

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

func TestMMGenerate(t *testing.T) {
	db := setUpTabletServerTest()
	testUtils := newTestUtils()
	config := testUtils.newQueryServiceConfig()
	config.TransactionCap = 1
	tsv := NewTabletServer(config)
	dbconfigs := testUtils.newDBConfigs(db)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err := tsv.StartService(target, dbconfigs, testUtils.newMysqld(&dbconfigs))
	if err != nil {
		t.Fatalf("StartService failed: %v", err)
	}
	defer tsv.StopService()
	mm := NewMessageManager(tsv, "foo", 1*time.Second, 3*time.Second, 1, 10, 1*time.Second, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	query, bv := mm.GenerateAckQuery([]string{"1", "2"})
	wantQuery := "update foo set time_acked = :time_acked, time_next = null where id in ::ids"
	if query != wantQuery {
		t.Errorf("GenerateAckQuery query: %s, want %s", query, wantQuery)
	}
	gotAcked := bv["time_acked"].(int64)
	wantAcked := time.Now().UnixNano()
	if wantAcked-gotAcked > 10e9 {
		t.Errorf("gotAcked: %d, should be with 10s of %d", gotAcked, wantAcked)
	}
	wantids := []interface{}{"1", "2"}
	gotids := bv["ids"].([]interface{})
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
	wantbv := map[string]interface{}{
		"wait_time": int64(1e9),
		"ids":       []interface{}{"1", "2"},
	}
	if !reflect.DeepEqual(bv, wantbv) {
		t.Errorf("gotid: %v, want %v", bv, wantbv)
	}

	query, bv = mm.GeneratePurgeQuery(3)
	wantQuery = "delete from foo where time_scheduled < :time_scheduled and time_acked is not null limit 500"
	if query != wantQuery {
		t.Errorf("GeneratePurgeQuery query: %s, want %s", query, wantQuery)
	}
	wantbv = map[string]interface{}{
		"time_scheduled": int64(3),
	}
	if !reflect.DeepEqual(bv, wantbv) {
		t.Errorf("gotid: %v, want %v", bv, wantbv)
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
