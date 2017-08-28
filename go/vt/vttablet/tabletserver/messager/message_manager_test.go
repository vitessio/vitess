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
	"io"
	"reflect"
	"runtime"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
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
	rcv   *messageReceiver
	done  chan struct{}
	count sync2.AtomicInt64
	ch    chan *sqltypes.Result
}

func newTestReceiver(size int) *testReceiver {
	tr := &testReceiver{
		ch: make(chan *sqltypes.Result, size),
	}
	tr.rcv, tr.done = newMessageReceiver(func(qr *sqltypes.Result) error {
		select {
		case <-tr.done:
			return io.EOF
		default:
		}
		tr.count.Add(1)
		select {
		case tr.ch <- qr:
		case <-time.After(20 * time.Second):
			panic("test may be hung")
		}
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

func TestReceiverEOF(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	mm := newMessageManager(newFakeTabletServer(), mmTable, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(0)
	r1.done = make(chan struct{})
	mm.Subscribe(r1.rcv)
	close(r1.done)
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
	mm := newMessageManager(newFakeTabletServer(), mmTable, newMMConnPool(db))
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
	db := fakesqldb.New(t)
	defer db.Close()
	ti := newMMTable()
	ti.MessageInfo.CacheSize = 1
	mm := newMessageManager(newFakeTabletServer(), ti, newMMConnPool(db))
	mm.Open()
	defer mm.Close()

	row1 := &MessageRow{
		Row: []sqltypes.Value{sqltypes.NewVarBinary("1")},
	}
	if mm.Add(row1) {
		t.Error("Add(no receivers): true, want false")
	}

	r1 := newTestReceiver(0)
	mm.Subscribe(r1.rcv)
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
	// Drain the receiver to prevent hangs.
	go func() {
		for range r1.ch {
		}
	}()
}

func TestMessageManagerSend(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	tsv := newFakeTabletServer()
	mm := newMessageManager(tsv, mmTable, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(1)
	mm.Subscribe(r1.rcv)
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
	if got := <-ch; got != mmTable.Name.String() {
		t.Errorf("Postpone: %s, want %v", got, mmTable.Name)
	}

	// Verify item has been removed from cache.
	if _, ok := mm.cache.messages["1"]; ok {
		t.Error("Message 1 is still present in cache")
	}

	r2 := newTestReceiver(1)
	mm.Subscribe(r2.rcv)
	<-r2.ch
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("2")}})
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("3")}})
	// Send should be round-robin.
	<-r1.ch
	<-r2.ch
	r2.rcv.Cancel()
	r2.WaitForDone()
	// One of these messages will fail to send
	// because r1 will return EOF.
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("4")}})
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("5")}})
	mm.Add(&MessageRow{Row: []sqltypes.Value{sqltypes.NewVarBinary("6")}})
	// Only r1 should be receiving.
	<-r1.ch
	<-r1.ch
}

func TestMessageManagerBatchSend(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	ti := newMMTable()
	ti.MessageInfo.BatchSize = 2
	mm := newMessageManager(newFakeTabletServer(), ti, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(1)
	mm.Subscribe(r1.rcv)
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
		"select time_next, epoch, id, time_scheduled, message from foo.*",
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
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(10),
				sqltypes.NewVarBinary("01"),
			}, {
				sqltypes.NewInt64(2),
				sqltypes.NewInt64(0),
				sqltypes.NewInt64(2),
				sqltypes.NewInt64(20),
				sqltypes.NewVarBinary("02"),
			}, {
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(1),
				sqltypes.NewInt64(3),
				sqltypes.NewInt64(30),
				sqltypes.NewVarBinary("11"),
			}},
		},
	)
	ti := newMMTable()
	ti.MessageInfo.BatchSize = 2
	ti.MessageInfo.PollInterval = 20 * time.Second
	mm := newMessageManager(newFakeTabletServer(), ti, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(1)
	mm.Subscribe(r1.rcv)
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
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQueryPattern(
		"select time_next, epoch, id, time_scheduled, message from foo.*",
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
	mm := newMessageManager(newFakeTabletServer(), ti, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(0)
	mm.Subscribe(r1.rcv)
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
		"select time_next, epoch, id, time_scheduled, message from foo.*",
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
	mm := newMessageManager(newFakeTabletServer(), ti, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver(0)
	mm.Subscribe(r1.rcv)
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
	// Consume the rest of the messages asynchronously to
	// prevent hangs.
	go func() {
		for range r1.ch {
		}
	}()
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
	mm := newMessageManager(tsv, ti, newMMConnPool(db))
	mm.Open()
	defer mm.Close()
	// Ensure Purge got called.
	if got := <-ch; got != mmTable.Name.String() {
		t.Errorf("Postpone: %s, want %v", got, mmTable.Name)
	}
}

func TestMMGenerate(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	mm := newMessageManager(newFakeTabletServer(), mmTable, newMMConnPool(db))
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
	ch chan string
}

func newFakeTabletServer() *fakeTabletServer { return &fakeTabletServer{} }

func (fts *fakeTabletServer) CheckMySQL() {}

func (fts *fakeTabletServer) SetChannel(ch chan string) {
	fts.ch = ch
}

func (fts *fakeTabletServer) PostponeMessages(ctx context.Context, target *querypb.Target, name string, ids []string) (count int64, err error) {
	if fts.ch != nil {
		fts.ch <- name
	}
	return 0, nil
}

func (fts *fakeTabletServer) PurgeMessages(ctx context.Context, target *querypb.Target, name string, timeCutoff int64) (count int64, err error) {
	if fts.ch != nil {
		fts.ch <- name
	}
	return 0, nil
}

func newMMConnPool(db *fakesqldb.DB) *connpool.Pool {
	pool := connpool.New("", 20, time.Duration(10*time.Minute), newFakeTabletServer())
	dbconfigs := dbconfigs.DBConfigs{
		App:           *db.ConnParams(),
		SidecarDBName: "_vt",
	}
	pool.Open(&dbconfigs.App, &dbconfigs.Dba, &dbconfigs.AppDebug)
	return pool
}
