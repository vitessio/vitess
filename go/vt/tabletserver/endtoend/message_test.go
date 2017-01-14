// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
)

type testReceiver struct {
	name string
	ch   chan *tabletserver.MessageRow
}

func (tr *testReceiver) Send(name string, mr *tabletserver.MessageRow) error {
	tr.name = name
	tr.ch <- mr
	return nil
}

func (tr *testReceiver) Cancel() {
}

func TestMessage(t *testing.T) {
	tr := &testReceiver{ch: make(chan *tabletserver.MessageRow)}
	framework.Server.MessageSubscribe("vitess_message", tr)
	defer framework.Server.MessageUnsubscribe(tr)
	client := framework.NewClient()
	err := client.Begin()
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute("insert into vitess_message(id, message) values(1, 'hello world')", nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = client.Commit()
	if err != nil {
		t.Error(err)
		return
	}
	start := time.Now().UnixNano()
	mr := <-tr.ch
	got := tabletserver.MessageRow{
		ID:      mr.ID,
		Message: mr.Message,
	}
	want := tabletserver.MessageRow{
		ID:      sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
		Message: sqltypes.MakeTrusted(sqltypes.VarChar, []byte("hello world")),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("message received: %v, want %v", got, want)
	}
	qr, err := client.Execute("select time_next, epoch from vitess_message where id = 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	next, epoch := getTimeEpoch(qr)
	// epoch could be 0 or 1, depending on how fast the row is updated
	switch epoch {
	case 0:
		if !(start-1e9 < next && next < start) {
			t.Errorf("next: %d. must be within 1s of start: %d", next/1e9, start/1e9)
		}
	case 1:
		if !(start < next && next < start+3e9) {
			t.Errorf("next: %d. must be about 1s after start: %d", next/1e9, start/1e9)
		}
	default:
		t.Errorf("epoch: %d, must be 0 or 1", epoch)
	}
	<-tr.ch
	qr, err = client.Execute("select time_next, epoch from vitess_message where id = 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	next, epoch = getTimeEpoch(qr)
	// epoch could be 1 or 2, depending on how fast the row is updated
	switch epoch {
	case 1:
		if !(start < next && next < start+3e9) {
			t.Errorf("next: %d. must be about 1s after start: %d", next/1e9, start/1e9)
		}
	case 2:
		if !(start+2e9 < next && next < start+6e9) {
			t.Errorf("next: %d. must be about 3s after start: %d", next/1e9, start/1e9)
		}
	default:
		t.Errorf("epoch: %d, must be 1 or 2", epoch)
	}
	count, err := client.AckMessages("vitess_message", []string{"1"})
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("count: %d, want 1", count)
	}
	qr, err = client.Execute("select time_acked, epoch from vitess_message where id = 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	end := time.Now().UnixNano()
	ack, _ := getTimeEpoch(qr)
	if !(end-1e9 < ack && ack < end) {
		t.Errorf("ack: %d. must be within 1s of end: %d", ack/1e9, end/1e9)
	}
	// Within 3+1 seconds, the row should be deleted.
	time.Sleep(4 * time.Second)
	qr, err = client.Execute("select time_acked, epoch from vitess_message where id = 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if qr.RowsAffected != 0 {
		t.Error("The row has not been purged yet")
	}
}

func getTimeEpoch(qr *sqltypes.Result) (int64, int64) {
	if len(qr.Rows) != 1 {
		return 0, 0
	}
	t, _ := strconv.Atoi(qr.Rows[0][0].String())
	e, _ := strconv.Atoi(qr.Rows[0][1].String())
	return int64(t), int64(e)
}
