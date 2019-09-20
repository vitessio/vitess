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

package endtoend

import (
	"io"
	"reflect"
	"runtime"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var createMessage = `create table vitess_message(
	time_scheduled bigint,
	id bigint,
	time_next bigint,
	epoch bigint,
	time_created bigint,
	time_acked bigint,
	message varchar(128),
	primary key(time_scheduled, id),
	unique index id_idx(id),
	index next_idx(time_next, epoch))
comment 'vitess_message,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=2,vt_cache_size=10,vt_poller_interval=1'`

func TestMessage(t *testing.T) {
	ch := make(chan *sqltypes.Result)
	done := make(chan struct{})
	client := framework.NewClient()
	if _, err := client.Execute(createMessage, nil); err != nil {
		t.Fatal(err)
	}
	defer client.Execute("drop table vitess_message", nil)

	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_message.Acked"), 0; got != want {
		t.Errorf("Messages/vitess_message.Acked: %d, want %d", got, want)
	}
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_message.Queued"), 0; got != want {
		t.Errorf("Messages/vitess_message.Queued: %d, want %d", got, want)
	}

	// Start goroutine to consume message stream.
	go waitForMessage(t, client, "vitess_message", ch, done)
	got := <-ch
	want := &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "id",
			Type: sqltypes.Int64,
		}, {
			Name: "time_scheduled",
			Type: sqltypes.Int64,
		}, {
			Name: "message",
			Type: sqltypes.VarChar,
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("message(field) received:\n%v, want\n%v", got, want)
	}
	runtime.Gosched()
	defer func() { close(done) }()

	// Create message.
	err := client.Begin(false)
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
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_message.Queued"), 1; got != want {
		t.Errorf("Messages/vitess_message.Queued: %d, want %d", got, want)
	}

	// Consume first message.
	start := time.Now().UnixNano()
	got = <-ch
	// Check time_scheduled separately.
	scheduled, err := sqltypes.ToInt64(got.Rows[0][1])
	if err != nil {
		t.Error(err)
	}
	if now := time.Now().UnixNano(); now-scheduled >= int64(10*time.Second) {
		t.Errorf("scheduled: %v, must be close to %v", scheduled, now)
	}
	want = &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			got.Rows[0][1],
			sqltypes.NewVarChar("hello world"),
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("message received:\n%v, want\n%v", got, want)
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
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_message.Delayed"), 0; got != want {
		t.Errorf("Messages/vitess_message.Delayed: %d, want %d", got, want)
	}

	// Consume the resend.
	<-ch
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
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_message.Delayed"), 1; got != want {
		t.Errorf("Messages/vitess_message.Delayed: %d, want %d", got, want)
	}

	// Ack the message.
	count, err := client.MessageAck("vitess_message", []string{"1"})
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
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_message.Acked"), 1; got != want {
		t.Errorf("Messages/vitess_message.Acked: %d, want %d", got, want)
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
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_message.Purged"), 1; got != want {
		t.Errorf("Messages/vitess_message.Purged: %d, want %d", got, want)
	}

	// Verify final counts.
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_message.Queued"), 1; got != want {
		t.Errorf("Messages/vitess_message.Queued: %d, want %d", got, want)
	}
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_message.Acked"), 1; got != want {
		t.Errorf("Messages/vitess_message.Acked: %d, want %d", got, want)
	}
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_message.Delayed"), 1; got != want {
		t.Errorf("Messages/vitess_message.Delayed: %d, want %d", got, want)
	}
}

var createThreeColMessage = `create table vitess_message3(
	time_scheduled bigint,
	id bigint,
	time_next bigint,
	epoch bigint,
	time_created bigint,
	time_acked bigint,
	msg1 varchar(128),
	msg2 bigint,
	primary key(time_scheduled, id),
	unique index id_idx(id),
	index next_idx(time_next, epoch))
comment 'vitess_message,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=2,vt_cache_size=10,vt_poller_interval=1'`

func TestThreeColMessage(t *testing.T) {
	ch := make(chan *sqltypes.Result)
	done := make(chan struct{})
	client := framework.NewClient()
	if _, err := client.Execute(createThreeColMessage, nil); err != nil {
		t.Fatal(err)
	}
	defer client.Execute("drop table vitess_message3", nil)

	go waitForMessage(t, client, "vitess_message3", ch, done)

	// Verify fields.
	got := <-ch
	want := &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "id",
			Type: sqltypes.Int64,
		}, {
			Name: "time_scheduled",
			Type: sqltypes.Int64,
		}, {
			Name: "msg1",
			Type: sqltypes.VarChar,
		}, {
			Name: "msg2",
			Type: sqltypes.Int64,
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("message(field) received:\n%v, want\n%v", got, want)
	}
	runtime.Gosched()
	defer func() { close(done) }()
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute("insert into vitess_message3(id, msg1, msg2) values(1, 'hello world', 3)", nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = client.Commit()
	if err != nil {
		t.Error(err)
		return
	}

	// Verify row.
	got = <-ch
	want = &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			got.Rows[0][1],
			sqltypes.NewVarChar("hello world"),
			sqltypes.NewInt64(3),
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("message received:\n%v, want\n%v", got, want)
	}

	// Verify Ack.
	count, err := client.MessageAck("vitess_message3", []string{"1"})
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("count: %d, want 1", count)
	}
}

func getTimeEpoch(qr *sqltypes.Result) (int64, int64) {
	if len(qr.Rows) != 1 {
		return 0, 0
	}
	t, _ := sqltypes.ToInt64(qr.Rows[0][0])
	e, _ := sqltypes.ToInt64(qr.Rows[0][1])
	return t, e
}

var createMessageAuto = `create table vitess_message_auto(
	time_scheduled bigint,
	id bigint auto_increment,
	time_next bigint,
	epoch bigint,
	time_created bigint,
	time_acked bigint,
	message varchar(128),
	primary key(time_scheduled, id),
	unique index id_idx(id),
	index next_idx(time_next, epoch))
comment 'vitess_message,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=1'`

// TestMessageAuto tests for the case where id is an auto-inc column.
func TestMessageAuto(t *testing.T) {
	ch := make(chan *sqltypes.Result)
	done := make(chan struct{})
	client := framework.NewClient()
	if _, err := client.Execute(createMessageAuto, nil); err != nil {
		t.Fatal(err)
	}
	defer client.Execute("drop table vitess_message_auto", nil)

	// Start goroutine to consume message stream.
	go waitForMessage(t, client, "vitess_message_auto", ch, done)
	<-ch
	defer func() { close(done) }()

	// Create message.
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	// This insert should cause the engine to make a best-effort guess at generated ids.
	// It will expedite the first two rows with null values, and the third row, and will
	// give up on the last row, which should eventually be picked up by the poller.
	_, err = client.Execute("insert into vitess_message_auto(id, message) values(null, 'msg1'), (null, 'msg2'), (5, 'msg5'), (null, 'msg6')", nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = client.Commit()
	if err != nil {
		t.Error(err)
		return
	}

	// Only three messages should be queued.
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_message_auto.Queued"), 3; got != want {
		t.Errorf("Messages/vitess_message_auto.Queued: %d, want %d", got, want)
	}

	wantResults := []*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NULL,
			sqltypes.NewVarChar("msg1"),
		}},
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(2),
			sqltypes.NULL,
			sqltypes.NewVarChar("msg2"),
		}},
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(5),
			sqltypes.NULL,
			sqltypes.NewVarChar("msg5"),
		}},
	}}

	// Consume first three messages
	// and ensure they were received promptly.
	start := time.Now()
	for i := 0; i < 3; i++ {
		got := <-ch
		got.Rows[0][1] = sqltypes.NULL

		// Results can come in any order.
		found := false
		for _, want := range wantResults {
			if reflect.DeepEqual(got, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("message fetch: %v not found in expected list: %v", got, wantResults)
		}
	}
	if d := time.Since(start); d > 1*time.Second {
		t.Errorf("First three messages were delayed: %v", d)
	}

	_, err = client.MessageAck("vitess_message_auto", []string{"1, 2, 5"})
	if err != nil {
		t.Error(err)
	}

	// Ensure msg6 is eventually received.
	got := <-ch
	got.Rows[0][1] = sqltypes.NULL
	want := &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(6),
			sqltypes.NULL,
			sqltypes.NewVarChar("msg6"),
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("message received:\n%v, want\n%v", got, want)
	}
}

var createMessageTopic1 = `create table vitess_topic_subscriber_1(
	time_scheduled bigint,
	id bigint,
	time_next bigint,
	epoch bigint,
	time_created bigint,
	time_acked bigint,
	message varchar(128),
	primary key(time_scheduled, id),
	unique index id_idx(id),
	index next_idx(time_next, epoch))
comment 'vitess_message,vt_topic=test_topic,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=1'`

var createMessageTopic2 = `create table vitess_topic_subscriber_2(
	time_scheduled bigint,
	id bigint,
	time_next bigint,
	epoch bigint,
	time_created bigint,
	time_acked bigint,
	message varchar(128),
	primary key(time_scheduled, id),
	unique index id_idx(id),
	index next_idx(time_next, epoch))
comment 'vitess_message,vt_topic=test_topic,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=1'`

// TestMessageTopic tests for the case where id is an auto-inc column.
func TestMessageTopic(t *testing.T) {
	ch1 := make(chan *sqltypes.Result)
	ch2 := make(chan *sqltypes.Result)
	done := make(chan struct{})
	client := framework.NewClient()

	//
	// phase 1 tests inserts into a topic going to two subscribed message tables
	//
	if _, err := client.Execute(createMessageTopic1, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Execute(createMessageTopic2, nil); err != nil {
		t.Fatal(err)
	}

	// Start goroutines to consume message stream.
	go waitForMessage(t, client, "vitess_topic_subscriber_1", ch1, done)
	<-ch1
	go waitForMessage(t, client, "vitess_topic_subscriber_2", ch2, done)
	<-ch2
	defer func() { close(done) }()

	// Create message.
	err := client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	// This insert should cause the engine to make a best-effort guess at generated ids.
	// It will expedite the first two rows with null values, and the third row, and will
	// give up on the last row, which should eventually be picked up by the poller.
	_, err = client.Execute("insert into test_topic(id, message) values(1, 'msg1'), (2, 'msg2'), (3, 'msg3')", nil)
	if err != nil {
		t.Error(err)
		return
	}

	err = client.Commit()
	if err != nil {
		t.Error(err)
		return
	}

	// Only three messages should be queued on the first subscription table
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_topic_subscriber_1.Queued"), 3; got != want {
		t.Errorf("Messages/vitess_topic_subscriber_1.Queued: %d, want %d", got, want)
	}

	// Only three messages should be queued on the second subscription table
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_topic_subscriber_2.Queued"), 3; got != want {
		t.Errorf("Messages/vitess_topic_subscriber_2.Queued: %d, want %d", got, want)
	}

	wantResults := []*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NULL,
			sqltypes.NewVarChar("msg1"),
		}},
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(2),
			sqltypes.NULL,
			sqltypes.NewVarChar("msg2"),
		}},
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(3),
			sqltypes.NULL,
			sqltypes.NewVarChar("msg3"),
		}},
	}}

	// Consume first three messages
	// and ensure they were received promptly.
	start := time.Now()
	for i := 0; i < 3; i++ {
		// make sure the first message table received all three messages
		got1 := <-ch1
		got1.Rows[0][1] = sqltypes.NULL

		// Results can come in any order.
		found := false
		for _, want := range wantResults {
			if reflect.DeepEqual(got1, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("message fetch 1: %v not found in expected list: %v", got1, wantResults)
		}

		// make sure the second message table received all three messages
		got2 := <-ch2
		got2.Rows[0][1] = sqltypes.NULL

		// Results can come in any order.
		found = false
		for _, want := range wantResults {
			if reflect.DeepEqual(got2, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("message fetch 2: %v not found in expected list: %v", got2, wantResults)
		}
	}
	if d := time.Since(start); d > 1*time.Second {
		t.Errorf("messages were delayed: %v", d)
	}

	// ack the first subscriber
	_, err = client.MessageAck("vitess_topic_subscriber_1", []string{"1, 2, 3"})
	if err != nil {
		t.Error(err)
	}

	// ack the second subscriber
	_, err = client.MessageAck("vitess_topic_subscriber_2", []string{"1, 2, 3"})
	if err != nil {
		t.Error(err)
	}

	//
	// phase 2 tests deleting one of the subscribers and making sure
	// that inserts into a topic go to one subscribed message table
	//

	client.Execute("drop table vitess_topic_subscriber_1", nil)

	// Create message.
	err = client.Begin(false)
	if err != nil {
		t.Error(err)
		return
	}
	// This insert should cause the engine to make a best-effort guess at generated ids.
	// It will expedite the first two rows with null values, and the third row, and will
	// give up on the last row, which should eventually be picked up by the poller.
	_, err = client.Execute("insert into test_topic(id, message) values(4, 'msg4'), (5, 'msg5'), (6, 'msg6')", nil)
	if err != nil {
		t.Error(err)
		return
	}

	err = client.Commit()
	if err != nil {
		t.Error(err)
		return
	}

	// no messages should be queued on the first subscription table
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_topic_subscriber_1.Queued"), 3; got != want {
		t.Errorf("Messages/vitess_topic_subscriber_1.Queued: %d, want %d", got, want)
	}

	// Only three messages should be queued on the second subscription table
	if got, want := framework.FetchInt(framework.DebugVars(), "Messages/vitess_topic_subscriber_2.Queued"), 6; got != want {
		t.Errorf("Messages/vitess_topic_subscriber_2.Queued: %d, want %d", got, want)
	}

	wantResults = []*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(4),
			sqltypes.NULL,
			sqltypes.NewVarChar("msg4"),
		}},
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(5),
			sqltypes.NULL,
			sqltypes.NewVarChar("msg5"),
		}},
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(6),
			sqltypes.NULL,
			sqltypes.NewVarChar("msg6"),
		}},
	}}

	// Consume first three messages
	// and ensure they were received promptly.
	start = time.Now()
	for i := 0; i < 3; i++ {
		// make sure the second message table received all three messages
		got2 := <-ch2
		got2.Rows[0][1] = sqltypes.NULL

		// Results can come in any order.
		found := false
		for _, want := range wantResults {
			if reflect.DeepEqual(got2, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("message fetch 2: %v not found in expected list: %v", got2, wantResults)
		}
	}
	if d := time.Since(start); d > 1*time.Second {
		t.Errorf("messages were delayed: %v", d)
	}

	// ack the second subscriber
	_, err = client.MessageAck("vitess_topic_subscriber_2", []string{"4, 5, 6"})
	if err != nil {
		t.Error(err)
	}

	//
	// phase 3 tests deleting the last subscriber and making sure
	// that inserts into a topic error out with table not found
	//

	// remove the second subscriber which should remove the topic
	if _, err := client.Execute("drop table vitess_topic_subscriber_2", nil); err != nil {
		t.Fatal(err)
	}

	// this should fail because the topic doesn't exist. Any other outcome fails the test
	_, err = client.Execute("insert into test_topic(id, message) values(4, 'msg4'), (5, 'msg5'), (6, 'msg6')", nil)
	switch {
	case err == nil:
		t.Error("test_topic shouldn't have existed for inserts to succeed")

	case err.Error() == "table test_topic not found in schema (CallerID: dev)":

	default:
		t.Error(err)
	}
}

func waitForMessage(t *testing.T, client *framework.QueryClient, tableName string, ch chan *sqltypes.Result, done chan struct{}) {
	if err := client.MessageStream(tableName, func(qr *sqltypes.Result) error {
		select {
		case <-done:
			return io.EOF
		default:
		}
		ch <- qr
		return nil
	}); err != nil {
		t.Error(err)
	}
	close(ch)
}
