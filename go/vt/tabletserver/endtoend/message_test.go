// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"reflect"
	"testing"

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
		t.Errorf("messag received: %v, want %v", got, want)
	}
}
