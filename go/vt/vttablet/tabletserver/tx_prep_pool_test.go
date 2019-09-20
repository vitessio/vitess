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

package tabletserver

import (
	"testing"
)

func TestEmptyPrep(t *testing.T) {
	pp := NewTxPreparedPool(0)
	want := "prepared transactions exceeded limit: 0"
	err := pp.Put(nil, "aa")
	if err == nil || err.Error() != want {
		t.Errorf("Put err: %v, want %s", err, want)
	}
}

func TestPrepPut(t *testing.T) {
	pp := NewTxPreparedPool(2)
	err := pp.Put(nil, "aa")
	if err != nil {
		t.Error(err)
	}
	err = pp.Put(nil, "bb")
	if err != nil {
		t.Error(err)
	}
	want := "prepared transactions exceeded limit: 2"
	err = pp.Put(nil, "cc")
	if err == nil || err.Error() != want {
		t.Errorf("Put err: %v, want %s", err, want)
	}
	err = pp.Put(nil, "aa")
	want = "duplicate DTID in Prepare: aa"
	if err == nil || err.Error() != want {
		t.Errorf("Put err: %v, want %s", err, want)
	}
	_, err = pp.FetchForCommit("aa")
	if err != nil {
		t.Error(err)
	}
	err = pp.Put(nil, "aa")
	want = "duplicate DTID in Prepare: aa"
	if err == nil || err.Error() != want {
		t.Errorf("Put err: %v, want %s", err, want)
	}
	pp.Forget("aa")
	err = pp.Put(nil, "aa")
	if err != nil {
		t.Error(err)
	}
}

func TestPrepFetchForRollback(t *testing.T) {
	pp := NewTxPreparedPool(2)
	conn := &TxConnection{}
	pp.Put(conn, "aa")
	got := pp.FetchForRollback("bb")
	if got != nil {
		t.Errorf("Get(bb): %v, want nil", got)
	}
	got = pp.FetchForRollback("aa")
	if got != conn {
		t.Errorf("pp.Get(aa): %p, want %p", got, conn)
	}
	got = pp.FetchForRollback("aa")
	if got != nil {
		t.Errorf("Get(aa): %v, want nil", got)
	}
}

func TestPrepFetchForCommit(t *testing.T) {
	pp := NewTxPreparedPool(2)
	conn := &TxConnection{}
	got, err := pp.FetchForCommit("aa")
	if err != nil {
		t.Error(err)
	}
	if got != nil {
		t.Errorf("Get(aa): %v, want nil", got)
	}
	pp.Put(conn, "aa")
	got, err = pp.FetchForCommit("aa")
	if err != nil {
		t.Error(err)
	}
	if got != conn {
		t.Errorf("pp.Get(aa): %p, want %p", got, conn)
	}
	_, err = pp.FetchForCommit("aa")
	want := "committing"
	if err == nil || err.Error() != want {
		t.Errorf("FetchForCommit err: %v, want %s", err, want)
	}
	pp.SetFailed("aa")
	_, err = pp.FetchForCommit("aa")
	want = "failed"
	if err == nil || err.Error() != want {
		t.Errorf("FetchForCommit err: %v, want %s", err, want)
	}
	pp.SetFailed("bb")
	_, err = pp.FetchForCommit("bb")
	want = "failed"
	if err == nil || err.Error() != want {
		t.Errorf("FetchForCommit err: %v, want %s", err, want)
	}
	pp.Forget("aa")
	got, err = pp.FetchForCommit("aa")
	if err != nil {
		t.Error(err)
	}
	if got != nil {
		t.Errorf("Get(aa): %v, want nil", got)
	}
}

func TestPrepFetchAll(t *testing.T) {
	pp := NewTxPreparedPool(2)
	conn1 := &TxConnection{}
	conn2 := &TxConnection{}
	pp.Put(conn1, "aa")
	pp.Put(conn2, "bb")
	got := pp.FetchAll()
	if len(got) != 2 {
		t.Errorf("FetchAll len: %d, want 2", len(got))
	}
	if len(pp.conns) != 0 {
		t.Errorf("len(pp.conns): %d, want 0", len(pp.conns))
	}
}
