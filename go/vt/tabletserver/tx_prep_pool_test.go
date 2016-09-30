// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"testing"
)

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
}

func TestPrepGet(t *testing.T) {
	pp := NewTxPreparedPool(2)
	conn := &TxConnection{}
	pp.Put(conn, "aa")
	got := pp.Get("bb")
	if got != nil {
		t.Errorf("Get(bb): %v, want nil", got)
	}
	got = pp.Get("aa")
	if got != conn {
		t.Errorf("pp.Get(aa): %p, want %p", got, conn)
	}
	got = pp.Get("aa")
	if got != nil {
		t.Errorf("Get(aa): %v, want nil", got)
	}
}
