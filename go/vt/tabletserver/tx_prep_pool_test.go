// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"testing"
)

func TestPrepRegisterFail(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("panic expected")
		}
	}()
	pp := NewTxPreparedPool()
	pp.Register(nil, "aa")
	pp.Register(nil, "aa")
}

func TestPrepUnregister(t *testing.T) {
	pp := NewTxPreparedPool()
	pp.Register(nil, "aa")
	pp.Unregister("aa")
	pp.Register(nil, "aa")
}

func TestPrepGetFailPurpose(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("panic expected")
		}
	}()
	NewTxPreparedPool().Get("aa", "")
}

func TestPrepGet(t *testing.T) {
	pp := NewTxPreparedPool()
	conn := &DBConn{}
	pp.Register(conn, "aa")
	_, err := pp.Get("bb", "use")
	want := "not found"
	if err == nil || err.Error() != want {
		t.Errorf("pp.Get(bb): %v, want %s", err, want)
	}
	got, err := pp.Get("aa", "use")
	if err != nil {
		t.Error(err)
	}
	if got != conn {
		t.Errorf("pp.Get(aa): %p, want %p", got, conn)
	}
	_, err = pp.Get("aa", "use")
	want = "in use: use"
	if err == nil || err.Error() != want {
		t.Errorf("pp.Get(bb): %v, want %s", err, want)
	}
}

func TestPrepPutFail(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("panic expected")
		}
	}()
	NewTxPreparedPool().Put("aa")
}

func TestPut(t *testing.T) {
	pp := NewTxPreparedPool()
	conn := &DBConn{}
	pp.Register(conn, "aa")
	_, err := pp.Get("aa", "use")
	if err != nil {
		t.Error(err)
	}
	pp.Put("aa")
	_, err = pp.Get("aa", "use")
	if err != nil {
		t.Error(err)
	}
}
