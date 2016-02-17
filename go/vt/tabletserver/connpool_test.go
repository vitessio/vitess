// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"golang.org/x/net/context"
)

func TestConnPoolGet(t *testing.T) {
	db := fakesqldb.Register()
	testUtils := newTestUtils()
	appParams := &sqldb.ConnParams{Engine: db.Name}
	dbaParams := &sqldb.ConnParams{Engine: db.Name}
	connPool := testUtils.newConnPool()
	connPool.Open(appParams, dbaParams)
	defer connPool.Close()
	dbConn, err := connPool.Get(context.Background())
	if err != nil {
		t.Fatalf("should get an error, but got: %v", err)
	}
	if dbConn == nil {
		t.Fatalf("db conn should not be nil")
	}
	dbConn.Recycle()
}

func TestConnPoolPutWhilePoolIsClosed(t *testing.T) {
	fakesqldb.Register()
	testUtils := newTestUtils()
	connPool := testUtils.newConnPool()
	defer func() {
		if recover() == nil {
			t.Fatalf("pool is closed, should get an error")
		}
	}()
	connPool.Put(nil)
}

func TestConnPoolSetCapacity(t *testing.T) {
	db := fakesqldb.Register()
	testUtils := newTestUtils()
	appParams := &sqldb.ConnParams{Engine: db.Name}
	dbaParams := &sqldb.ConnParams{Engine: db.Name}
	connPool := testUtils.newConnPool()
	connPool.Open(appParams, dbaParams)
	defer connPool.Close()
	err := connPool.SetCapacity(-10)
	if err == nil {
		t.Fatalf("set capacity should return error for negative capacity")
	}
	err = connPool.SetCapacity(10)
	if err != nil {
		t.Fatalf("set capacity should succeed")
	}
	if connPool.Capacity() != 10 {
		t.Fatalf("capacity should be 10")
	}
}

func TestConnPoolStatJSON(t *testing.T) {
	db := fakesqldb.Register()
	testUtils := newTestUtils()
	connPool := testUtils.newConnPool()
	if connPool.StatsJSON() != "{}" {
		t.Fatalf("pool is closed, stats json should be empty: {}")
	}
	appParams := &sqldb.ConnParams{Engine: db.Name}
	dbaParams := &sqldb.ConnParams{Engine: db.Name}
	connPool.Open(appParams, dbaParams)
	defer connPool.Close()
	statsJSON := connPool.StatsJSON()
	if statsJSON == "" || statsJSON == "{}" {
		t.Fatalf("stats json should not be empty")
	}
}

func TestConnPoolStateWhilePoolIsClosed(t *testing.T) {
	fakesqldb.Register()
	testUtils := newTestUtils()
	connPool := testUtils.newConnPool()
	if connPool.Capacity() != 0 {
		t.Fatalf("pool capacity should be 0 because it is still closed")
	}
	if connPool.Available() != 0 {
		t.Fatalf("pool available connections should be 0 because it is still closed")
	}
	if connPool.MaxCap() != 0 {
		t.Fatalf("pool max capacity should be 0 because it is still closed")
	}
	if connPool.WaitCount() != 0 {
		t.Fatalf("pool wait count should be 0 because it is still closed")
	}
	if connPool.WaitTime() != 0 {
		t.Fatalf("pool wait time should be 0 because it is still closed")
	}
	if connPool.IdleTimeout() != 0 {
		t.Fatalf("pool idle timeout should be 0 because it is still closed")
	}
}

func TestConnPoolStateWhilePoolIsOpen(t *testing.T) {
	db := fakesqldb.Register()
	testUtils := newTestUtils()
	appParams := &sqldb.ConnParams{Engine: db.Name}
	dbaParams := &sqldb.ConnParams{Engine: db.Name}
	idleTimeout := 10 * time.Second
	connPool := testUtils.newConnPool()
	connPool.Open(appParams, dbaParams)
	defer connPool.Close()
	if connPool.Capacity() != 100 {
		t.Fatalf("pool capacity should be 100")
	}
	if connPool.MaxCap() != 100 {
		t.Fatalf("pool max capacity should be 100")
	}
	if connPool.WaitTime() != 0 {
		t.Fatalf("pool wait time should be 0")
	}
	if connPool.WaitCount() != 0 {
		t.Fatalf("pool wait count should be 0")
	}
	if connPool.IdleTimeout() != idleTimeout {
		t.Fatalf("pool idle timeout should be 0")
	}
	if connPool.Available() != 100 {
		t.Fatalf("pool available connections should be 100")
	}
	dbConn, _ := connPool.Get(context.Background())
	if connPool.Available() != 99 {
		t.Fatalf("pool available connections should be 99")
	}
	dbConn.Recycle()
	if connPool.Available() != 100 {
		t.Fatalf("pool available connections should be 100")
	}
}
