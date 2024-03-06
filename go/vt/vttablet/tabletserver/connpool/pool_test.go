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

package connpool

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/pools/smartconnpool"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func TestConnPoolGet(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	dbConn, err := connPool.Get(context.Background(), nil)
	if err != nil {
		t.Fatalf("should not get an error, but got: %v", err)
	}
	if dbConn == nil {
		t.Fatalf("db conn should not be nil")
	}
	dbConn.Recycle()
}

func TestConnPoolTimeout(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	cfg := tabletenv.ConnPoolConfig{
		Size: 1,
	}
	cfg.Timeout = time.Second
	cfg.IdleTimeout = 10 * time.Second
	connPool := NewPool(tabletenv.NewEnv(vtenv.NewTestEnv(), nil, "PoolTest"), "TestPool", cfg)
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	dbConn, err := connPool.Get(context.Background(), nil)
	require.NoError(t, err)
	defer dbConn.Recycle()
	_, err = connPool.Get(context.Background(), nil)
	assert.EqualError(t, err, "resource pool timed out")
}

func TestConnPoolGetEmptyDebugConfig(t *testing.T) {
	db := fakesqldb.New(t)
	debugConn := dbconfigs.New(db.ConnParamsWithUname(""))
	defer db.Close()
	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, debugConn)
	im := callerid.NewImmediateCallerID("")
	ecid := callerid.NewEffectiveCallerID("p", "c", "sc")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, ecid, im)
	defer connPool.Close()
	dbConn, err := connPool.Get(ctx, nil)
	if err != nil {
		t.Fatalf("should not get an error, but got: %v", err)
	}
	if dbConn == nil {
		t.Fatalf("db conn should not be nil")
	}
	dbConn.Recycle()
}

func TestConnPoolGetAppDebug(t *testing.T) {
	db := fakesqldb.New(t)
	debugConn := dbconfigs.New(db.ConnParamsWithUname("debugUsername"))
	ctx := context.Background()
	im := callerid.NewImmediateCallerID("debugUsername")
	ecid := callerid.NewEffectiveCallerID("p", "c", "sc")
	ctx = callerid.NewContext(ctx, ecid, im)
	defer db.Close()
	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, debugConn)
	defer connPool.Close()
	dbConn, err := connPool.Get(ctx, nil)
	if err != nil {
		t.Fatalf("should not get an error, but got: %v", err)
	}
	if dbConn == nil {
		t.Fatalf("db conn should not be nil")
	}
	dbConn.Recycle()
	if !dbConn.Conn.IsClosed() {
		t.Fatalf("db conn should be closed after recycle")
	}
}

func TestConnPoolSetCapacity(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()

	assert.Panics(t, func() {
		connPool.SetCapacity(-10)
	})
	connPool.SetCapacity(10)
	if connPool.Capacity() != 10 {
		t.Fatalf("capacity should be 10")
	}
}

func TestConnPoolStatJSON(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	if connPool.StatsJSON() != "{}" {
		t.Fatalf("pool is closed, stats json should be empty; was: %q", connPool.StatsJSON())
	}
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	statsJSON := connPool.StatsJSON()
	if statsJSON == "" || statsJSON == "{}" {
		t.Fatalf("stats json should not be empty")
	}
}

func TestConnPoolStateWhilePoolIsClosed(t *testing.T) {
	connPool := newPool()
	assert.EqualValues(t, 0, connPool.Capacity(), "pool capacity should be 0 because it is still closed")
	assert.EqualValues(t, 0, connPool.Available(), "pool available connections should be 0 because it is still closed")
}

func TestConnPoolStateWhilePoolIsOpen(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	idleTimeout := 10 * time.Second
	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	assert.EqualValues(t, 100, connPool.Capacity(), "pool capacity should be 100")
	assert.EqualValues(t, 0, connPool.Metrics.WaitTime(), "pool wait time should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.WaitCount(), "pool wait count should be 0")
	assert.EqualValues(t, idleTimeout, connPool.IdleTimeout(), "pool idle timeout should be 0")
	assert.EqualValues(t, 100, connPool.Available(), "pool available connections should be 100")
	assert.EqualValues(t, 0, connPool.Active(), "pool active connections should be 0")
	assert.EqualValues(t, 0, connPool.InUse(), "pool inUse connections should be 0")

	dbConn, _ := connPool.Get(context.Background(), nil)
	assert.EqualValues(t, 99, connPool.Available(), "pool available connections should be 99")
	assert.EqualValues(t, 1, connPool.Active(), "pool active connections should be 1")
	assert.EqualValues(t, 1, connPool.InUse(), "pool inUse connections should be 1")

	dbConn.Recycle()
	assert.EqualValues(t, 100, connPool.Available(), "pool available connections should be 100")
	assert.EqualValues(t, 1, connPool.Active(), "pool active connections should be 1")
	assert.EqualValues(t, 0, connPool.InUse(), "pool inUse connections should be 0")
}

func TestConnPoolStateWithSettings(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	capacity := 5
	connPool := newPoolWithCapacity(capacity)
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	assert.EqualValues(t, 5, connPool.Available(), "pool available connections should be 5")
	assert.EqualValues(t, 0, connPool.Active(), "pool active connections should be 0")
	assert.EqualValues(t, 0, connPool.InUse(), "pool inUse connections should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.GetCount(), "pool get count should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.GetSettingCount(), "pool get with settings should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.DiffSettingCount(), "pool different settings count should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.ResetSettingCount(), "pool reset settings count should be 0")

	dbConn, err := connPool.Get(context.Background(), nil)
	require.NoError(t, err)
	assert.EqualValues(t, 4, connPool.Available(), "pool available connections should be 4")
	assert.EqualValues(t, 1, connPool.Active(), "pool active connections should be 1")
	assert.EqualValues(t, 1, connPool.InUse(), "pool inUse connections should be 1")
	assert.EqualValues(t, 1, connPool.Metrics.GetCount(), "pool get count should be 1")
	assert.EqualValues(t, 0, connPool.Metrics.GetSettingCount(), "pool get with settings should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.DiffSettingCount(), "pool different settings count should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.ResetSettingCount(), "pool reset settings count should be 0")

	dbConn.Recycle()
	assert.EqualValues(t, 5, connPool.Available(), "pool available connections should be 5")
	assert.EqualValues(t, 1, connPool.Active(), "pool active connections should be 1")
	assert.EqualValues(t, 0, connPool.InUse(), "pool inUse connections should be 0")
	assert.EqualValues(t, 1, connPool.Metrics.GetCount(), "pool get count should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.GetSettingCount(), "pool get with settings should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.DiffSettingCount(), "pool different settings count should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.ResetSettingCount(), "pool reset settings count should be 0")

	db.AddQuery("a", &sqltypes.Result{})
	sa := smartconnpool.NewSetting("a", "")
	dbConn, err = connPool.Get(context.Background(), sa)
	require.NoError(t, err)
	assert.EqualValues(t, 4, connPool.Available(), "pool available connections should be 4")
	assert.EqualValues(t, 1, connPool.Active(), "pool active connections should be 1")
	assert.EqualValues(t, 1, connPool.InUse(), "pool inUse connections should be 1")
	assert.EqualValues(t, 1, connPool.Metrics.GetCount(), "pool get count should be 1")
	assert.EqualValues(t, 1, connPool.Metrics.GetSettingCount(), "pool get with settings should be 1")
	assert.EqualValues(t, 0, connPool.Metrics.DiffSettingCount(), "pool different settings count should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.ResetSettingCount(), "pool reset settings count should be 0")

	dbConn.Recycle()
	assert.EqualValues(t, 5, connPool.Available(), "pool available connections should be 5")
	assert.EqualValues(t, 1, connPool.Active(), "pool active connections should be 1")
	assert.EqualValues(t, 0, connPool.InUse(), "pool inUse connections should be 0")
	assert.EqualValues(t, 1, connPool.Metrics.GetCount(), "pool get count should be 1")
	assert.EqualValues(t, 1, connPool.Metrics.GetSettingCount(), "pool get with settings should be 1")
	assert.EqualValues(t, 0, connPool.Metrics.DiffSettingCount(), "pool different settings count should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.ResetSettingCount(), "pool reset settings count should be 0")

	// now showcasing diff and reset setting.
	// Steps 1: acquire all connection with same setting
	// Steps 2: put all back
	// Steps 3: acquire a connection with no setting - this will show reset setting count
	// Steps 4: acquire a connection with different setting - this will show diff setting count

	// Step 1
	var conns []*PooledConn
	for i := 0; i < capacity; i++ {
		dbConn, err = connPool.Get(context.Background(), sa)
		require.NoError(t, err)
		conns = append(conns, dbConn)
	}
	assert.EqualValues(t, 0, connPool.Available(), "pool available connections should be 0")
	assert.EqualValues(t, 5, connPool.Active(), "pool active connections should be 5")
	assert.EqualValues(t, 5, connPool.InUse(), "pool inUse connections should be 5")
	assert.EqualValues(t, 1, connPool.Metrics.GetCount(), "pool get count should be 1")
	assert.EqualValues(t, 6, connPool.Metrics.GetSettingCount(), "pool get with settings should be 6")
	assert.EqualValues(t, 0, connPool.Metrics.DiffSettingCount(), "pool different settings count should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.ResetSettingCount(), "pool reset settings count should be 0")

	// Step 2
	for _, conn := range conns {
		conn.Recycle()
	}
	assert.EqualValues(t, 5, connPool.Available(), "pool available connections should be 5")
	assert.EqualValues(t, 5, connPool.Active(), "pool active connections should be 5")
	assert.EqualValues(t, 0, connPool.InUse(), "pool inUse connections should be 0")
	assert.EqualValues(t, 1, connPool.Metrics.GetCount(), "pool get count should be 1")
	assert.EqualValues(t, 6, connPool.Metrics.GetSettingCount(), "pool get with settings should be 6")
	assert.EqualValues(t, 0, connPool.Metrics.DiffSettingCount(), "pool different settings count should be 0")
	assert.EqualValues(t, 0, connPool.Metrics.ResetSettingCount(), "pool reset settings count should be 0")

	// Step 3
	dbConn, err = connPool.Get(context.Background(), nil)
	require.NoError(t, err)
	assert.EqualValues(t, 4, connPool.Available(), "pool available connections should be 4")
	assert.EqualValues(t, 5, connPool.Active(), "pool active connections should be 5")
	assert.EqualValues(t, 1, connPool.InUse(), "pool inUse connections should be 1")
	assert.EqualValues(t, 2, connPool.Metrics.GetCount(), "pool get count should be 2")
	assert.EqualValues(t, 6, connPool.Metrics.GetSettingCount(), "pool get with settings should be 6")
	assert.EqualValues(t, 0, connPool.Metrics.DiffSettingCount(), "pool different settings count should be 0")
	assert.EqualValues(t, 1, connPool.Metrics.ResetSettingCount(), "pool reset settings count should be 1")
	dbConn.Recycle()

	// Step 4
	db.AddQuery("b", &sqltypes.Result{})
	sb := smartconnpool.NewSetting("b", "")
	dbConn, err = connPool.Get(context.Background(), sb)
	require.NoError(t, err)
	assert.EqualValues(t, 4, connPool.Available(), "pool available connections should be 4")
	assert.EqualValues(t, 5, connPool.Active(), "pool active connections should be 5")
	assert.EqualValues(t, 1, connPool.InUse(), "pool inUse connections should be 1")
	assert.EqualValues(t, 2, connPool.Metrics.GetCount(), "pool get count should be 2")
	assert.EqualValues(t, 7, connPool.Metrics.GetSettingCount(), "pool get with settings should be 7")
	assert.EqualValues(t, 0, connPool.Metrics.DiffSettingCount(), "pool different settings count should be 0")
	assert.EqualValues(t, 1, connPool.Metrics.ResetSettingCount(), "pool reset settings count should be 1")
	dbConn.Recycle()
}

func TestPoolGetConnTime(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()
	connPool.getConnTime.Reset()

	getTimeMap := connPool.getConnTime.Counts()
	assert.Zero(t, getTimeMap["PoolTest.GetWithSettings"])
	assert.Zero(t, getTimeMap["PoolTest.GetWithoutSettings"])

	dbConn, err := connPool.Get(context.Background(), nil)
	require.NoError(t, err)
	defer dbConn.Recycle()

	getTimeMap = connPool.getConnTime.Counts()
	assert.EqualValues(t, 1, getTimeMap["PoolTest.GetWithoutSettings"])
	assert.Zero(t, getTimeMap["PoolTest.GetWithSettings"])

	db.AddQuery("b", &sqltypes.Result{})
	sb := smartconnpool.NewSetting("b", "")
	dbConn, err = connPool.Get(context.Background(), sb)
	require.NoError(t, err)
	defer dbConn.Recycle()

	getTimeMap = connPool.getConnTime.Counts()
	assert.EqualValues(t, 1, getTimeMap["PoolTest.GetWithSettings"])
}

func newPool() *Pool {
	return newPoolWithCapacity(100)
}

func newPoolWithCapacity(capacity int) *Pool {
	return NewPool(tabletenv.NewEnv(vtenv.NewTestEnv(), nil, "PoolTest"), "TestPool", tabletenv.ConnPoolConfig{
		Size:        capacity,
		IdleTimeout: 10 * time.Second,
	})
}
