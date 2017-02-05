// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"html/template"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

type dummyChecker struct {
}

func (dummyChecker) CheckMySQL() {}

var DummyChecker = dummyChecker{}

type fakeCallInfo struct {
	remoteAddr string
	username   string
	text       string
	html       string
}

func (fci *fakeCallInfo) RemoteAddr() string {
	return fci.remoteAddr
}

func (fci *fakeCallInfo) Username() string {
	return fci.username
}

func (fci *fakeCallInfo) Text() string {
	return fci.text
}

func (fci *fakeCallInfo) HTML() template.HTML {
	return template.HTML(fci.html)
}

type testUtils struct{}

func newTestUtils() *testUtils {
	return &testUtils{}
}

func (util *testUtils) checkEqual(t *testing.T, expected interface{}, result interface{}) {
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("expect to get: %v, but got: %v", expected, result)
	}
}

func (util *testUtils) checkTabletError(t *testing.T, err interface{}, tabletErrCode vtrpcpb.ErrorCode, tabletErrStr string) {
	tabletError, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("should return a TabletError, but got err: %v", err)
	}
	if tabletError.ErrorCode != tabletErrCode {
		t.Fatalf("got a TabletError with error code %s but wanted: %s", tabletError.ErrorCode, tabletErrCode)
	}
	if !strings.Contains(tabletError.Error(), tabletErrStr) {
		t.Fatalf("expect the tablet error should contain string: '%s', but it does not. Got tablet error: '%s'", tabletErrStr, tabletError.Error())
	}
}

func (util *testUtils) newMysqld(dbcfgs *dbconfigs.DBConfigs) mysqlctl.MysqlDaemon {
	cnf := mysqlctl.NewMycnf(11111, 6802)
	// Assigning ServerID to be different from tablet UID to make sure that there are no
	// assumptions in the code that those IDs are the same.
	cnf.ServerID = 22222
	return mysqlctl.NewMysqld(
		cnf,
		dbcfgs,
		dbconfigs.AppConfig, // These tests only use the app pool.
	)
}

func (util *testUtils) newDBConfigs(db *fakesqldb.DB) dbconfigs.DBConfigs {
	return dbconfigs.DBConfigs{
		App:           *db.ConnParams(),
		SidecarDBName: "_vt",
	}
}

func (util *testUtils) newQueryServiceConfig() Config {
	randID := rand.Int63()
	config := DefaultQsConfig
	config.DebugURLPrefix = fmt.Sprintf("/debug-%d-", randID)
	config.PoolNamePrefix = fmt.Sprintf("Pool-%d-", randID)
	config.StrictMode = true
	return config
}

func (util *testUtils) newConnPool() *ConnPool {
	return NewConnPool(
		fmt.Sprintf("TestPool%d", rand.Int63()),
		100,
		10*time.Second,
		DummyChecker,
	)
}

func newTestSchemaInfo(
	queryCacheSize int,
	reloadTime time.Duration,
	idleTimeout time.Duration) *SchemaInfo {
	randID := rand.Int63()
	return NewSchemaInfo(
		DummyChecker,
		queryCacheSize,
		reloadTime,
		idleTimeout,
		map[string]string{
			debugQueryPlansKey: fmt.Sprintf("/debug/query_plans_%d", randID),
			debugQueryStatsKey: fmt.Sprintf("/debug/query_stats_%d", randID),
			debugSchemaKey:     fmt.Sprintf("/debug/schema_%d", randID),
		},
	)
}
