// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var errRejected = errors.New("rejected")

type dummyChecker struct {
}

func (dummyChecker) CheckMySQL() {}

var DummyChecker = dummyChecker{}

type testUtils struct{}

func newTestUtils() *testUtils {
	return &testUtils{}
}

func (util *testUtils) checkEqual(t *testing.T, expected interface{}, result interface{}) {
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("expect to get: %v, but got: %v", expected, result)
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

func (util *testUtils) newQueryServiceConfig() tabletenv.TabletConfig {
	randID := rand.Int63()
	config := tabletenv.DefaultQsConfig
	config.PoolNamePrefix = fmt.Sprintf("Pool-%d-", randID)
	return config
}
