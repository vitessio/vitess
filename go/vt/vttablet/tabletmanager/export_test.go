/*
Copyright 2026 The Vitess Authors.

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

package tabletmanager

import (
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	"golang.org/x/sync/semaphore"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// NewUnauthProbeTabletManager builds a TabletManager whose DBA, all-privs,
// app, and replication calls reach the fake MySQL daemon. The grants gate is
// already open and the action lock is usable, so an RPC that is dispatched
// runs the same statements a production tablet would.
func NewUnauthProbeTabletManager(db *fakesqldb.DB) *TabletManager {
	cp := mysql.ConnParams{}
	tm := &TabletManager{
		MysqlDaemon:            mysqlctl.NewFakeMysqlDaemon(db),
		DBConfigs:              dbconfigs.NewTestDBConfigs(cp, cp, "testdb"),
		QueryServiceControl:    tabletservermock.NewController(),
		Env:                    vtenv.NewTestEnv(),
		actionSema:             semaphore.NewWeighted(1),
		_waitForGrantsComplete: make(chan struct{}),
		tabletAlias:            &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
	}
	close(tm._waitForGrantsComplete)
	return tm
}
