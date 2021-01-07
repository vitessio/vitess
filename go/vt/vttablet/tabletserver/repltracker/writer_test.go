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

package repltracker

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var (
	now         = time.Now()
	mockNowFunc = func() time.Time {
		return now
	}
)

func TestCreateSchema(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	tw := newTestWriter(db, mockNowFunc)
	defer tw.Close()
	writes.Reset()

	db.OrderMatters()
	upsert := fmt.Sprintf("INSERT INTO %s.heartbeat (ts, tabletUid, keyspaceShard) VALUES (%d, %d, '%s') ON DUPLICATE KEY UPDATE ts=VALUES(ts), tabletUid=VALUES(tabletUid)",
		"_vt", now.UnixNano(), tw.tabletAlias.Uid, tw.keyspaceShard)
	failInsert := fakesqldb.ExpectedExecuteFetch{
		Query: upsert,
		Error: mysql.NewSQLError(mysql.ERBadDb, "", "bad db error"),
	}
	db.AddExpectedExecuteFetch(failInsert)
	db.AddExpectedQuery(fmt.Sprintf(sqlCreateSidecarDB, "_vt"), nil)
	db.AddExpectedQuery(fmt.Sprintf(sqlCreateHeartbeatTable, "_vt"), nil)
	db.AddExpectedQuery(upsert, nil)

	err := tw.write()
	require.NoError(t, err)
}

func TestWriteHeartbeat(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	tw := newTestWriter(db, mockNowFunc)
	upsert := fmt.Sprintf("INSERT INTO %s.heartbeat (ts, tabletUid, keyspaceShard) VALUES (%d, %d, '%s') ON DUPLICATE KEY UPDATE ts=VALUES(ts), tabletUid=VALUES(tabletUid)",
		"_vt", now.UnixNano(), tw.tabletAlias.Uid, tw.keyspaceShard)
	db.AddQuery(upsert, &sqltypes.Result{})

	writes.Reset()
	writeErrors.Reset()

	tw.writeHeartbeat()
	assert.Equal(t, int64(1), writes.Get())
	assert.Equal(t, int64(0), writeErrors.Get())
}

func TestWriteHeartbeatError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	tw := newTestWriter(db, mockNowFunc)

	writes.Reset()
	writeErrors.Reset()

	tw.writeHeartbeat()
	assert.Equal(t, int64(0), writes.Get())
	assert.Equal(t, int64(1), writeErrors.Get())
}

func newTestWriter(db *fakesqldb.DB, nowFunc func() time.Time) *heartbeatWriter {
	config := tabletenv.NewDefaultConfig()
	config.ReplicationTracker.Mode = tabletenv.Heartbeat
	config.ReplicationTracker.HeartbeatIntervalSeconds = 1

	params, _ := db.ConnParams().MysqlParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "")

	tw := newHeartbeatWriter(tabletenv.NewEnv(config, "WriterTest"), topodatapb.TabletAlias{Cell: "test", Uid: 1111})
	tw.keyspaceShard = "test:0"
	tw.now = nowFunc
	tw.pool.Open(dbc.AppWithDB(), dbc.DbaWithDB(), dbc.AppDebugWithDB())

	return tw
}
