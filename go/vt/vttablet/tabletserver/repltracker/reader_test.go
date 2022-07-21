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

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TestReaderReadHeartbeat tests that reading a heartbeat sets the appropriate
// fields on the object.
func TestReaderReadHeartbeat(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	tr := newReader(db, mockNowFunc)
	defer tr.Close()

	db.AddQuery(fmt.Sprintf("SELECT ts FROM %s.heartbeat WHERE keyspaceShard='%s'", "_vt", tr.keyspaceShard), &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "ts", Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(now.Add(-10 * time.Second).UnixNano()),
		}},
	})

	cumulativeLagNs.Reset()
	readErrors.Reset()
	reads.Reset()

	tr.readHeartbeat()
	lag, err := tr.Status()

	require.NoError(t, err)
	expectedLag := 10 * time.Second
	assert.Equal(t, expectedLag, lag, "wrong latest lag")
	expectedCumLag := 10 * time.Second.Nanoseconds()
	assert.Equal(t, expectedCumLag, cumulativeLagNs.Get(), "wrong cumulative lag")
	assert.Equal(t, int64(1), reads.Get(), "wrong read count")
	assert.Equal(t, int64(0), readErrors.Get(), "wrong read error count")
	expectedHisto := map[string]int64{
		"0":      int64(0),
		"1ms":    int64(0),
		"10ms":   int64(0),
		"100ms":  int64(0),
		"1s":     int64(0),
		"10s":    int64(1),
		"100s":   int64(0),
		"1000s":  int64(0),
		">1000s": int64(0),
	}
	utils.MustMatch(t, expectedHisto, heartbeatLagNsHistogram.Counts(), "wrong counts in histogram")
}

// TestReaderReadHeartbeatError tests that we properly account for errors
// encountered in the reading of heartbeat.
func TestReaderReadHeartbeatError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	tr := newReader(db, mockNowFunc)
	defer tr.Close()

	cumulativeLagNs.Reset()
	readErrors.Reset()

	tr.readHeartbeat()
	lag, err := tr.Status()

	require.Error(t, err)
	assert.EqualError(t, err, tr.lastKnownError.Error(), "expected error")
	assert.Equal(t, 0*time.Second, lag, "wrong lastKnownLag")
	assert.Equal(t, int64(0), cumulativeLagNs.Get(), "wrong cumulative lag")
	assert.Equal(t, int64(1), readErrors.Get(), "wrong read error count")
}

func newReader(db *fakesqldb.DB, nowFunc func() time.Time) *heartbeatReader {
	config := tabletenv.NewDefaultConfig()
	config.ReplicationTracker.Mode = tabletenv.Heartbeat
	config.ReplicationTracker.HeartbeatIntervalSeconds = 1
	params, _ := db.ConnParams().MysqlParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "")

	tr := newHeartbeatReader(tabletenv.NewEnv(config, "ReaderTest"))
	tr.keyspaceShard = "test:0"
	tr.now = nowFunc
	tr.pool.Open(dbc.AppWithDB(), dbc.DbaWithDB(), dbc.AppDebugWithDB())

	return tr
}
