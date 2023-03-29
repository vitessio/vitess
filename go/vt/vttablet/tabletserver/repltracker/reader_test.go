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

	now := time.Now()
	tr := newReader(db, &now)
	defer tr.Close()

	tr.pool.Open(tr.env.Config().DB.AppWithDB(), tr.env.Config().DB.DbaWithDB(), tr.env.Config().DB.AppDebugWithDB())

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

// TestReaderCloseSetsCurrentLagToZero tests that when closing the heartbeat reader, the current lag is
// set to zero.
func TestReaderCloseSetsCurrentLagToZero(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	tr := newReader(db, nil)

	db.AddQuery(fmt.Sprintf("SELECT ts FROM %s.heartbeat WHERE keyspaceShard='%s'", "_vt", tr.keyspaceShard), &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "ts", Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(time.Now().Add(-10 * time.Second).UnixNano()),
		}},
	})

	currentLagNs.Reset()

	tr.Open()
	time.Sleep(2 * time.Second)

	assert.Greater(t, currentLagNs.Get(), int64(0), "lag should be greater than zero")

	tr.Close()

	assert.Equal(t, int64(0), currentLagNs.Get(), "lag should be be zero after closing the reader.")
}

// TestReaderReadHeartbeatError tests that we properly account for errors
// encountered in the reading of heartbeat.
func TestReaderReadHeartbeatError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	now := time.Now()
	tr := newReader(db, &now)
	defer tr.Close()

	tr.pool.Open(tr.env.Config().DB.AppWithDB(), tr.env.Config().DB.DbaWithDB(), tr.env.Config().DB.AppDebugWithDB())

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

func newReader(db *fakesqldb.DB, frozenTime *time.Time) *heartbeatReader {
	config := tabletenv.NewDefaultConfig()
	config.ReplicationTracker.Mode = tabletenv.Heartbeat
	config.ReplicationTracker.HeartbeatIntervalSeconds = 1
	params, _ := db.ConnParams().MysqlParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "")
	config.DB = dbc

	tr := newHeartbeatReader(tabletenv.NewEnv(config, "ReaderTest"))
	tr.keyspaceShard = "test:0"

	if frozenTime != nil {
		tr.now = func() time.Time {
			return *frozenTime
		}
	}

	return tr
}
