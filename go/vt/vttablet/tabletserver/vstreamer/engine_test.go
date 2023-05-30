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

package vstreamer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

var (
	shardedVSchema = `{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    }
  },
  "tables": {
    "t1": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "hash"
        }
      ]
    }
  }
}`

	multicolumnVSchema = `{
  "sharded": true,
  "vindexes": {
    "region_vdx": {
      "type": "region_experimental",
			"params": {
				"region_bytes": "1"
			}
    }
  },
  "tables": {
    "t1": {
      "column_vindexes": [
        {
          "columns": [
						"region",
						"id"
					],
          "name": "region_vdx"
        }
      ]
    }
  }
}`
)

func TestUpdateVSchema(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	defer env.SetVSchema("{}")

	// We have to start at least one stream to start the vschema watcher.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}
	// Stream should terminate immediately due to invalid pos.
	_ = engine.Stream(ctx, "invalid", nil, filter, throttlerapp.VStreamerName, func(_ []*binlogdatapb.VEvent) error {
		return nil
	})

	startCount := expectUpdateCount(t, 1)

	if err := env.SetVSchema(shardedVSchema); err != nil {
		t.Fatal(err)
	}
	expectUpdateCount(t, startCount+1)

	want := `{
  "routing_rules": {},
  "keyspaces": {
    "vttest": {
      "sharded": true,
      "tables": {
        "t1": {
          "name": "t1",
          "column_vindexes": [
            {
              "columns": [
                "id1"
              ],
              "type": "hash",
              "name": "hash",
              "vindex": {}
            }
          ],
          "ordered": [
            {
              "columns": [
                "id1"
              ],
              "type": "hash",
              "name": "hash",
              "vindex": {}
            }
          ]
        }
      },
      "vindexes": {
        "hash": {}
      }
    }
  },
  "shard_routing_rules": null
}`
	b, err := json.MarshalIndent(engine.vschema(), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	require.Equal(t, want, got)
}

func expectUpdateCount(t *testing.T, wantCount int64) int64 {
	for i := 0; i < 10; i++ {
		gotCount := engine.vschemaUpdates.Get()
		if gotCount >= wantCount {
			return gotCount
		}
		if i == 9 {
			t.Fatalf("update count: %d, want %d", gotCount, wantCount)
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("unreachable")
}

func TestVStreamerWaitForMySQL(t *testing.T) {
	tableName := "test"
	type fields struct {
		vse                   *Engine
		cp                    dbconfigs.Connector
		se                    *schema.Engine
		ReplicationLagSeconds int64
		maxInnoDBTrxHistLen   int64
		maxMySQLReplLagSecs   int64
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Small InnoDB MVCC impact limit",
			fields: fields{
				vse:                 engine,
				se:                  engine.se,
				maxInnoDBTrxHistLen: 100,
				maxMySQLReplLagSecs: 5000,
			},
			wantErr: true,
		},
		{
			name: "Small Repl Lag impact limit",
			fields: fields{
				vse:                 engine,
				se:                  engine.se,
				maxInnoDBTrxHistLen: 10000,
				maxMySQLReplLagSecs: 5,
			},
			wantErr: true,
		},
		{
			name: "Large impact limits",
			fields: fields{
				vse:                 engine,
				se:                  engine.se,
				maxInnoDBTrxHistLen: 10000,
				maxMySQLReplLagSecs: 200,
			},
			wantErr: false,
		},
	}
	testDB := fakesqldb.New(t)
	hostres := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"hostname|port",
		"varchar|int64"),
		"localhost|3306",
	)
	thlres := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"history_len",
		"int64"),
		"1000",
	)
	sbmres := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Seconds_Behind_Master",
		"int64"),
		"10",
	)
	testDB.AddQuery(hostQuery, hostres)
	testDB.AddQuery(trxHistoryLenQuery, thlres)
	testDB.AddQuery(replicaLagQuery, sbmres)
	for _, tt := range tests {
		tt.fields.cp = testDB.ConnParams()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		t.Run(tt.name, func(t *testing.T) {
			uvs := &uvstreamer{
				ctx:                   ctx,
				cancel:                cancel,
				vse:                   tt.fields.vse,
				cp:                    tt.fields.cp,
				se:                    tt.fields.se,
				ReplicationLagSeconds: tt.fields.ReplicationLagSeconds,
			}
			env.TabletEnv.Config().RowStreamer.MaxInnoDBTrxHistLen = tt.fields.maxInnoDBTrxHistLen
			env.TabletEnv.Config().RowStreamer.MaxMySQLReplLagSecs = tt.fields.maxMySQLReplLagSecs
			if err := uvs.vse.waitForMySQL(ctx, uvs.cp, tableName); (err != nil) != tt.wantErr {
				t.Errorf("vstreamer.waitForMySQL() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	require.Equal(t, engine.rowStreamerWaits.Counts()["VStreamerTest.waitForMySQL"], int64(2))
	require.Equal(t, engine.vstreamerPhaseTimings.Counts()["VStreamerTest."+tableName+":waitForMySQL"], int64(2))
}
