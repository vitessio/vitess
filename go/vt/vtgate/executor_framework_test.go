/*
Copyright 2017 Google Inc.

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

package vtgate

import (
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/vttablet/sandboxconn"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var executorVSchema = `
{
	"sharded": true,
	"vindexes": {
		"user_index": {
			"type": "hash"
		},
		"music_user_map": {
			"type": "lookup_hash_unique",
			"owner": "music",
			"params": {
				"table": "music_user_map",
				"from": "music_id",
				"to": "user_id"
			}
		},
		"name_user_map": {
			"type": "lookup_hash",
			"owner": "user",
			"params": {
				"table": "name_user_map",
				"from": "name",
				"to": "user_id"
			}
		},
		"insert_ignore_idx": {
			"type": "lookup_hash",
			"owner": "insert_ignore_test",
			"params": {
				"table": "ins_lookup",
				"from": "fromcol",
				"to": "tocol"
			}
		},
		"idx1": {
			"type": "hash"
		},
		"idx_noauto": {
			"type": "hash",
			"owner": "noauto_table"
		},
		"keyspace_id": {
			"type": "numeric"
		}
	},
	"tables": {
		"user": {
			"column_vindexes": [
				{
					"column": "Id",
					"name": "user_index"
				},
				{
					"column": "name",
					"name": "name_user_map"
				}
			],
			"auto_increment": {
				"column": "id",
				"sequence": "user_seq"
			}
		},
		"user_extra": {
			"column_vindexes": [
				{
					"column": "user_id",
					"name": "user_index"
				}
			]
		},
		"music": {
			"column_vindexes": [
				{
					"column": "user_id",
					"name": "user_index"
				},
				{
					"column": "id",
					"name": "music_user_map"
				}
			],
			"auto_increment": {
				"column": "id",
				"sequence": "user_seq"
			}
		},
		"music_extra": {
			"column_vindexes": [
				{
					"column": "user_id",
					"name": "user_index"
				},
				{
					"column": "music_id",
					"name": "music_user_map"
				}
			]
		},
		"music_extra_reversed": {
			"column_vindexes": [
				{
					"column": "music_id",
					"name": "music_user_map"
				},
				{
					"column": "user_id",
					"name": "user_index"
				}
			]
		},
		"insert_ignore_test": {
			"column_vindexes": [
				{
					"column": "pv",
					"name": "music_user_map"
				},
				{
					"column": "owned",
					"name": "insert_ignore_idx"
				},
				{
					"column": "verify",
					"name": "user_index"
				}
			]
		},
		"noauto_table": {
			"column_vindexes": [
				{
					"column": "id",
					"name": "idx_noauto"
				}
			]
		},
		"ksid_table": {
			"column_vindexes": [
				{
					"column": "keyspace_id",
					"name": "keyspace_id"
				}
			]
		}
	}
}
`
var badVSchema = `
{
	"sharded": false,
	"tables": {
		"sharded_table": {}
	}
}
`

var unshardedVSchema = `
{
	"sharded": false,
	"tables": {
		"user_seq": {
			"type": "sequence"
		},
		"music_user_map": {},
		"name_user_map": {},
		"ins_lookup": {},
		"main1": {
			"auto_increment": {
				"column": "id",
				"sequence": "user_seq"
			}
		},
		"simple": {}
	}
}
`

const testBufferSize = 10

func createExecutorEnv() (executor *Executor, sbc1, sbc2, sbclookup *sandboxconn.SandboxConn) {
	cell := "aa"
	hc := discovery.NewFakeHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	serv := new(sandboxTopo)
	resolver := newTestResolver(hc, serv, cell)
	sbc1 = hc.AddTestTablet(cell, "-20", 1, "TestExecutor", "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc2 = hc.AddTestTablet(cell, "40-60", 1, "TestExecutor", "40-60", topodatapb.TabletType_MASTER, true, 1, nil)
	// Create these connections so scatter queries don't fail.
	_ = hc.AddTestTablet(cell, "20-40", 1, "TestExecutor", "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "60-60", 1, "TestExecutor", "60-80", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "80-a0", 1, "TestExecutor", "80-a0", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "a0-c0", 1, "TestExecutor", "a0-c0", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "c0-e0", 1, "TestExecutor", "c0-e0", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "e0-", 1, "TestExecutor", "e0-", topodatapb.TabletType_MASTER, true, 1, nil)

	createSandbox(KsTestUnsharded)
	sbclookup = hc.AddTestTablet(cell, "0", 1, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	// Ues the 'X' in the name to ensure it's not alphabetically first.
	// Otherwise, it would become the default keyspace for the dual table.
	bad := createSandbox("TestXBadSharding")
	bad.VSchema = badVSchema

	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema

	executor = NewExecutor(context.Background(), serv, cell, "", resolver, false, testBufferSize)
	return executor, sbc1, sbc2, sbclookup
}

func executorExec(executor *Executor, sql string, bv map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return executor.Execute(context.Background(),
		masterSession,
		sql,
		bv)
}

func executorStream(executor *Executor, sql string) (qr *sqltypes.Result, err error) {
	results := make(chan *sqltypes.Result, 100)
	err = executor.StreamExecute(
		context.Background(),
		masterSession,
		sql,
		nil,
		querypb.Target{
			TabletType: topodatapb.TabletType_MASTER,
		},
		func(qr *sqltypes.Result) error {
			results <- qr
			return nil
		},
	)
	close(results)
	if err != nil {
		return nil, err
	}
	first := true
	for r := range results {
		if first {
			qr = &sqltypes.Result{Fields: r.Fields}
			first = false
		}
		qr.Rows = append(qr.Rows, r.Rows...)
	}
	return qr, nil
}
