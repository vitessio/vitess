// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/vttablet/sandboxconn"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var routerVSchema = `
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

func createRouterEnv() (router *Router, sbc1, sbc2, sbclookup *sandboxconn.SandboxConn) {
	cell := "aa"
	hc := discovery.NewFakeHealthCheck()
	s := createSandbox("TestRouter")
	s.VSchema = routerVSchema
	serv := new(sandboxTopo)
	scatterConn := newTestScatterConn(hc, serv, cell)
	sbc1 = hc.AddTestTablet(cell, "-20", 1, "TestRouter", "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc2 = hc.AddTestTablet(cell, "40-60", 1, "TestRouter", "40-60", topodatapb.TabletType_MASTER, true, 1, nil)

	createSandbox(KsTestUnsharded)
	sbclookup = hc.AddTestTablet(cell, "0", 1, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	bad := createSandbox("TestBadSharding")
	bad.VSchema = badVSchema

	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema

	router = NewRouter(context.Background(), serv, cell, "", scatterConn, false)
	return router, sbc1, sbc2, sbclookup
}

func routerExec(router *Router, sql string, bv map[string]interface{}) (*sqltypes.Result, error) {
	return router.Execute(context.Background(),
		sql,
		bv,
		"",
		topodatapb.TabletType_MASTER,
		nil,
		false,
		nil)
}

func routerStream(router *Router, sql string) (qr *sqltypes.Result, err error) {
	results := make(chan *sqltypes.Result, 10)
	err = router.StreamExecute(context.Background(), sql, nil, "", topodatapb.TabletType_MASTER, nil, func(qr *sqltypes.Result) error {
		results <- qr
		return nil
	})
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
		qr.RowsAffected += r.RowsAffected
	}
	return qr, nil
}
