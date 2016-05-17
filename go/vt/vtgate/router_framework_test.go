// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var routerVSchema = `
{
	"Sharded": true,
	"Vindexes": {
		"user_index": {
			"Type": "hash"
		},
		"music_user_map": {
			"Type": "lookup_hash_unique",
			"Owner": "music",
			"Params": {
				"Table": "music_user_map",
				"From": "music_id",
				"To": "user_id"
			}
		},
		"name_user_map": {
			"Type": "lookup_hash",
			"Owner": "user",
			"Params": {
				"Table": "name_user_map",
				"From": "name",
				"To": "user_id"
			}
		},
		"idx1": {
			"Type": "hash"
		},
		"idx_noauto": {
			"Type": "hash",
			"Owner": "noauto_table"
		},
		"keyspace_id": {
			"Type": "numeric"
		}
	},
	"Tables": {
		"user": {
			"ColVindexes": [
				{
					"Col": "Id",
					"Name": "user_index"
				},
				{
					"Col": "name",
					"Name": "name_user_map"
				}
			],
			"Autoinc" : {
				"Col": "id",
				"Sequence": "user_seq"
			}
		},
		"user_extra": {
			"ColVindexes": [
				{
					"Col": "user_id",
					"Name": "user_index"
				}
			]
		},
		"music": {
			"ColVindexes": [
				{
					"Col": "user_id",
					"Name": "user_index"
				},
				{
					"Col": "id",
					"Name": "music_user_map"
				}
			],
			"Autoinc" : {
				"Col": "id",
				"Sequence": "user_seq"
			}
		},
		"music_extra": {
			"ColVindexes": [
				{
					"Col": "user_id",
					"Name": "user_index"
				},
				{
					"Col": "music_id",
					"Name": "music_user_map"
				}
			]
		},
		"music_extra_reversed": {
			"ColVindexes": [
				{
					"Col": "music_id",
					"Name": "music_user_map"
				},
				{
					"Col": "user_id",
					"Name": "user_index"
				}
			]
		},
		"noauto_table": {
			"ColVindexes": [
				{
					"Col": "id",
					"Name": "idx_noauto"
				}
			]
		},
		"ksid_table": {
			"ColVindexes": [
				{
					"Col": "keyspace_id",
					"Name": "keyspace_id"
				}
			]
		}
	}
}
`
var badVSchema = `
{
	"Sharded": false,
	"Tables": {
		"sharded_table": {}
	}
}
`

var unshardedVSchema = `
{
	"Sharded": false,
	"Tables": {
		"user_seq": {
			"Type": "Sequence"
		},
		"music_user_map": {},
		"name_user_map": {}
	}
}
`

func createRouterEnv() (router *Router, sbc1, sbc2, sbclookup *sandboxConn) {
	cell := "aa"
	hc := newFakeHealthCheck()
	s := createSandbox("TestRouter")
	s.VSchema = routerVSchema
	sbc1 = &sandboxConn{}
	sbc2 = &sandboxConn{}
	hc.addTestTablet(cell, "-20", 1, "TestRouter", "-20", topodatapb.TabletType_MASTER, true, 1, nil, sbc1)
	hc.addTestTablet(cell, "40-60", 1, "TestRouter", "40-60", topodatapb.TabletType_MASTER, true, 1, nil, sbc2)

	createSandbox(KsTestUnsharded)
	sbclookup = &sandboxConn{}
	hc.addTestTablet(cell, "0", 1, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil, sbclookup)

	bad := createSandbox("TestBadSharding")
	bad.VSchema = badVSchema

	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(hc, topo.Server{}, serv, "", cell, 10, nil)
	router = NewRouter(context.Background(), serv, cell, "", scatterConn)
	return router, sbc1, sbc2, sbclookup
}

func routerExec(router *Router, sql string, bv map[string]interface{}) (*sqltypes.Result, error) {
	return router.Execute(context.Background(),
		sql,
		bv,
		"",
		topodatapb.TabletType_MASTER,
		nil,
		false)
}

func routerStream(router *Router, sql string) (qr *sqltypes.Result, err error) {
	results := make(chan *sqltypes.Result, 10)
	err = router.StreamExecute(context.Background(), sql, nil, "", topodatapb.TabletType_MASTER, func(qr *sqltypes.Result) error {
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
