// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var routerVSchema = createTestVSchema(`
{
  "Keyspaces": {
    "TestRouter": {
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
      "Classes": {
        "user": {
          "ColVindexes": [
            {
              "Col": "id",
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
      },
      "Tables": {
        "user": "user",
        "user_extra": "user_extra",
        "music": "music",
        "music_extra": "music_extra",
        "music_extra_reversed": "music_extra_reversed",
        "noauto_table": "noauto_table",
        "ksid_table": "ksid_table"
      }
    },
    "TestBadSharding": {
      "Sharded": false,
      "Tables": {
        "sharded_table": ""
      }
    },
    "TestUnsharded": {
      "Sharded": false,
			"Classes": {
				"seq": {
					"Type": "Sequence"
				}
			},
      "Tables": {
        "user_seq": "seq",
        "music_user_map": "",
        "name_user_map": ""
      }
    }
  }
}
`)

// createTestVSchema creates a vschema based on the JSON specs.
// It panics on failure.
func createTestVSchema(vschemaJSON string) *planbuilder.VSchema {
	f, err := ioutil.TempFile("", "vtgate_schema")
	if err != nil {
		panic(err)
	}
	fname := f.Name()
	f.Close()
	defer os.Remove(fname)

	err = ioutil.WriteFile(fname, []byte(vschemaJSON), 0644)
	if err != nil {
		panic(err)
	}
	vschema, err := planbuilder.LoadFile(fname)
	if err != nil {
		panic(err)
	}
	return vschema
}

func createRouterEnv() (router *Router, sbc1, sbc2, sbclookup *sandboxConn) {
	s := createSandbox("TestRouter")
	sbc1 = &sandboxConn{}
	sbc2 = &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)

	l := createSandbox(KsTestUnsharded)
	sbclookup = &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	createSandbox("TestBadSharding")

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(nil, topo.Server{}, serv, "", "aa", 1*time.Second, 10, 20*time.Millisecond, 10*time.Millisecond, 24*time.Hour, nil, "")
	router = NewRouter(serv, "aa", routerVSchema, "", scatterConn)
	return router, sbc1, sbc2, sbclookup
}

func routerExec(router *Router, sql string, bv map[string]interface{}) (*sqltypes.Result, error) {
	return router.Execute(context.Background(),
		sql,
		bv,
		topodatapb.TabletType_MASTER,
		nil,
		false)
}

func routerStream(router *Router, sql string) (qr *sqltypes.Result, err error) {
	results := make(chan *sqltypes.Result, 10)
	err = router.StreamExecute(context.Background(), sql, nil, topodatapb.TabletType_MASTER, func(qr *sqltypes.Result) error {
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
