/*
Copyright 2021 The Vitess Authors.

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
// +build gofuzz

/*
	DEPENDENCIES:
	This fuzzer relies heavily on
	$VTROOT/go/vt/vtgate/engine/fake_vcursor_test.go,
	and in order to run it, it is required to rename:
	$VTROOT/go/vt/vtgate/engine/fake_vcursor_test.go
	to
	$VTROOT/go/vt/vtgate/engine/fake_vcursor.go

	This is handled by the OSS-fuzz build script and
	is only important to make note of if the fuzzer
	is run locally.

	STATUS:
	The fuzzer does not currently implement executions
	for all possible API's in the engine package, and
	it can be considered experimental, as I (@AdamKorcz)
	am interested in its performance when being run
	continuously by OSS-fuzz. Needless to say, more
	APIs can be added with ease.
*/

package engine

import (
	"errors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"

	fuzz "github.com/AdaLogics/go-fuzz-headers"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func createVSchema() (vschema *vindexes.VSchema, err error) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"primary": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "prim",
							"from":  "from1",
							"to":    "toc",
						},
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "primary",
							Columns: []string{"id"},
						}},
					},
				},
			},
		},
	}
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		return nil, err
	}
	return vs, nil
}

// FuzzEngine implements the fuzzer
func FuzzEngine(data []byte) int {
	c := fuzz.NewConsumer(data)
	vc := newFuzzDMLTestVCursor("0")
	vs, err := createVSchema()
	if err != nil {
		return -1
	}
	for i := 0; i < 20; i++ {
		newInt, err := c.GetInt()
		if err != nil {
			return -1
		}
		execCommand(newInt%7, c, vc, vs)
	}
	return 1
}

func execUnshardedUpdate(query string, vc *loggingVCursor) {
	upd := &Update{
		DML: DML{
			Opcode: Unsharded,
			Keyspace: &vindexes.Keyspace{
				Name:    "ks",
				Sharded: false,
			},
			Query: query,
		},
	}
	_, _ = upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
}

func execUnshardedInsert(query string, vc *loggingVCursor) {
	ins := &Insert{
		Opcode: InsertUnsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		Query: query,
	}
	_, _ = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
}

func execShardedInsert(query string, vc *loggingVCursor, vs *vindexes.VSchema) {
	ks := vs.Keyspaces["sharded"]
	ins := NewInsert(
		InsertSharded,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			Values: []sqltypes.PlanValue{{
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewVarChar(query),
				}},
			}},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)
	_, _ = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
}

func execUnshardedRoute(query, field string, vc *loggingVCursor) {
	sel := NewRoute(
		SelectUnsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		query,
		field,
	)
	_, _ = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
}

func execShardedRoute(query, field string, vc *loggingVCursor) {
	sel := NewRoute(
		SelectScatter,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		query,
		field,
	)
	_, _ = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
}

func execEqualUniqueRoute(query, field, hash, value string, vc *loggingVCursor) {
	vindex, _ := vindexes.NewHash(hash, nil)
	sel := NewRoute(
		SelectEqualUnique,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		query,
		field,
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []sqltypes.PlanValue{{Value: sqltypes.NewVarChar(value)}}
	_, _ = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
}

var defaultFuzzSelectResult = sqltypes.MakeTestResult(
	sqltypes.MakeTestFields(
		"id",
		"int64",
	),
	"1",
)

func execRouteSelectDBA(query, field, tablename, schema, shards string) {
	stringToExpr := func(in string) []evalengine.Expr {
		var schema []evalengine.Expr
		if in != "" {
			schema = []evalengine.Expr{evalengine.NewLiteralString([]byte(in))}
		}
		return schema
	}

	sel := &Route{
		Opcode: SelectDBA,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		Query:               query,
		FieldQuery:          field,
		SysTableTableSchema: stringToExpr(schema),
		SysTableTableName:   stringToExpr(tablename),
	}
	vc := &loggingVCursor{
		shards:  []string{shards},
		results: []*sqltypes.Result{defaultFuzzSelectResult},
	}

	vc.tableRoutes = tableRoutes{
		tbl: &vindexes.Table{
			Name:     sqlparser.NewTableIdent("routedTable"),
			Keyspace: &vindexes.Keyspace{Name: "routedKeyspace"},
		}}
	_, _ = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
}

func queryAndField(c *fuzz.ConsumeFuzzer) (string, string, error) {
	query, err := c.GetString()
	if err != nil {
		return "nil", "nil", err
	}
	field, err := c.GetString()
	if err != nil {
		return "nil", "nil", err
	}
	return query, field, nil
}

func execCommand(index int, c *fuzz.ConsumeFuzzer, vc *loggingVCursor, vs *vindexes.VSchema) error {
	switch i := index; i {
	case 0:
		query, err := c.GetString()
		if err != nil {
			return err
		}
		execUnshardedUpdate(query, vc)
		return nil
	case 1:
		query, err := c.GetString()
		if err != nil {
			return err
		}
		execUnshardedInsert(query, vc)
		return nil
	case 2:
		query, err := c.GetString()
		if err != nil {
			return err
		}
		execShardedInsert(query, vc, vs)
		return nil
	case 3:
		query, field, err := queryAndField(c)
		if err != nil {
			return err
		}
		execUnshardedRoute(query, field, vc)
		return nil
	case 4:
		query, field, err := queryAndField(c)
		if err != nil {
			return err
		}
		execShardedRoute(query, field, vc)
		return nil
	case 5:
		query, field, err := queryAndField(c)
		if err != nil {
			return err
		}
		hash, err := c.GetString()
		if err != nil {
			return err
		}
		value, err := c.GetString()
		if err != nil {
			return err
		}
		execEqualUniqueRoute(query, field, hash, value, vc)
		return nil
	case 6:
		query, field, err := queryAndField(c)
		if err != nil {
			return err
		}
		tablename, err := c.GetString()
		if err != nil {
			return err
		}
		schema, err := c.GetString()
		if err != nil {
			return err
		}
		shards, err := c.GetString()
		if err != nil {
			return err
		}
		execRouteSelectDBA(query, field, tablename, schema, shards)
		return nil
	default:
		return errors.New("Could not exec")
	}
}

func newFuzzDMLTestVCursor(shards ...string) *loggingVCursor {
	return &loggingVCursor{shards: shards, resolvedTargetTabletType: topodatapb.TabletType_MASTER}
}
