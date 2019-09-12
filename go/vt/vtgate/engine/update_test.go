/*
Copyright 2018 The Vitess Authors.

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

package engine

import (
	"errors"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func TestUpdateUnsharded(t *testing.T) {
	upd := &Update{
		Opcode: UpdateUnsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		Query: "dummy_update",
	}

	vc := &loggingVCursor{shards: []string{"0"}}
	_, err := upd.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: dummy_update {} true true`,
	})

	// Failure cases
	vc = &loggingVCursor{shardErr: errors.New("shard_error")}
	_, err = upd.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "execUpdateUnsharded: shard_error")

	vc = &loggingVCursor{}
	_, err = upd.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "Keyspace does not have exactly one shard: []")
}

func TestUpdateEqual(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	upd := &Update{
		Opcode: UpdateEqual,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:  "dummy_update",
		Vindex: vindex,
		Values: []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	_, err := upd.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard ks.-20: dummy_update /* vtgate:: keyspace_id:166b40b44aba4bd6 */ {} true true`,
	})

	// Failure case
	upd.Values = []sqltypes.PlanValue{{Key: "aa"}}
	_, err = upd.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "execUpdateEqual: missing bind var aa")
}

func TestUpdateScatter(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	upd := &Update{
		Opcode: UpdateScatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:  "dummy_update",
		Vindex: vindex,
		Values: []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	_, err := upd.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}

	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_update {} ks.20-: dummy_update {} true false`,
	})

	// works with multishard autocommit
	upd = &Update{
		Opcode: UpdateScatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:                "dummy_update",
		Vindex:               vindex,
		Values:               []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
		MultiShardAutocommit: true,
	}

	vc = &loggingVCursor{shards: []string{"-20", "20-"}}
	_, err = upd.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}

	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_update {} ks.20-: dummy_update {} true true`,
	})
}

func TestUpdateEqualNoRoute(t *testing.T) {
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
	})
	upd := &Update{
		Opcode: UpdateEqual,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:  "dummy_update",
		Vindex: vindex,
		Values: []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
	}

	vc := &loggingVCursor{shards: []string{"0"}}
	_, err := upd.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// This lookup query will return no rows. So, the DML will not be sent anywhere.
		`Execute select toc from lkp where from = :from from: type:INT64 value:"1"  false`,
	})
}

func TestUpdateEqualNoScatter(t *testing.T) {
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table":      "lkp",
		"from":       "from",
		"to":         "toc",
		"write_only": "true",
	})
	upd := &Update{
		Opcode: UpdateEqual,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:  "dummy_update",
		Vindex: vindex,
		Values: []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
	}

	vc := &loggingVCursor{shards: []string{"0"}}
	_, err := upd.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "execUpdateEqual: cannot map vindex to unique keyspace id: DestinationKeyRange(-)")
}

func TestUpdateEqualChangedVindex(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{
		Opcode:   UpdateEqual,
		Keyspace: ks.Keyspace,
		Query:    "dummy_update",
		Vindex:   ks.Vindexes["hash"],
		Values:   []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
		ChangedVindexValues: map[string][]sqltypes.PlanValue{
			"twocol": {{
				Value: sqltypes.NewInt64(1),
			}, {
				Value: sqltypes.NewInt64(2),
			}},
			"onecol": {{
				Value: sqltypes.NewInt64(3),
			}},
		},
		Table:            ks.Tables["t1"],
		OwnedVindexQuery: "dummy_subquery",
	}

	results := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"c1|c2|c3",
			"int64|int64|int64",
		),
		"4|5|6",
	)}
	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: results,
	}

	_, err := upd.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from10, :from20, :toc0) from10: type:INT64 value:"1" from20: type:INT64 value:"2" toc0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 6 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp1(from, toc) values(:from0, :toc0) from0: type:INT64 value:"3" toc0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update /* vtgate:: keyspace_id:166b40b44aba4bd6 */ {} true true`,
	})

	// No rows changing
	vc = &loggingVCursor{
		shards: []string{"-20", "20-"},
	}
	_, err = upd.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Subquery returns no rows. So, no vindexes are updated. We still pass-through the original update.
		`ExecuteMultiShard sharded.-20: dummy_update /* vtgate:: keyspace_id:166b40b44aba4bd6 */ {} true true`,
	})

	// Failure case: multiple rows changing.
	results = []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"c1|c2|c3",
			"int64|int64|int64",
		),
		"4|5|6",
		"7|8|9",
	)}
	vc = &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: results,
	}
	_, err = upd.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "execUpdateEqual: unsupported: update changes multiple rows in the vindex")
}

func TestUpdateNoStream(t *testing.T) {
	upd := &Update{}
	err := upd.StreamExecute(context.Background(), nil, nil, false, nil)
	expectError(t, "StreamExecute", err, `query "" cannot be used for streaming`)
}

func buildTestVSchema() *vindexes.VSchema {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
					"twocol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp2",
							"from":  "from1,from2",
							"to":    "toc",
						},
						Owner: "t1",
					},
					"onecol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp1",
							"from":  "from",
							"to":    "toc",
						},
						Owner: "t1",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"},
						}, {
							Name:    "twocol",
							Columns: []string{"c1", "c2"},
						}, {
							Name:    "onecol",
							Columns: []string{"c3"},
						}},
					},
				},
			},
		},
	}
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		panic(err)
	}
	return vs
}
