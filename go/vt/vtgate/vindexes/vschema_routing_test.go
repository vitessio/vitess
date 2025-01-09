/*
Copyright 2025 The Vitess Authors.

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

package vindexes

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
)

// TestAutoGlobalRoutingExtended tests the global routing of tables across various keyspace configurations,
// including unsharded and sharded keyspaces, with and without the RequireExplicitRouting flag.
func TestAutoGlobalRoutingExtended(t *testing.T) {
	isTableGloballyRoutable := func(vschema *VSchema, tableName string) (isGlobal, isAmbiguous bool) {
		table, err := vschema.FindTable("", tableName)
		if err != nil {
			if strings.Contains(err.Error(), "ambiguous") {
				return false, true
			}
			return false, false
		}
		return table != nil, false
	}
	type testKeySpace struct {
		name string
		ks   *vschemapb.Keyspace
	}
	unsharded1 := &testKeySpace{
		name: "unsharded1",
		ks: &vschemapb.Keyspace{
			Tables: map[string]*vschemapb.Table{
				"table1":   {},
				"table2":   {},
				"scommon1": {},
				"ucommon3": {},
			},
		},
	}
	unsharded2 := &testKeySpace{
		name: "unsharded2",
		ks: &vschemapb.Keyspace{
			Tables: map[string]*vschemapb.Table{
				"table3":   {},
				"table4":   {},
				"scommon1": {},
				"scommon2": {},
				"ucommon3": {},
			},
		},
	}
	sharded1 := &testKeySpace{
		name: "sharded1",
		ks: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"xxhash": {
					Type: "xxhash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"table5":   {},
				"scommon1": {},
				"scommon2": {},
			},
		},
	}
	sharded2 := &testKeySpace{
		name: "sharded2",
		ks: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"xxhash": {
					Type: "xxhash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"table6":   {},
				"scommon2": {},
				"scommon3": {},
			},
		},
	}
	for _, tables := range []*vschemapb.Keyspace{sharded1.ks, sharded2.ks} {
		for _, t := range tables.Tables {
			t.ColumnVindexes = append(t.ColumnVindexes, &vschemapb.ColumnVindex{
				Column: "c1",
				Name:   "xxhash",
			})
		}
	}
	type testCase struct {
		name               string
		keyspaces          []*testKeySpace
		expGlobalTables    []string
		expAmbiguousTables []string
		explicit           []string
	}
	testCases := []testCase{
		{
			name:               "no keyspaces",
			keyspaces:          []*testKeySpace{},
			expGlobalTables:    nil,
			expAmbiguousTables: nil,
		},
		{
			name:               "one unsharded keyspace",
			keyspaces:          []*testKeySpace{unsharded1},
			expGlobalTables:    []string{"table1", "table2", "scommon1", "ucommon3"},
			expAmbiguousTables: nil,
		},
		{
			name:               "two unsharded keyspaces",
			keyspaces:          []*testKeySpace{unsharded1, unsharded2},
			expGlobalTables:    []string{"table1", "table2", "table3", "table4", "scommon2"},
			expAmbiguousTables: []string{"scommon1", "ucommon3"},
		},
		{
			name:            "two unsharded keyspaces, one with RequireExplicitRouting",
			keyspaces:       []*testKeySpace{unsharded1, unsharded2},
			explicit:        []string{"unsharded1"},
			expGlobalTables: []string{"table3", "table4", "scommon1", "scommon2", "ucommon3"},
		},
		{
			name:            "one sharded keyspace",
			keyspaces:       []*testKeySpace{sharded1},
			expGlobalTables: []string{"table5", "scommon1", "scommon2"},
		},
		{
			name:               "two sharded keyspaces",
			keyspaces:          []*testKeySpace{sharded1, sharded2},
			expGlobalTables:    []string{"table5", "table6", "scommon1", "scommon3"},
			expAmbiguousTables: []string{"scommon2"},
		},
		{
			name:            "two sharded keyspaces, one with RequireExplicitRouting",
			keyspaces:       []*testKeySpace{sharded1, sharded2},
			explicit:        []string{"sharded2"},
			expGlobalTables: []string{"table5", "scommon1", "scommon2"},
		},
		{
			name:            "two sharded keyspaces, both with RequireExplicitRouting",
			keyspaces:       []*testKeySpace{sharded1, sharded2},
			explicit:        []string{"sharded1", "sharded2"},
			expGlobalTables: nil,
		},
		{
			name:               "two sharded keyspaces, one unsharded keyspace",
			keyspaces:          []*testKeySpace{sharded1, sharded2, unsharded1},
			expGlobalTables:    []string{"table1", "table2", "table5", "table6", "scommon3", "ucommon3"},
			expAmbiguousTables: []string{"scommon1", "scommon2"},
		},
		{
			name:               "two sharded keyspaces, one unsharded keyspace, one with RequireExplicitRouting",
			keyspaces:          []*testKeySpace{sharded1, sharded2, unsharded1, unsharded2},
			explicit:           []string{"unsharded1"},
			expGlobalTables:    []string{"table3", "table4", "table5", "table6", "scommon3", "ucommon3"},
			expAmbiguousTables: []string{"scommon1", "scommon2"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			allTables := make(map[string]bool)
			source := &vschemapb.SrvVSchema{
				Keyspaces: make(map[string]*vschemapb.Keyspace),
			}
			for _, ks := range tc.keyspaces {
				source.Keyspaces[ks.name] = ks.ks
				// set it false for all keyspaces here and later override for those requested in the test case
				ks.ks.RequireExplicitRouting = false
				for tname := range ks.ks.Tables {
					_, ok := allTables[tname]
					if !ok {
						allTables[tname] = true
					}
				}
			}
			for _, ksName := range tc.explicit {
				source.Keyspaces[ksName].RequireExplicitRouting = true
			}
			vschema := BuildVSchema(source, sqlparser.NewTestParser())
			require.NotNil(t, vschema)
			AddAdditionalGlobalTables(source, vschema)

			var globalTables, ambiguousTables []string
			for tname := range allTables {
				isGlobal, isAmbiguous := isTableGloballyRoutable(vschema, tname)
				if isGlobal {
					globalTables = append(globalTables, tname)
				} else if isAmbiguous {
					ambiguousTables = append(ambiguousTables, tname)
				}
			}
			sort.Strings(globalTables)
			sort.Strings(ambiguousTables)
			sort.Strings(tc.expGlobalTables)
			sort.Strings(tc.expAmbiguousTables)
			require.EqualValuesf(t, tc.expGlobalTables, globalTables, "global tables mismatch")
			require.EqualValuesf(t, tc.expAmbiguousTables, ambiguousTables, "ambiguous tables mismatch")
		})
	}
}

// TestAutoGlobalRouting tests adding tables in unsharded keyspaces to global routing if they don't have
// an associated VSchema which has the RequireExplicitRouting flag set. These tables should also not be
// already part of the global routing tables via the VSchema of sharded keyspaces.
func TestAutoGlobalRoutingBasic(t *testing.T) {
	// Create two unsharded keyspaces and two sharded keyspaces, each with some common tables.
	unsharded1 := &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"table1":   {}, // unique, should be added to global routing
			"table2":   {}, // unique, should be added to global routing
			"scommon1": {}, // common with sharded1, should not be added to global routing because it is already global because of sharded1
			"ucommon3": {}, // common with unsharded2, should not be added to global routing because it is ambiguous because of unsharded2
		},
	}
	unsharded2 := &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"table3":   {}, // unique, should be added to global routing
			"table4":   {}, // unique, should be added to global routing
			"scommon1": {}, // common with sharded1, should not be added to global routing because it is already global because of sharded1
			"scommon2": {}, // common with sharded1, should not be added to global routing because it is already global because of sharded1
			"ucommon3": {}, // common with unsharded1, should not be added to global routing because it is ambiguous because of unsharded1
		},
	}
	sharded1 := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"table5":   {}, // unique, should be added to global routing
			"scommon1": {}, // common with unsharded1 and unsharded2, should be added to global routing because it's in the vschema
			"scommon2": {}, // common with unsharded2, not ambiguous because sharded2 sets RequireExplicitRouting
		},
	}
	sharded2 := &vschemapb.Keyspace{
		Sharded:                true,
		RequireExplicitRouting: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		// none should be considered for choice as global or ambiguous because of RequireExplicitRouting
		Tables: map[string]*vschemapb.Table{
			"table6":   {}, // unique
			"scommon2": {}, // common with sharded1, but has RequireExplicitRouting
			"scommon3": {}, // unique
		},
	}
	for _, ks := range []*vschemapb.Keyspace{sharded1, sharded2} {
		for _, t := range ks.Tables {
			t.ColumnVindexes = append(t.ColumnVindexes, &vschemapb.ColumnVindex{
				Column: "c1",
				Name:   "xxhash",
			})
		}
	}
	source := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded1": sharded1,
			"sharded2": sharded2,
		},
	}

	vschema := BuildVSchema(source, sqlparser.NewTestParser())
	require.NotNil(t, vschema)

	// Check table is global
	mustRouteGlobally := func(t *testing.T, tname, ksName string) {
		t.Helper()
		_, err := vschema.FindTable("", tname)
		require.NoError(t, err)
		// The vtgate data structures don't always set the keyspace name, so cannot reliably check that at the moment.
		_ = ksName
	}

	mustNotRouteGlobally := func(t *testing.T, tname string) {
		t.Helper()
		_, err := vschema.FindTable("", tname)
		require.Error(t, err)
	}

	// Verify the global tables
	ks := vschema.Keyspaces["sharded1"]
	require.EqualValues(t, vschema.globalTables, map[string]*Table{
		"table5":   ks.Tables["table5"],
		"scommon1": ks.Tables["scommon1"],
		"scommon2": ks.Tables["scommon2"],
	})
	mustRouteGlobally(t, "table5", "sharded1")
	mustRouteGlobally(t, "scommon1", "sharded1")
	mustRouteGlobally(t, "scommon2", "sharded1")

	// Add unsharded keyspaces to SrvVSchema and build VSchema
	var err error
	source.Keyspaces["unsharded1"] = unsharded1
	source.Keyspaces["unsharded2"] = unsharded2
	vschema.Keyspaces["unsharded1"], err = BuildKeyspace(unsharded1, sqlparser.NewTestParser())
	require.NoError(t, err)
	vschema.Keyspaces["unsharded2"], err = BuildKeyspace(unsharded2, sqlparser.NewTestParser())
	require.NoError(t, err)

	// Verify the global tables don't change
	mustRouteGlobally(t, "table5", "sharded1")
	mustRouteGlobally(t, "scommon1", "sharded1")
	mustRouteGlobally(t, "scommon2", "sharded1")

	// Add additional global tables and then verify that the unsharded global tables are added
	AddAdditionalGlobalTables(source, vschema)

	mustRouteGlobally(t, "table1", "unsharded1")
	mustRouteGlobally(t, "table2", "unsharded1")

	mustRouteGlobally(t, "table3", "unsharded2")
	mustRouteGlobally(t, "table4", "unsharded2")
	mustNotRouteGlobally(t, "ucommon3")

	mustRouteGlobally(t, "scommon1", "sharded1")
	mustRouteGlobally(t, "scommon2", "sharded1")
	mustRouteGlobally(t, "table5", "sharded1")

	mustNotRouteGlobally(t, "table6")
	mustNotRouteGlobally(t, "scommon3")
}

func TestVSchemaRoutingRules(t *testing.T) {
	input := vschemapb.SrvVSchema{
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{{
				FromTable: "rt1",
				ToTables:  []string{"ks1.t1", "ks2.t2"},
			}, {
				FromTable: "rt2",
				ToTables:  []string{"ks2.t2"},
			}, {
				FromTable: "escaped",
				ToTables:  []string{"`ks2`.`t2`"},
			}, {
				FromTable: "dup",
				ToTables:  []string{"ks1.t1"},
			}, {
				FromTable: "dup",
				ToTables:  []string{"ks1.t1"},
			}, {
				FromTable: "badname",
				ToTables:  []string{"t1.t2.t3"},
			}, {
				FromTable: "unqualified",
				ToTables:  []string{"t1"},
			}, {
				FromTable: "badkeyspace",
				ToTables:  []string{"ks3.t1"},
			}, {
				FromTable: "notfound",
				ToTables:  []string{"ks1.t2"},
			}},
		},
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks1": {
				Sharded:        true,
				ForeignKeyMode: vschemapb.Keyspace_unmanaged,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu1": {
						Type: "stfu",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stfu1",
							},
						},
					},
				},
			},
			"ks2": {
				ForeignKeyMode: vschemapb.Keyspace_managed,
				Tables: map[string]*vschemapb.Table{
					"t2": {},
				},
			},
		},
	}
	got := BuildVSchema(&input, sqlparser.NewTestParser())
	ks1 := &Keyspace{
		Name:    "ks1",
		Sharded: true,
	}
	ks2 := &Keyspace{
		Name: "ks2",
	}
	vindex1 := &stFU{
		name: "stfu1",
	}
	t1 := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ks1,
		ColumnVindexes: []*ColumnVindex{{
			Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c1")},
			Type:     "stfu",
			Name:     "stfu1",
			Vindex:   vindex1,
			isUnique: vindex1.IsUnique(),
			cost:     vindex1.Cost(),
		}},
	}
	t1.Ordered = []*ColumnVindex{
		t1.ColumnVindexes[0],
	}
	t2 := &Table{
		Name:     sqlparser.NewIdentifierCS("t2"),
		Keyspace: ks2,
	}
	want := &VSchema{
		MirrorRules: map[string]*MirrorRule{},
		RoutingRules: map[string]*RoutingRule{
			"rt1": {
				Error: errors.New("table rt1 has more than one target: [ks1.t1 ks2.t2]"),
			},
			"rt2": {
				Tables: []*Table{t2},
			},
			"escaped": {
				Tables: []*Table{t2},
			},
			"dup": {
				Error: errors.New("duplicate rule for entry dup"),
			},
			"badname": {
				Error: errors.New("invalid table name: 't1.t2.t3', it must be of the qualified form <keyspace_name>.<table_name> (dots are not allowed in either name)"),
			},
			"unqualified": {
				Error: errors.New("invalid table name: 't1', it must be of the qualified form <keyspace_name>.<table_name> (dots are not allowed in either name)"),
			},
			"badkeyspace": {
				Error: errors.New("VT05003: unknown database 'ks3' in vschema"),
			},
			"notfound": {
				Error: errors.New("table t2 not found"),
			},
		},
		globalTables: map[string]*Table{
			"t1": t1,
			"t2": t2,
		},
		uniqueVindexes: map[string]Vindex{
			"stfu1": vindex1,
		},
		Keyspaces: map[string]*KeyspaceSchema{
			"ks1": {
				Keyspace:       ks1,
				ForeignKeyMode: vschemapb.Keyspace_unmanaged,
				Tables: map[string]*Table{
					"t1": t1,
				},
				Vindexes: map[string]Vindex{
					"stfu1": vindex1,
				},
			},
			"ks2": {
				ForeignKeyMode: vschemapb.Keyspace_managed,
				Keyspace:       ks2,
				Tables: map[string]*Table{
					"t2": t2,
				},
				Vindexes: map[string]Vindex{},
			},
		},
	}
	gotb, _ := json.MarshalIndent(got, "", "  ")
	wantb, _ := json.MarshalIndent(want, "", "  ")
	assert.Equal(t, string(wantb), string(gotb), string(gotb))
}
