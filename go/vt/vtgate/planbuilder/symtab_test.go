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

package planbuilder

import (
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestSymtabAddVindexTable(t *testing.T) {
	tname := sqlparser.TableName{Name: sqlparser.NewTableIdent("t")}
	rb := &route{}

	tcases := []struct {
		in            []*vindexes.Table
		authoritative bool
		vindexes      [][]string
	}{{
		in: []*vindexes.Table{{
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}, {
				Name: sqlparser.NewColIdent("C2"),
				Type: sqltypes.VarChar,
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{}},
	}, {
		in: []*vindexes.Table{{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("C1")},
			}},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}, {
				Name: sqlparser.NewColIdent("C2"),
				Type: sqltypes.VarChar,
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{"c1"}},
	}, {
		in: []*vindexes.Table{{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.ColIdent{
					sqlparser.NewColIdent("C1"),
					sqlparser.NewColIdent("C2"),
				},
			}},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}, {
				Name: sqlparser.NewColIdent("C2"),
				Type: sqltypes.VarChar,
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{"c1"}},
	}, {
		in: []*vindexes.Table{{
			AutoIncrement: &vindexes.AutoIncrement{
				Column: sqlparser.NewColIdent("C1"),
			},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}, {
				Name: sqlparser.NewColIdent("C2"),
				Type: sqltypes.VarChar,
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{}},
	}, {
		in: []*vindexes.Table{{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("C1")},
			}},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C2"),
				Type: sqltypes.VarChar,
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{"c1"}},
	}, {
		in: []*vindexes.Table{{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.ColIdent{
					sqlparser.NewColIdent("C1"),
					sqlparser.NewColIdent("C2"),
				},
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{"c1"}},
	}, {
		in: []*vindexes.Table{{
			AutoIncrement: &vindexes.AutoIncrement{
				Column: sqlparser.NewColIdent("C1"),
			},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C2"),
				Type: sqltypes.VarChar,
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{}},
	}, {
		in: []*vindexes.Table{{
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C2"),
				Type: sqltypes.VarChar,
			}},
		}, {
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
				Type: sqltypes.VarChar,
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{}, {}},
	}, {
		in: []*vindexes.Table{{
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C2"),
				Type: sqltypes.VarChar,
			}},
		}, {
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
				Type: sqltypes.VarChar,
			}},
			ColumnListAuthoritative: true,
		}},
		authoritative: true,
		vindexes:      [][]string{{}, {}},
	}, {
		in: []*vindexes.Table{{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.ColIdent{
					sqlparser.NewColIdent("C1"),
				},
			}},
		}, {
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.ColIdent{
					sqlparser.NewColIdent("C2"),
				},
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{"c1"}, {"c2"}},
	}, {
		in: []*vindexes.Table{{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.ColIdent{
					sqlparser.NewColIdent("C1"),
				},
			}, {
				Columns: []sqlparser.ColIdent{
					sqlparser.NewColIdent("C2"),
				},
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{"c1", "c2"}, {}},
	}}

	out := []string{"c1", "c2"}
	for _, tcase := range tcases {
		st := newSymtab()
		vindexMaps, err := st.AddVSchemaTable(tname, tcase.in, rb)
		if err != nil {
			t.Error(err)
			continue
		}
		tab := st.tables[tname]
		for _, col := range out {
			if tab.columns[col] == nil {
				t.Errorf("st.AddVSchemaTable(%+v): column %s not found", tcase.in, col)
			}
		}
		for i, cols := range tcase.vindexes {
			for _, col := range cols {
				c := tab.columns[col]
				if c == nil {
					t.Errorf("st.AddVSchemaTable(%+v): column %s not found", tcase.in, col)
				}
				if _, ok := vindexMaps[i][c]; !ok {
					t.Errorf("st.AddVSchemaTable(%+v).vindexMap[%d]: column %s not found", tcase.in, i, col)
				}
			}
		}
		if tab.isAuthoritative != tcase.authoritative {
			t.Errorf("st.AddVSchemaTable(%+v).authoritative: %v want %v", tcase.in, tab.isAuthoritative, tcase.authoritative)
		}
	}
}
