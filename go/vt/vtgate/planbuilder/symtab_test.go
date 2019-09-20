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

package planbuilder

import (
	"encoding/json"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestSymtabAddVSchemaTable(t *testing.T) {
	tname := sqlparser.TableName{Name: sqlparser.NewTableIdent("t")}
	rb := &route{}

	tcases := []struct {
		in            []*vindexes.Table
		authoritative bool
		vindexes      [][]string
		err           string
	}{{
		// Single table.
		in: []*vindexes.Table{{
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}, {
				Name: sqlparser.NewColIdent("C2"),
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{}},
	}, {
		// Column vindex specified.
		in: []*vindexes.Table{{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("C1")},
			}},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}, {
				Name: sqlparser.NewColIdent("C2"),
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{"c1"}},
	}, {
		// Multi-column vindex.
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
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{"c1"}},
	}, {
		// AutoIncrement.
		in: []*vindexes.Table{{
			AutoIncrement: &vindexes.AutoIncrement{
				Column: sqlparser.NewColIdent("C1"),
			},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}, {
				Name: sqlparser.NewColIdent("C2"),
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{}},
	}, {
		// Column vindex specifies a column not in list.
		in: []*vindexes.Table{{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("C1")},
			}},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C2"),
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{"c1"}},
	}, {
		// Column vindex specifies columns with none in list.
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
		// AutoIncrement specifies a column not in list.
		in: []*vindexes.Table{{
			AutoIncrement: &vindexes.AutoIncrement{
				Column: sqlparser.NewColIdent("C1"),
			},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C2"),
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{}},
	}, {
		// Two tables.
		in: []*vindexes.Table{{
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C2"),
			}},
		}, {
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}},
		}},
		authoritative: false,
		vindexes:      [][]string{{}, {}},
	}, {
		// Two tables, with column vindexes.
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
		// One table with two column vindexes.
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
	}, {
		// First table is authoritative.
		in: []*vindexes.Table{{
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}, {
				Name: sqlparser.NewColIdent("C2"),
			}},
			ColumnListAuthoritative: true,
		}, {
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}},
		}},
		authoritative: true,
		vindexes:      [][]string{{}, {}},
	}, {
		// Both tables are authoritative.
		in: []*vindexes.Table{{
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}, {
				Name: sqlparser.NewColIdent("C2"),
			}},
			ColumnListAuthoritative: true,
		}, {
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}},
			ColumnListAuthoritative: true,
		}},
		authoritative: true,
		vindexes:      [][]string{{}, {}},
	}, {
		// Second table is authoritative.
		in: []*vindexes.Table{{
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}, {
				Name: sqlparser.NewColIdent("C2"),
			}},
		}, {
			Name: sqlparser.NewTableIdent("t1"),
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}},
			ColumnListAuthoritative: true,
		}},
		authoritative: true,
		vindexes:      [][]string{{}, {}},
		err:           "intermixing of authoritative and non-authoritative tables not allowed: t1",
	}, {
		// Cannot add to authoritative table (column list).
		in: []*vindexes.Table{{
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}},
			ColumnListAuthoritative: true,
		}, {
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C2"),
			}},
		}},
		err: "column C2 not found in t",
	}, {
		// Cannot add to authoritative table (column vindex).
		in: []*vindexes.Table{{
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}},
			ColumnListAuthoritative: true,
		}, {
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.ColIdent{
					sqlparser.NewColIdent("C2"),
				},
			}},
		}},
		err: "column C2 not found in t",
	}, {
		// Cannot add to authoritative table (autoinc).
		in: []*vindexes.Table{{
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}},
			ColumnListAuthoritative: true,
		}, {
			AutoIncrement: &vindexes.AutoIncrement{
				Column: sqlparser.NewColIdent("C2"),
			},
		}},
		err: "column C2 not found in t",
	}}

	out := []string{"c1", "c2"}
	for _, tcase := range tcases {
		st := newSymtab()
		vindexMaps, err := st.AddVSchemaTable(tname, tcase.in, rb)
		tcasein, _ := json.Marshal(tcase.in)
		if err != nil {
			if err.Error() != tcase.err {
				t.Errorf("st.AddVSchemaTable(%s) err: %v, want %s", tcasein, err, tcase.err)
			}
			continue
		} else if tcase.err != "" {
			t.Errorf("st.AddVSchemaTable(%s) succeeded, want error: %s", tcasein, tcase.err)
			continue
		}
		tab := st.tables[tname]
		for _, col := range out {
			if tab.columns[col] == nil {
				t.Errorf("st.AddVSchemaTable(%s): column %s not found", tcasein, col)
			}
		}
		for i, cols := range tcase.vindexes {
			for _, col := range cols {
				c := tab.columns[col]
				if c == nil {
					t.Errorf("st.AddVSchemaTable(%s): column %s not found", tcasein, col)
				}
				if _, ok := vindexMaps[i][c]; !ok {
					t.Errorf("st.AddVSchemaTable(%s).vindexMap[%d]: column %s not found", tcasein, i, col)
				}
			}
		}
		if tab.isAuthoritative != tcase.authoritative {
			t.Errorf("st.AddVSchemaTable(%s).authoritative: %v want %v", tcasein, tab.isAuthoritative, tcase.authoritative)
		}
	}
}
