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

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

func TestSymtabAddVindexTable(t *testing.T) {
	tname := sqlparser.TableName{Name: sqlparser.NewTableIdent("t")}
	rb := &route{}

	tcases := []struct {
		in  *vindexes.Table
		out []string
	}{{
		in: &vindexes.Table{
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}, {
				Name: sqlparser.NewColIdent("C2"),
				Type: sqltypes.VarChar,
			}},
		},
		out: []string{"c1", "c2"},
	}, {
		in: &vindexes.Table{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("C1")},
			}},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}, {
				Name: sqlparser.NewColIdent("C2"),
				Type: sqltypes.VarChar,
			}},
		},
		out: []string{"c1", "c2"},
	}, {
		in: &vindexes.Table{
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
		},
		out: []string{"c1", "c2"},
	}, {
		in: &vindexes.Table{
			AutoIncrement: &vindexes.AutoIncrement{
				Column: sqlparser.NewColIdent("C1"),
			},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C1"),
			}, {
				Name: sqlparser.NewColIdent("C2"),
				Type: sqltypes.VarChar,
			}},
		},
		out: []string{"c1", "c2"},
	}, {
		in: &vindexes.Table{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("C1")},
			}},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C2"),
				Type: sqltypes.VarChar,
			}},
		},
		out: []string{"c1", "c2"},
	}, {
		in: &vindexes.Table{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.ColIdent{
					sqlparser.NewColIdent("C1"),
					sqlparser.NewColIdent("C2"),
				},
			}},
		},
		out: []string{"c1", "c2"},
	}, {
		in: &vindexes.Table{
			AutoIncrement: &vindexes.AutoIncrement{
				Column: sqlparser.NewColIdent("C1"),
			},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("C2"),
				Type: sqltypes.VarChar,
			}},
		},
		out: []string{"c1", "c2"},
	}}

	for _, tcase := range tcases {
		st := newSymtab(nil)
		err := st.AddVindexTable(tname, tcase.in, rb)
		if err != nil {
			t.Error(err)
			continue
		}
		tab := st.tables[tname]
		for _, col := range tcase.out {
			if tab.columns[col] == nil {
				t.Errorf("st.AddVindexTable(%+v): column %s not found", tcase.in, col)
			}
		}
	}
}
