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
	"testing"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

/*
func TestSymtabAddVSchemaTable(t *testing.T) {
	tname := sqlparser.TableName{Name: sqlparser.NewIdentifierCS("t")}
	rb := &route{}

	null, _ := vindexes.CreateVindex("null", "null", nil)

	tcases := []struct {
		in            *vindexes.Table
		authoritative bool
		vindex        []string
		err           string
	}{{
		// Single table.
		in: &vindexes.Table{
			Columns: []vindexes.Column{{
				Name: sqlparser.NewIdentifierCI("C1"),
			}, {
				Name: sqlparser.NewIdentifierCI("C2"),
			}},
		},
		authoritative: false,
		vindex:        []string{},
	}, {
		// Column vindex specified.
		in: &vindexes.Table{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("C1")},
				Vindex:  null,
			}},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewIdentifierCI("C1"),
			}, {
				Name: sqlparser.NewIdentifierCI("C2"),
			}},
		},
		authoritative: false,
		vindex:        []string{"c1"},
	}, {
		// Multi-column vindex.
		in: &vindexes.Table{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.IdentifierCI{
					sqlparser.NewIdentifierCI("C1"),
					sqlparser.NewIdentifierCI("C2"),
				},
				Vindex: null,
			}},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewIdentifierCI("C1"),
			}, {
				Name: sqlparser.NewIdentifierCI("C2"),
			}},
		},
		authoritative: false,
		vindex:        []string{"c1"},
	}, {
		// AutoIncrement.
		in: &vindexes.Table{
			AutoIncrement: &vindexes.AutoIncrement{
				Column: sqlparser.NewIdentifierCI("C1"),
			},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewIdentifierCI("C1"),
			}, {
				Name: sqlparser.NewIdentifierCI("C2"),
			}},
		},
		authoritative: false,
		vindex:        []string{},
	}, {
		// Column vindex specifies a column not in list.
		in: &vindexes.Table{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("C1")},
				Vindex:  null,
			}},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewIdentifierCI("C2"),
			}},
		},
		authoritative: false,
		vindex:        []string{"c1"},
	}, {
		// Column vindex specifies columns with none in list.
		in: &vindexes.Table{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.IdentifierCI{
					sqlparser.NewIdentifierCI("C1"),
					sqlparser.NewIdentifierCI("C2"),
				},
				Vindex: null,
			}},
		},
		authoritative: false,
		vindex:        []string{"c1"},
	}, {
		// AutoIncrement specifies a column not in list.
		in: &vindexes.Table{
			AutoIncrement: &vindexes.AutoIncrement{
				Column: sqlparser.NewIdentifierCI("C1"),
			},
			Columns: []vindexes.Column{{
				Name: sqlparser.NewIdentifierCI("C2"),
			}},
		},
		authoritative: false,
		vindex:        []string{},
	}, {
		// Two column vindexes.
		in: &vindexes.Table{
			ColumnVindexes: []*vindexes.ColumnVindex{{
				Columns: []sqlparser.IdentifierCI{
					sqlparser.NewIdentifierCI("C1"),
				},
				Vindex: null,
			}, {
				Columns: []sqlparser.IdentifierCI{
					sqlparser.NewIdentifierCI("C2"),
				},
				Vindex: null,
			}},
		},
		authoritative: false,
		vindex:        []string{"c1", "c2"},
	}}

	out := []string{"c1", "c2"}
	for _, tcase := range tcases {
		st := newSymtab()
		vindexMap, err := st.AddVSchemaTable(tname, tcase.in, rb)
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
		for _, col := range tcase.vindex {
			c := tab.columns[col]
			if c == nil {
				t.Errorf("st.AddVSchemaTable(%s): column %s not found", tcasein, col)
			}
			if _, ok := vindexMap[c]; !ok {
				t.Errorf("st.AddVSchemaTable(%s).vindexMap: column %s not found", tcasein, col)
			}
		}
		if tab.isAuthoritative != tcase.authoritative {
			t.Errorf("st.AddVSchemaTable(%s).authoritative: %v want %v", tcasein, tab.isAuthoritative, tcase.authoritative)
		}
	}
}
*/

func TestGetReturnType(t *testing.T) {
	tests := []struct {
		input       sqlparser.Expr
		output      querypb.Type
		expectedErr error
	}{{
		input: &sqlparser.FuncExpr{Name: sqlparser.NewIdentifierCI("Abs"), Exprs: sqlparser.SelectExprs{
			&sqlparser.AliasedExpr{
				Expr: &sqlparser.ColName{
					Name: sqlparser.NewIdentifierCI("A"),
					Metadata: &column{
						typ: querypb.Type_DECIMAL,
					},
				},
			},
		}},
		output: querypb.Type_DECIMAL,
	}, {
		input:  &sqlparser.Count{},
		output: querypb.Type_INT64,
	}, {
		input:  &sqlparser.CountStar{},
		output: querypb.Type_INT64,
	}}

	for _, test := range tests {
		t.Run(sqlparser.String(test.input), func(t *testing.T) {
			got, err := GetReturnType(test.input)
			if test.expectedErr != nil {
				require.EqualError(t, err, test.expectedErr.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, got)
			}
		})
	}
}
