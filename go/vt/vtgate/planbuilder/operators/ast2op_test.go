/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// Test_fkNeedsHandlingForUpdates tests the functionality of the function fkNeedsHandlingForUpdates.
// It verifies the different cases in which foreign key handling is required on vtgate level.
func Test_fkNeedsHandlingForUpdates(t *testing.T) {
	t1 := &vindexes.Table{
		Name: sqlparser.NewIdentifierCS("t1"),
	}

	tests := []struct {
		name            string
		updateExprs     sqlparser.UpdateExprs
		parentFks       []vindexes.ParentFKInfo
		childFks        []vindexes.ChildFKInfo
		parentFKsWanted []bool
		childFKsWanted  []bool
	}{
		{
			name: "No Fks filtered",
			updateExprs: sqlparser.UpdateExprs{
				&sqlparser.UpdateExpr{
					Name: sqlparser.NewColName("a"),
					Expr: sqlparser.NewIntLiteral("1"),
				},
			},
			childFks: []vindexes.ChildFKInfo{
				{
					Table:         t1,
					ParentColumns: sqlparser.MakeColumns("a", "b", "c"),
				},
			},
			parentFks: []vindexes.ParentFKInfo{
				{
					Table:        t1,
					ChildColumns: sqlparser.MakeColumns("a", "b", "c"),
				},
			},
			parentFKsWanted: []bool{true},
			childFKsWanted:  []bool{true},
		}, {
			name: "Child Fks filtering",
			updateExprs: sqlparser.UpdateExprs{
				&sqlparser.UpdateExpr{
					Name: sqlparser.NewColName("a"),
					Expr: sqlparser.NewIntLiteral("1"),
				},
			},
			childFks: []vindexes.ChildFKInfo{
				{
					Table:         t1,
					ParentColumns: sqlparser.MakeColumns("b", "a", "c"),
				}, {
					Table:         t1,
					ParentColumns: sqlparser.MakeColumns("d", "c"),
				},
			},
			parentFks: []vindexes.ParentFKInfo{
				{
					Table:        t1,
					ChildColumns: sqlparser.MakeColumns("a", "b", "c"),
				},
			},
			parentFKsWanted: []bool{true},
			childFKsWanted:  []bool{true, false},
		}, {
			name: "Parent Fks filtered based on columns",
			updateExprs: sqlparser.UpdateExprs{
				&sqlparser.UpdateExpr{
					Name: sqlparser.NewColName("a"),
					Expr: sqlparser.NewIntLiteral("1"),
				},
			},
			childFks: []vindexes.ChildFKInfo{
				{
					Table:         t1,
					ParentColumns: sqlparser.MakeColumns("a", "b", "c"),
				},
			},
			parentFks: []vindexes.ParentFKInfo{
				{
					Table:        t1,
					ChildColumns: sqlparser.MakeColumns("b", "a", "c"),
				}, {
					Table:        t1,
					ChildColumns: sqlparser.MakeColumns("d", "b"),
				},
			},
			parentFKsWanted: []bool{true, false},
			childFKsWanted:  []bool{true},
		}, {
			name: "Parent Fks filtered because all null values",
			updateExprs: sqlparser.UpdateExprs{
				&sqlparser.UpdateExpr{
					Name: sqlparser.NewColName("a"),
					Expr: &sqlparser.NullVal{},
				},
			},
			childFks: []vindexes.ChildFKInfo{
				{
					Table:         t1,
					ParentColumns: sqlparser.MakeColumns("a", "b", "c"),
				},
			},
			parentFks: []vindexes.ParentFKInfo{
				{
					Table:        t1,
					ChildColumns: sqlparser.MakeColumns("b", "a", "c"),
				}, {
					Table:        t1,
					ChildColumns: sqlparser.MakeColumns("a", "b"),
				},
			},
			parentFKsWanted: []bool{false, false},
			childFKsWanted:  []bool{true},
		}, {
			name: "Parent Fks filtered because some column has null values",
			updateExprs: sqlparser.UpdateExprs{
				&sqlparser.UpdateExpr{
					Name: sqlparser.NewColName("a"),
					Expr: sqlparser.NewIntLiteral("1"),
				}, &sqlparser.UpdateExpr{
					Name: sqlparser.NewColName("c"),
					Expr: &sqlparser.NullVal{},
				},
			},
			childFks: []vindexes.ChildFKInfo{
				{
					Table:         t1,
					ParentColumns: sqlparser.MakeColumns("a", "b", "c"),
				},
			},
			parentFks: []vindexes.ParentFKInfo{
				{
					Table:        t1,
					ChildColumns: sqlparser.MakeColumns("b", "a", "c"),
				}, {
					Table:        t1,
					ChildColumns: sqlparser.MakeColumns("a", "b"),
				},
			},
			parentFKsWanted: []bool{false, true},
			childFKsWanted:  []bool{true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parentFksGot, childFksGot := fkNeedsHandlingForUpdates(tt.updateExprs, tt.parentFks, tt.childFks)
			var pFks []vindexes.ParentFKInfo
			for idx, b := range tt.parentFKsWanted {
				if b {
					pFks = append(pFks, tt.parentFks[idx])
				}
			}
			var cFks []vindexes.ChildFKInfo
			for idx, b := range tt.childFKsWanted {
				if b {
					cFks = append(cFks, tt.childFks[idx])
				}
			}
			require.EqualValues(t, pFks, parentFksGot)
			require.EqualValues(t, cFks, childFksGot)
		})
	}
}
