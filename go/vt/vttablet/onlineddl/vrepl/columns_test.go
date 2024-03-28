/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/
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

package vrepl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	columnsA = &ColumnList{
		columns: []Column{
			{
				Name: "id",
			},
			{
				Name: "cint",
			},
			{
				Name: "cgen1",
			},
			{
				Name: "cgen2",
			},
			{
				Name: "cchar",
			},
			{
				Name: "cremoved",
			},
			{
				Name:       "cnullable",
				IsNullable: true,
			},
			{
				Name:          "cnodefault",
				IsNullable:    false,
				IsDefaultNull: true,
			},
		},
		Ordinals: ColumnsMap{},
	}
	columnsB = &ColumnList{
		columns: []Column{
			{
				Name: "id",
			},
			{
				Name: "cint",
			},
			{
				Name: "cgen1",
			},
			{
				Name: "cchar_alternate",
			},
			{
				Name:       "cnullable",
				IsNullable: true,
			},
			{
				Name:          "cnodefault",
				IsNullable:    false,
				IsDefaultNull: true,
			},
		},
		Ordinals: ColumnsMap{},
	}
	columnsVirtual = ParseColumnList("cgen1,cgen2")
)

func TestGetSharedColumns(t *testing.T) {
	tt := []struct {
		name                                    string
		sourceCols                              *ColumnList
		targetCols                              *ColumnList
		renameMap                               map[string]string
		expectSourceSharedColNames              []string
		expectTargetSharedColNames              []string
		expectDroppedSourceNonGeneratedColNames []string
	}{
		{
			name:                                    "rename map empty",
			sourceCols:                              columnsA,
			targetCols:                              columnsB,
			renameMap:                               map[string]string{},
			expectSourceSharedColNames:              []string{"id", "cint", "cnullable", "cnodefault"},
			expectTargetSharedColNames:              []string{"id", "cint", "cnullable", "cnodefault"},
			expectDroppedSourceNonGeneratedColNames: []string{"cchar", "cremoved"},
		},
		{
			name:                                    "renamed column",
			sourceCols:                              columnsA,
			targetCols:                              columnsB,
			renameMap:                               map[string]string{"cchar": "cchar_alternate"},
			expectSourceSharedColNames:              []string{"id", "cint", "cchar", "cnullable", "cnodefault"},
			expectTargetSharedColNames:              []string{"id", "cint", "cchar_alternate", "cnullable", "cnodefault"},
			expectDroppedSourceNonGeneratedColNames: []string{"cremoved"},
		},
	}

	parser := NewAlterTableParser()
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			parser.columnRenameMap = tc.renameMap
			sourceSharedCols, targetSharedCols, droppedNonGeneratedCols, _ := GetSharedColumns(
				tc.sourceCols, tc.targetCols,
				columnsVirtual, columnsVirtual,
				parser,
			)
			assert.Equal(t, tc.expectSourceSharedColNames, sourceSharedCols.Names())
			assert.Equal(t, tc.expectTargetSharedColNames, targetSharedCols.Names())
			assert.Equal(t, tc.expectDroppedSourceNonGeneratedColNames, droppedNonGeneratedCols.Names())
		})
	}
}

func TestGetExpandedColumnNames(t *testing.T) {
	var (
		columnsA = &ColumnList{
			columns: []Column{
				{
					Name:       "c1",
					IsNullable: true,
				},
				{
					Name:       "c2",
					IsNullable: true,
				},
				{
					Name:       "c3",
					IsNullable: false,
				},
			},
			Ordinals: ColumnsMap{},
		}
		columnsB = &ColumnList{
			columns: []Column{
				{
					Name:       "c1",
					IsNullable: true,
				},
				{
					Name:       "c2",
					IsNullable: false,
				},
				{
					Name:       "c3",
					IsNullable: true,
				},
			},
			Ordinals: ColumnsMap{},
		}
	)
	tcases := []struct {
		name      string
		sourceCol Column
		targetCol Column
		expanded  bool
	}{
		{
			"both nullable",
			Column{
				IsNullable: true,
			},
			Column{
				IsNullable: true,
			},
			false,
		},
		{
			"nullable to non nullable",
			Column{
				IsNullable: true,
			},
			Column{
				IsNullable: false,
			},
			false,
		},
		{
			"non nullable to nullable",
			Column{
				IsNullable: false,
			},
			Column{
				IsNullable: true,
			},
			true,
		},
		{
			"signed to unsigned",
			Column{
				Type:             IntegerColumnType,
				NumericPrecision: 4,
				IsUnsigned:       false,
			},
			Column{
				Type:             IntegerColumnType,
				NumericPrecision: 4,
				IsUnsigned:       true,
			},
			true,
		},
		{
			"unsigned to signed",
			Column{
				Type:             IntegerColumnType,
				NumericPrecision: 4,
				IsUnsigned:       true,
			},
			Column{
				Type:             IntegerColumnType,
				NumericPrecision: 4,
				IsUnsigned:       false,
			},
			true,
		},
		{
			"signed to smaller unsigned",
			Column{
				Type:             IntegerColumnType,
				NumericPrecision: 8,
				IsUnsigned:       false,
			},
			Column{
				Type:             IntegerColumnType,
				NumericPrecision: 4,
				IsUnsigned:       true,
			},
			false,
		},
		{
			"same char length",
			Column{
				CharacterMaximumLength: 20,
			},
			Column{
				CharacterMaximumLength: 20,
			},
			false,
		},
		{
			"reduced char length",
			Column{
				CharacterMaximumLength: 20,
			},
			Column{
				CharacterMaximumLength: 19,
			},
			false,
		},
		{
			"increased char length",
			Column{
				CharacterMaximumLength: 20,
			},
			Column{
				CharacterMaximumLength: 21,
			},
			true,
		},
		{
			"expand temporal",
			Column{
				DataType: "time",
			},
			Column{
				DataType: "timestamp",
			},
			true,
		},
		{
			"expand temporal",
			Column{
				DataType: "date",
			},
			Column{
				DataType: "timestamp",
			},
			true,
		},
		{
			"expand temporal",
			Column{
				DataType: "date",
			},
			Column{
				DataType: "datetime",
			},
			true,
		},
		{
			"non expand temporal",
			Column{
				DataType: "datetime",
			},
			Column{
				DataType: "timestamp",
			},
			false,
		},
		{
			"expand temporal",
			Column{
				DataType: "timestamp",
			},
			Column{
				DataType: "datetime",
			},
			true,
		},
		{
			"expand enum",
			Column{
				Type:       EnumColumnType,
				EnumValues: "'a','b'",
			},
			Column{
				Type:       EnumColumnType,
				EnumValues: "'a','x'",
			},
			true,
		},
		{
			"expand enum",
			Column{
				Type:       EnumColumnType,
				EnumValues: "'a','b'",
			},
			Column{
				Type:       EnumColumnType,
				EnumValues: "'a','b','c'",
			},
			true,
		},
		{
			"reduce enum",
			Column{
				Type:       EnumColumnType,
				EnumValues: "'a','b','c'",
			},
			Column{
				Type:       EnumColumnType,
				EnumValues: "'a','b'",
			},
			false,
		},
	}

	expectedExpandedColumnNames := []string{"c3"}
	expandedColumnNames, _ := GetExpandedColumnNames(columnsA, columnsB)
	assert.Equal(t, expectedExpandedColumnNames, expandedColumnNames)

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			expanded, _ := isExpandedColumn(&tcase.sourceCol, &tcase.targetCol)
			assert.Equal(t, tcase.expanded, expanded)
		})
	}
}
