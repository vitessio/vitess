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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseColumnList(t *testing.T) {
	names := "id,category,max_len"

	columnList := ParseColumnList(names)
	assert.Equal(t, columnList.Len(), 3)
	assert.Equal(t, columnList.Names(), []string{"id", "category", "max_len"})
	assert.Equal(t, columnList.Ordinals["id"], 0)
	assert.Equal(t, columnList.Ordinals["category"], 1)
	assert.Equal(t, columnList.Ordinals["max_len"], 2)
}

func TestGetColumn(t *testing.T) {
	names := "id,category,max_len"
	columnList := ParseColumnList(names)
	{
		column := columnList.GetColumn("category")
		assert.NotNil(t, column)
		assert.Equal(t, column.Name, "category")
	}
	{
		column := columnList.GetColumn("no_such_column")
		assert.True(t, column == nil)
	}
}

func TestIsSubsetOf(t *testing.T) {
	tt := []struct {
		columns1     *ColumnList
		columns2     *ColumnList
		expectSubset bool
	}{
		{
			columns1:     ParseColumnList(""),
			columns2:     ParseColumnList("a,b,c"),
			expectSubset: true,
		},
		{
			columns1:     ParseColumnList("a,b,c"),
			columns2:     ParseColumnList("a,b,c"),
			expectSubset: true,
		},
		{
			columns1:     ParseColumnList("a,c"),
			columns2:     ParseColumnList("a,b,c"),
			expectSubset: true,
		},
		{
			columns1:     ParseColumnList("b,c"),
			columns2:     ParseColumnList("a,b,c"),
			expectSubset: true,
		},
		{
			columns1:     ParseColumnList("b"),
			columns2:     ParseColumnList("a,b,c"),
			expectSubset: true,
		},
		{
			columns1:     ParseColumnList(""),
			columns2:     ParseColumnList("a,b,c"),
			expectSubset: true,
		},
		{
			columns1:     ParseColumnList("a,d"),
			columns2:     ParseColumnList("a,b,c"),
			expectSubset: false,
		},
		{
			columns1:     ParseColumnList("a,b,c"),
			columns2:     ParseColumnList("a,b"),
			expectSubset: false,
		},
		{
			columns1:     ParseColumnList("a,b,c"),
			columns2:     ParseColumnList(""),
			expectSubset: false,
		},
	}
	for _, tc := range tt {
		name := fmt.Sprintf("%v:%v", tc.columns1.Names(), tc.columns2.Names())
		t.Run(name, func(t *testing.T) {
			isSubset := tc.columns1.IsSubsetOf(tc.columns2)
			assert.Equal(t, tc.expectSubset, isSubset)
		},
		)
	}
}

func TestDifference(t *testing.T) {
	tt := []struct {
		columns1 *ColumnList
		columns2 *ColumnList
		expect   *ColumnList
	}{
		{
			columns1: ParseColumnList(""),
			columns2: ParseColumnList("a,b,c"),
			expect:   ParseColumnList(""),
		},
		{
			columns1: ParseColumnList("a,b,c"),
			columns2: ParseColumnList("a,b,c"),
			expect:   ParseColumnList(""),
		},
		{
			columns1: ParseColumnList("a,c"),
			columns2: ParseColumnList("a,b,c"),
			expect:   ParseColumnList(""),
		},
		{
			columns1: ParseColumnList("b,c"),
			columns2: ParseColumnList("a,b,c"),
			expect:   ParseColumnList(""),
		},
		{
			columns1: ParseColumnList("b"),
			columns2: ParseColumnList("a,b,c"),
			expect:   ParseColumnList(""),
		},
		{
			columns1: ParseColumnList(""),
			columns2: ParseColumnList("a,b,c"),
			expect:   ParseColumnList(""),
		},
		{
			columns1: ParseColumnList("a,d"),
			columns2: ParseColumnList("a,b,c"),
			expect:   ParseColumnList("d"),
		},
		{
			columns1: ParseColumnList("a,b,c"),
			columns2: ParseColumnList("a,b"),
			expect:   ParseColumnList("c"),
		},
		{
			columns1: ParseColumnList("a,b,c"),
			columns2: ParseColumnList(""),
			expect:   ParseColumnList("a,b,c"),
		},
		{
			columns1: ParseColumnList("a,b,c"),
			columns2: ParseColumnList("b,d,e"),
			expect:   ParseColumnList("a,c"),
		},
	}
	for _, tc := range tt {
		name := fmt.Sprintf("%v:%v", tc.columns1.Names(), tc.columns2.Names())
		t.Run(name, func(t *testing.T) {
			diff := tc.columns1.Difference(tc.columns2)
			assert.Equal(t, tc.expect, diff)
		},
		)
	}
}

func TestMappedNamesColumnList(t *testing.T) {
	tt := []struct {
		columns  *ColumnList
		namesMap map[string]string
		expected *ColumnList
	}{
		{
			columns:  ParseColumnList("a,b,c"),
			namesMap: map[string]string{},
			expected: ParseColumnList("a,b,c"),
		},
		{
			columns:  ParseColumnList("a,b,c"),
			namesMap: map[string]string{"x": "y"},
			expected: ParseColumnList("a,b,c"),
		},
		{
			columns:  ParseColumnList("a,b,c"),
			namesMap: map[string]string{"a": "x", "c": "y"},
			expected: ParseColumnList("x,b,y"),
		},
	}
	for _, tc := range tt {
		name := fmt.Sprintf("%v:%v", tc.columns.Names(), tc.namesMap)
		t.Run(name, func(t *testing.T) {
			mappedNames := tc.columns.MappedNamesColumnList(tc.namesMap)
			assert.Equal(t, tc.expected, mappedNames)
		},
		)
	}
}
