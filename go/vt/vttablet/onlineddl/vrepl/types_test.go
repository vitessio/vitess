/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
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
