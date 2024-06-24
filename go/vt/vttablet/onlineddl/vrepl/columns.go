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
	"strings"
)

// expandedDataTypes maps some known and difficult-to-compute by INFORMATION_SCHEMA data types which expand other data types.
// For example, in "date:datetime", datetime expands date because it has more precision. In "timestamp:date" date expands timestamp
// because it can contain years not covered by timestamp.
var expandedDataTypes = map[string]bool{
	"time:datetime":      true,
	"date:datetime":      true,
	"timestamp:datetime": true,
	"time:timestamp":     true,
	"date:timestamp":     true,
	"timestamp:date":     true,
}

// GetSharedColumns returns the intersection of two lists of columns in same order as the first list
func GetSharedColumns(
	sourceColumns, targetColumns *ColumnList,
	sourceVirtualColumns, targetVirtualColumns *ColumnList,
	parser *AlterTableParser,
) (
	sourceSharedColumns *ColumnList,
	targetSharedColumns *ColumnList,
	droppedSourceNonGeneratedColumns *ColumnList,
	sharedColumnsMap map[string]string,
) {
	sharedColumnNames := []string{}
	droppedSourceNonGeneratedColumnsNames := []string{}
	for _, sourceColumn := range sourceColumns.Names() {
		isSharedColumn := false
		isVirtualColumnOnSource := false
		for _, targetColumn := range targetColumns.Names() {
			if strings.EqualFold(sourceColumn, targetColumn) {
				// both tables have this column. Good start.
				isSharedColumn = true
				break
			}
			if strings.EqualFold(parser.columnRenameMap[sourceColumn], targetColumn) {
				// column in source is renamed in target
				isSharedColumn = true
				break
			}
		}
		for droppedColumn := range parser.DroppedColumnsMap() {
			if strings.EqualFold(sourceColumn, droppedColumn) {
				isSharedColumn = false
				break
			}
		}
		for _, virtualColumn := range sourceVirtualColumns.Names() {
			// virtual/generated columns on source are silently skipped
			if strings.EqualFold(sourceColumn, virtualColumn) {
				isSharedColumn = false
				isVirtualColumnOnSource = true
			}
		}
		for _, virtualColumn := range targetVirtualColumns.Names() {
			// virtual/generated columns on target are silently skipped
			if strings.EqualFold(sourceColumn, virtualColumn) {
				isSharedColumn = false
			}
		}
		if isSharedColumn {
			sharedColumnNames = append(sharedColumnNames, sourceColumn)
		} else if !isVirtualColumnOnSource {
			droppedSourceNonGeneratedColumnsNames = append(droppedSourceNonGeneratedColumnsNames, sourceColumn)
		}
	}
	sharedColumnsMap = map[string]string{}
	for _, columnName := range sharedColumnNames {
		if mapped, ok := parser.columnRenameMap[columnName]; ok {
			sharedColumnsMap[columnName] = mapped
		} else {
			sharedColumnsMap[columnName] = columnName
		}
	}
	mappedSharedColumnNames := []string{}
	for _, columnName := range sharedColumnNames {
		mappedSharedColumnNames = append(mappedSharedColumnNames, sharedColumnsMap[columnName])
	}
	return NewColumnList(sharedColumnNames), NewColumnList(mappedSharedColumnNames), NewColumnList(droppedSourceNonGeneratedColumnsNames), sharedColumnsMap
}

// GetExpandedColumnNames is given source and target shared columns, and returns the list of columns whose data type is expanded.
// An expanded data type is one where the target can have a value which the source does not. Examples:
// - any NOT NULL to NULLable (a NULL in the target cannot appear on source)
// - INT -> BIGINT (obvious)
// - BIGINT UNSIGNED -> INT SIGNED (negative values)
// - TIMESTAMP -> TIMESTAMP(3)
// etc.
func GetExpandedColumnNames(
	sourceSharedColumns *ColumnList,
	targetSharedColumns *ColumnList,
) (
	expandedColumnNames []string,
	expandedDescriptions map[string]string,
) {
	expandedDescriptions = map[string]string{}
	for i := range sourceSharedColumns.Columns() {
		// source and target columns assumed to be mapped 1:1, same length
		sourceColumn := sourceSharedColumns.Columns()[i]
		targetColumn := targetSharedColumns.Columns()[i]

		if isExpanded, description := targetColumn.Entity.Expands(sourceColumn.Entity); isExpanded {
			expandedColumnNames = append(expandedColumnNames, sourceColumn.Name)
			expandedDescriptions[sourceColumn.Name] = description
		}
	}
	return expandedColumnNames, expandedDescriptions
}

// GetNoDefaultColumnNames returns names of columns which have no default value, out of given list of columns
func GetNoDefaultColumnNames(columns *ColumnList) (names []string) {
	names = []string{}
	for _, col := range columns.Columns() {
		if !col.Entity.HasDefault() {
			names = append(names, col.Name)
		}
	}
	return names
}
