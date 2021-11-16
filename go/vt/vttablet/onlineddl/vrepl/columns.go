/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package vrepl

import (
	"strings"
)

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

// GetNoDefaultColumnNames returns names of columns which have no default value, out of given list of columns
func GetNoDefaultColumnNames(columns *ColumnList) (names []string) {
	names = []string{}
	for _, col := range columns.Columns() {
		if !col.HasDefault() {
			names = append(names, col.Name)
		}
	}
	return names
}
