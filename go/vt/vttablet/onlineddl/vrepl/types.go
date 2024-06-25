/*
	Original copyright by GitHub as follows. Additions by the Vitess authors as follows.
*/
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
	"strings"

	"vitess.io/vitess/go/vt/schemadiff"
)

// Column represents a table column
type Column struct {
	Name   string
	Entity *schemadiff.ColumnDefinitionEntity
}

// NewColumns creates a new column array from non empty names
func NewColumns(names []string) []Column {
	result := []Column{}
	for _, name := range names {
		if name == "" {
			continue
		}
		result = append(result, Column{Name: name})
	}
	return result
}

// ParseColumns creates a new column array fby parsing comma delimited names list
func ParseColumns(names string) []Column {
	namesArray := strings.Split(names, ",")
	return NewColumns(namesArray)
}

// ColumnsMap maps a column name onto its ordinal position
type ColumnsMap map[string]int

// NewColumnsMap creates a column map based on ordered list of columns
func NewColumnsMap(orderedColumns []Column) ColumnsMap {
	columnsMap := make(ColumnsMap, len(orderedColumns))
	for i, column := range orderedColumns {
		columnsMap[column.Name] = i
	}
	return columnsMap
}

// ColumnList makes for a named list of columns
type ColumnList struct {
	columns  []Column
	Ordinals ColumnsMap
}

// NewColumnList creates an object given ordered list of column names
func NewColumnList(names []string) *ColumnList {
	result := &ColumnList{
		columns: NewColumns(names),
	}
	result.Ordinals = NewColumnsMap(result.columns)
	return result
}

func ParseColumnList(names string) *ColumnList {
	result := &ColumnList{
		columns: ParseColumns(names),
	}
	result.Ordinals = NewColumnsMap(result.columns)
	return result
}

// Columns returns the list of columns
func (l *ColumnList) Union(other *ColumnList) *ColumnList {
	result := &ColumnList{
		columns: append(l.columns, other.columns...),
	}
	result.Ordinals = NewColumnsMap(result.columns)
	return result
}

// Columns returns the list of columns
func (l *ColumnList) Columns() []Column {
	return l.columns
}

// Names returns list of column names
func (l *ColumnList) Names() []string {
	names := make([]string, len(l.columns))
	for i := range l.columns {
		names[i] = l.columns[i].Name
	}
	return names
}

// GetColumn gets a column by name
func (l *ColumnList) GetColumn(columnName string) *Column {
	if ordinal, ok := l.Ordinals[columnName]; ok {
		return &l.columns[ordinal]
	}
	return nil
}

// String returns a comma separated list of column names
func (l *ColumnList) String() string {
	return strings.Join(l.Names(), ",")
}

// IsSubsetOf returns 'true' when column names of this list are a subset of
// another list, in arbitrary order (order agnostic)
func (l *ColumnList) IsSubsetOf(other *ColumnList) bool {
	for _, column := range l.columns {
		if _, exists := other.Ordinals[column.Name]; !exists {
			return false
		}
	}
	return true
}

// Len returns the length of this list
func (l *ColumnList) Len() int {
	return len(l.columns)
}

// MappedNamesColumnList returns a column list based on this list, with names possibly mapped by given map
func (l *ColumnList) MappedNamesColumnList(columnNamesMap map[string]string) *ColumnList {
	names := l.Names()
	for i := range names {
		if mappedName, ok := columnNamesMap[names[i]]; ok {
			names[i] = mappedName
		}
	}
	return NewColumnList(names)
}

// UniqueKey is the combination of a key's name and columns
type UniqueKey struct {
	Name    string
	Columns ColumnList

	HasNullable bool
}

// IsPrimary checks if this unique key is primary
func (k *UniqueKey) IsPrimary() bool {
	return k.Name == "PRIMARY"
}

// Len returns the length of this list
func (k *UniqueKey) Len() int {
	return k.Columns.Len()
}

// String returns a visual representation of this key
func (k *UniqueKey) String() string {
	return fmt.Sprintf("%s: %s", k.Name, k.Columns.Names())
}
