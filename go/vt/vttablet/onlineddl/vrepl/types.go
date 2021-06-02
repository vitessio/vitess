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
	"reflect"
	"strings"
)

// ColumnType indicated some MySQL data types
type ColumnType int

const (
	UnknownColumnType ColumnType = iota
	TimestampColumnType
	DateTimeColumnType
	EnumColumnType
	MediumIntColumnType
	JSONColumnType
	FloatColumnType
	BinaryColumnType
)

// Column represents a table column
type Column struct {
	Name                 string
	IsUnsigned           bool
	Charset              string
	Type                 ColumnType
	EnumValues           string
	EnumToTextConversion bool

	// add Octet length for binary type, fix bytes with suffix "00" get clipped in mysql binlog.
	// https://github.com/github/gh-ost/issues/909
	BinaryOctetLength uint64
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

// NewEmptyColumnsMap creates an empty map
func NewEmptyColumnsMap() ColumnsMap {
	columnsMap := make(map[string]int)
	return ColumnsMap(columnsMap)
}

// NewColumnsMap creates a column map based on ordered list of columns
func NewColumnsMap(orderedColumns []Column) ColumnsMap {
	columnsMap := NewEmptyColumnsMap()
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

// ParseColumnList parses a comma delimited list of column names
func ParseColumnList(names string) *ColumnList {
	result := &ColumnList{
		columns: ParseColumns(names),
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

// Equals checks for complete (deep) identities of columns, in order.
func (l *ColumnList) Equals(other *ColumnList) bool {
	return reflect.DeepEqual(l.Columns, other.Columns)
}

// EqualsByNames chcks if the names in this list equals the names of another list, in order. Type is ignored.
func (l *ColumnList) EqualsByNames(other *ColumnList) bool {
	return reflect.DeepEqual(l.Names(), other.Names())
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

// UniqueKey is the combination of a key's name and columns
type UniqueKey struct {
	Name            string
	Columns         ColumnList
	HasNullable     bool
	IsAutoIncrement bool
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
	description := k.Name
	if k.IsAutoIncrement {
		description = fmt.Sprintf("%s (auto_increment)", description)
	}
	return fmt.Sprintf("%s: %s; has nullable: %+v", description, k.Columns.Names(), k.HasNullable)
}
