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

	"vitess.io/vitess/go/vt/schemadiff"
)

// ColumnType indicated some MySQL data types
type ColumnType int

const (
	UnknownColumnType ColumnType = iota
	TimestampColumnType
	DateTimeColumnType
	EnumColumnType
	SetColumnType
	MediumIntColumnType
	JSONColumnType
	FloatColumnType
	DoubleColumnType
	BinaryColumnType
	StringColumnType
	IntegerColumnType
)

// Column represents a table column
type Column struct {
	Name                 string
	IsUnsigned           bool
	Charset              string
	Collation            string
	Type                 ColumnType
	EnumValues           string
	EnumToTextConversion bool
	DataType             string // from COLUMN_TYPE column

	IsNullable    bool
	IsDefaultNull bool

	CharacterMaximumLength int64
	NumericPrecision       int64
	NumericScale           int64
	DateTimePrecision      int64

	// add Octet length for binary type, fix bytes with suffix "00" get clipped in mysql binlog.
	// https://github.com/github/gh-ost/issues/909
	BinaryOctetLength uint64
}

// SetTypeIfUnknown will set a new column type only if the current type is unknown, otherwise silently skip
func (c *Column) SetTypeIfUnknown(t ColumnType) {
	if c.Type == UnknownColumnType {
		c.Type = t
	}
}

// HasDefault returns true if the column at all has a default value (possibly NULL)
func (c *Column) HasDefault() bool {
	if c.IsDefaultNull && !c.IsNullable {
		// based on INFORMATION_SCHEMA.COLUMNS, this is the indicator for a 'NOT NULL' column with no default value.
		return false
	}
	return true
}

// IsNumeric returns true if the column is of a numeric type
func (c *Column) IsNumeric() bool {
	return c.NumericPrecision > 0
}

// IsIntegralType returns true if the column is some form of an integer
func (c *Column) IsIntegralType() bool {
	return schemadiff.IsIntegralType(c.DataType)
}

// IsFloatingPoint returns true if the column is of a floating point numeric type
func (c *Column) IsFloatingPoint() bool {
	return c.Type == FloatColumnType || c.Type == DoubleColumnType
}

// IsFloatingPoint returns true if the column is of a temporal type
func (c *Column) IsTemporal() bool {
	return c.DateTimePrecision >= 0
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

// ColumnExists returns true if this column list has a column by a given name
func (l *ColumnList) ColumnExists(columnName string) bool {
	_, ok := l.Ordinals[columnName]
	return ok
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

// Difference returns a (new copy) subset of this column list, consisting of all
// column NOT in given list.
// The result is never nil, even if the difference is empty
func (l *ColumnList) Difference(other *ColumnList) (diff *ColumnList) {
	names := []string{}
	for _, column := range l.columns {
		if !other.ColumnExists(column.Name) {
			names = append(names, column.Name)
		}
	}
	return NewColumnList(names)
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

// SetEnumToTextConversion tells this column list that an enum is conveted to text
func (l *ColumnList) SetEnumToTextConversion(columnName string, enumValues string) {
	l.GetColumn(columnName).EnumToTextConversion = true
	l.GetColumn(columnName).EnumValues = enumValues
}

// IsEnumToTextConversion tells whether an enum was converted to text
func (l *ColumnList) IsEnumToTextConversion(columnName string) bool {
	return l.GetColumn(columnName).EnumToTextConversion
}

// UniqueKey is the combination of a key's name and columns
type UniqueKey struct {
	Name            string
	Columns         ColumnList
	HasNullable     bool
	HasSubpart      bool
	HasFloat        bool
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
