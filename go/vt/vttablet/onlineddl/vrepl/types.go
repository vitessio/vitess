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
	"strconv"
	"strings"
)

// ColumnType enumerates some important column types
type ColumnType int

const (
	UnknownColumnType ColumnType = iota
	TimestampColumnType
	DateTimeColumnType
	EnumColumnType
	MediumIntColumnType
	JSONColumnType
	FloatColumnType
)

const maxMediumintUnsigned int32 = 16777215

// TimezoneConversion indicates how to convert a timezone value
type TimezoneConversion struct {
	ToTimezone string
}

// Column represents a table column
type Column struct {
	Name               string
	IsUnsigned         bool
	Charset            string
	Type               ColumnType
	timezoneConversion *TimezoneConversion
}

func (c *Column) convertArg(arg interface{}) interface{} {
	if s, ok := arg.(string); ok {
		// string, charset conversion
		if encoding, ok := charsetEncodingMap[c.Charset]; ok {
			arg, _ = encoding.NewDecoder().String(s)
		}
		return arg
	}

	if c.IsUnsigned {
		if i, ok := arg.(int8); ok {
			return uint8(i)
		}
		if i, ok := arg.(int16); ok {
			return uint16(i)
		}
		if i, ok := arg.(int32); ok {
			if c.Type == MediumIntColumnType {
				// problem with mediumint is that it's a 3-byte type. There is no compatible golang type to match that.
				// So to convert from negative to positive we'd need to convert the value manually
				if i >= 0 {
					return i
				}
				return uint32(maxMediumintUnsigned + i + 1)
			}
			return uint32(i)
		}
		if i, ok := arg.(int64); ok {
			return strconv.FormatUint(uint64(i), 10)
		}
		if i, ok := arg.(int); ok {
			return uint(i)
		}
	}
	return arg
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

// SetUnsigned toggles on the unsigned property
func (l *ColumnList) SetUnsigned(columnName string) {
	l.GetColumn(columnName).IsUnsigned = true
}

// IsUnsigned returns true when the column is an unsigned numeral
func (l *ColumnList) IsUnsigned(columnName string) bool {
	return l.GetColumn(columnName).IsUnsigned
}

// SetCharset sets the charset property
func (l *ColumnList) SetCharset(columnName string, charset string) {
	l.GetColumn(columnName).Charset = charset
}

// GetCharset returns the hcarset property
func (l *ColumnList) GetCharset(columnName string) string {
	return l.GetColumn(columnName).Charset
}

// SetColumnType sets the type of the column (for interesting types)
func (l *ColumnList) SetColumnType(columnName string, columnType ColumnType) {
	l.GetColumn(columnName).Type = columnType
}

// GetColumnType gets type of column, for interesting types
func (l *ColumnList) GetColumnType(columnName string) ColumnType {
	return l.GetColumn(columnName).Type
}

// SetConvertDatetimeToTimestamp sets the timezone conversion
func (l *ColumnList) SetConvertDatetimeToTimestamp(columnName string, toTimezone string) {
	l.GetColumn(columnName).timezoneConversion = &TimezoneConversion{ToTimezone: toTimezone}
}

// HasTimezoneConversion sees if there's timezone conversion defined (only applicable to temporal values)
func (l *ColumnList) HasTimezoneConversion(columnName string) bool {
	return l.GetColumn(columnName).timezoneConversion != nil
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
