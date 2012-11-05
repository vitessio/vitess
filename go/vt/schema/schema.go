// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schema

// Yes, this sucks. It's a tiny tiny package that needs to be on its own
// It contains a data structure that's shared between sqlparser & tabletserver

import (
	"strings"

	"code.google.com/p/vitess/go/sqltypes"
)

// Column categories
const (
	CAT_OTHER = iota
	CAT_NUMBER
	CAT_VARBINARY
)

type TableColumn struct {
	Name     string
	Category int
	IsAuto   bool
	Default  sqltypes.Value
}

type Table struct {
	Name      string
	Columns   []TableColumn
	Indexes   []*Index
	PKColumns []int
	CacheType int
}

func NewTable(name string) *Table {
	return &Table{
		Name:    name,
		Columns: make([]TableColumn, 0, 16),
		Indexes: make([]*Index, 0, 8),
	}
}

func (self *Table) AddColumn(name string, columnType string, defval sqltypes.Value, extra string) {
	index := len(self.Columns)
	self.Columns = append(self.Columns, TableColumn{Name: name})
	if strings.Contains(columnType, "int") {
		self.Columns[index].Category = CAT_NUMBER
	} else if strings.HasPrefix(columnType, "varbinary") {
		self.Columns[index].Category = CAT_VARBINARY
	} else {
		self.Columns[index].Category = CAT_OTHER
	}
	if extra == "auto_increment" {
		self.Columns[index].IsAuto = true
		// Ignore default value, if any
		return
	}
	if defval.IsNull() {
		return
	}
	if self.Columns[index].Category == CAT_NUMBER {
		self.Columns[index].Default = sqltypes.MakeNumeric(defval.Raw())
	} else {
		self.Columns[index].Default = sqltypes.MakeString(defval.Raw())
	}
}

func (self *Table) FindColumn(name string) int {
	for i, col := range self.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

func (self *Table) GetPKColumn(index int) *TableColumn {
	return &self.Columns[self.PKColumns[index]]
}

func (self *Table) AddIndex(name string) (index *Index) {
	index = NewIndex(name)
	self.Indexes = append(self.Indexes, index)
	return index
}

type Index struct {
	Name        string
	Columns     []string
	Cardinality []uint64
	DataColumns []string
}

func NewIndex(name string) *Index {
	return &Index{name, make([]string, 0, 8), make([]uint64, 0, 8), nil}
}

func (self *Index) AddColumn(name string, cardinality uint64) {
	self.Columns = append(self.Columns, name)
	if cardinality == 0 {
		cardinality = uint64(len(self.Cardinality) + 1)
	}
	self.Cardinality = append(self.Cardinality, cardinality)
}

func (self *Index) FindColumn(name string) int {
	for i, colName := range self.Columns {
		if name == colName {
			return i
		}
	}
	return -1
}

func (self *Index) FindDataColumn(name string) int {
	for i, colName := range self.DataColumns {
		if name == colName {
			return i
		}
	}
	return -1
}
