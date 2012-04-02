/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package schema

// Yes, this sucks. It's a tiny tiny package that needs to be on its own
// It contains a data structure that's shared between sqlparser & tabletserver

import (
	"strings"
	"time"
)

// Column categories
const (
	CAT_OTHER = iota
	CAT_NUMBER
	CAT_VARBINARY
)

type Table struct {
	Version        int64
	Name           string
	Columns        []string
	ColumnCategory []int
	Indexes        []*Index
	PKColumns      []int
	CacheType      int
}

func NewTable(name string) *Table {
	return &Table{
		Version:        time.Now().UnixNano(),
		Name:           name,
		Columns:        make([]string, 0, 16),
		ColumnCategory: make([]int, 0, 16),
		Indexes:        make([]*Index, 0, 8),
	}
}

func (self *Table) AddColumn(name string, column_type string) {
	self.Columns = append(self.Columns, name)
	if strings.Contains(column_type, "int") {
		self.ColumnCategory = append(self.ColumnCategory, CAT_NUMBER)
	} else if strings.HasPrefix(column_type, "varbinary") {
		self.ColumnCategory = append(self.ColumnCategory, CAT_VARBINARY)
	} else {
		self.ColumnCategory = append(self.ColumnCategory, CAT_OTHER)
	}
}

func (self *Table) FindColumn(name string) int {
	for i, colName := range self.Columns {
		if name == colName {
			return i
		}
	}
	return -1
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
