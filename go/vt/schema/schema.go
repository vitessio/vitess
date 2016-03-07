// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schema

// Yes, this sucks. It's a tiny tiny package that needs to be on its own
// It contains a data structure that's shared between sqlparser & tabletserver

import (
	"strings"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// Cache types
const (
	CacheNone = 0
	CacheRW   = 1
	CacheW    = 2
	Sequence  = 3
)

// TableColumn contains info about a table's column.
type TableColumn struct {
	Name    string
	Type    querypb.Type
	IsAuto  bool
	Default sqltypes.Value
}

// Table contains info about a table.
type Table struct {
	Name      string
	Columns   []TableColumn
	Indexes   []*Index
	PKColumns []int
	Type      int

	// These vars can be accessed concurrently.
	TableRows   sync2.AtomicInt64
	DataLength  sync2.AtomicInt64
	IndexLength sync2.AtomicInt64
	DataFree    sync2.AtomicInt64
}

// NewTable creates a new Table.
func NewTable(name string) *Table {
	return &Table{
		Name: name,
	}
}

// IsCached returns true if the table has a rowcache association.
func (ta *Table) IsCached() bool {
	return ta.Type == CacheRW || ta.Type == CacheW
}

// IsReadCached returns true if the rowcache can be used for reads.
// TODO(sougou): remove after deprecating schema overrides.
func (ta *Table) IsReadCached() bool {
	return ta.Type == CacheRW
}

// AddColumn adds a column to the Table.
func (ta *Table) AddColumn(name string, columnType querypb.Type, defval sqltypes.Value, extra string) {
	index := len(ta.Columns)
	ta.Columns = append(ta.Columns, TableColumn{Name: strings.ToLower(name)})
	ta.Columns[index].Type = columnType
	if extra == "auto_increment" {
		ta.Columns[index].IsAuto = true
		// Ignore default value, if any
		return
	}
	if defval.IsNull() {
		return
	}
	// Schema values are trusted.
	ta.Columns[index].Default = sqltypes.MakeTrusted(ta.Columns[index].Type, defval.Raw())
}

// FindColumn finds a column in the table. It returns the index if found.
// Otherwise, it returns -1.
func (ta *Table) FindColumn(name string) int {
	for i, col := range ta.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

// GetPKColumn returns the pk column specified by the index.
func (ta *Table) GetPKColumn(index int) *TableColumn {
	return &ta.Columns[ta.PKColumns[index]]
}

// AddIndex adds an index to the table.
func (ta *Table) AddIndex(name string) (index *Index) {
	index = NewIndex(name)
	ta.Indexes = append(ta.Indexes, index)
	return index
}

// SetMysqlStats receives the values found in the mysql information_schema.tables table
func (ta *Table) SetMysqlStats(tr, dl, il, df sqltypes.Value) {
	v, _ := tr.ParseInt64()
	ta.TableRows.Set(v)
	v, _ = dl.ParseInt64()
	ta.DataLength.Set(v)
	v, _ = il.ParseInt64()
	ta.IndexLength.Set(v)
	v, _ = df.ParseInt64()
	ta.DataFree.Set(v)
}

// Index contains info about a table index.
type Index struct {
	Name        string
	Columns     []string
	Cardinality []uint64
	DataColumns []string
}

// NewIndex creates a new Index.
func NewIndex(name string) *Index {
	return &Index{name, make([]string, 0, 8), make([]uint64, 0, 8), nil}
}

// AddColumn adds a column to the index.
func (idx *Index) AddColumn(name string, cardinality uint64) {
	idx.Columns = append(idx.Columns, strings.ToLower(name))
	if cardinality == 0 {
		cardinality = uint64(len(idx.Cardinality) + 1)
	}
	idx.Cardinality = append(idx.Cardinality, cardinality)
}

// FindColumn finds a column in the index. It returns the index if found.
// Otherwise, it returns -1.
func (idx *Index) FindColumn(name string) int {
	for i, colName := range idx.Columns {
		if name == colName {
			return i
		}
	}
	return -1
}

// FindDataColumn finds a data column in the index. It returns the index if found.
// Otherwise, it returns -1.
func (idx *Index) FindDataColumn(name string) int {
	for i, colName := range idx.DataColumns {
		if name == colName {
			return i
		}
	}
	return -1
}
