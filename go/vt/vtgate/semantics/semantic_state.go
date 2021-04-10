/*
Copyright 2020 The Vitess Authors.

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

package semantics

import (
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	// TableInfo contains the alias table expr and vindex table
	TableInfo struct {
		ASTNode *sqlparser.AliasedTableExpr
		Table   *vindexes.Table
	}

	// TableSet is how a set of tables is expressed.
	// Tables get unique bits assigned in the order that they are encountered during semantic analysis
	TableSet uint64 // we can only join 64 tables with this underlying data type
	// TODO : change uint64 to struct to support arbitrary number of tables.

	// SemTable contains semantic analysis information about the query.
	SemTable struct {
		Tables           []*TableInfo
		exprDependencies map[sqlparser.Expr]TableSet
	}

	scope struct {
		parent *scope
		tables map[string]*TableInfo
	}

	// SchemaInformation is used tp provide table information from Vschema.
	SchemaInformation interface {
		FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error)
	}
)

// NewSemTable creates a new empty SemTable
func NewSemTable() *SemTable {
	return &SemTable{exprDependencies: map[sqlparser.Expr]TableSet{}}
}

// TableSetFor returns the bitmask for this particular tableshoe
func (st *SemTable) TableSetFor(t *sqlparser.AliasedTableExpr) TableSet {
	for idx, t2 := range st.Tables {
		if t == t2.ASTNode {
			return 1 << idx
		}
	}
	return 0
}

// TableInfoFor returns the table info for the table set. It should contains only single table.
func (st *SemTable) TableInfoFor(id TableSet) (*TableInfo, error) {
	if id.NumberOfTables() > 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] should only be used for single tables")
	}
	return st.Tables[id.TableOffset()], nil
}

// Dependencies return the table dependencies of the expression.
func (st *SemTable) Dependencies(expr sqlparser.Expr) TableSet {
	deps, found := st.exprDependencies[expr]
	if found {
		return deps
	}

	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		colName, ok := node.(*sqlparser.ColName)
		if ok {
			set := st.exprDependencies[colName]
			deps |= set
		}
		return true, nil
	}, expr)

	st.exprDependencies[expr] = deps

	return deps
}

func newScope(parent *scope) *scope {
	return &scope{tables: map[string]*TableInfo{}, parent: parent}
}

func (s *scope) addTable(name string, table *TableInfo) error {
	_, found := s.tables[name]
	if found {
		return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NonUniqTable, "Not unique table/alias: '%s'", name)
	}
	s.tables[name] = table
	return nil
}

// Analyse analyzes the parsed query.
func Analyse(statement sqlparser.Statement, si SchemaInformation) (*SemTable, error) {
	analyzer := newAnalyzer(si)
	// Initial scope
	err := analyzer.analyze(statement)
	if err != nil {
		return nil, err
	}
	return &SemTable{exprDependencies: analyzer.exprDeps, Tables: analyzer.Tables}, nil
}

// IsOverlapping returns true if at least one table exists in both sets
func (ts TableSet) IsOverlapping(b TableSet) bool { return ts&b != 0 }

// IsSolvedBy returns true if all of `ts` is contained in `b`
func (ts TableSet) IsSolvedBy(b TableSet) bool { return ts&b == ts }

// NumberOfTables returns the number of bits set
func (ts TableSet) NumberOfTables() int {
	// Brian Kernighan’s Algorithm
	count := 0
	for ts > 0 {
		ts &= ts - 1
		count++
	}
	return count
}

// TableOffset returns the offset in the Tables array from TableSet
func (ts TableSet) TableOffset() int {
	offset := 0
	for ts > 1 {
		ts = ts >> 1
		offset++
	}
	return offset
}

// Constituents returns an slice with all the
// individual tables in their own TableSet identifier
func (ts TableSet) Constituents() (result []TableSet) {
	mask := ts

	for mask > 0 {
		maskLeft := mask & (mask - 1)
		constituent := mask ^ maskLeft
		mask = maskLeft
		result = append(result, constituent)
	}
	return
}

// Merge creates a TableSet that contains both inputs
func (ts TableSet) Merge(other TableSet) TableSet {
	return ts | other
}
