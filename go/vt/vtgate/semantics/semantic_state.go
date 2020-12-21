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
	"fmt"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	table = *sqlparser.AliasedTableExpr

	// TableSet is how a set of tables is expressed.
	// Tables get unique bits assigned in the order that they are encountered during semantic analysis
	TableSet uint64 // we can only join 64 tables with this underlying data type

	// SemTable contains semantic analysis information about the query.
	SemTable struct {
		Tables           []table
		exprDependencies map[sqlparser.Expr]TableSet
	}
	schemaInformation interface {
		FindTable(tablename sqlparser.TableName) (*vindexes.Table, error)
	}
	scope struct {
		parent *scope
		tables map[string]*sqlparser.AliasedTableExpr
	}
)

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

// TableSetFor returns the bitmask for this particular tableshoe
func (st *SemTable) TableSetFor(t table) TableSet {
	for idx, t2 := range st.Tables {
		if t == t2 {
			return 1 << idx
		}
	}
	return 0
}

// Dependencies return the table dependencies of the expression.
func (st *SemTable) Dependencies(expr sqlparser.Expr) TableSet {
	var deps TableSet

	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		colName, ok := node.(*sqlparser.ColName)
		if ok {
			set := st.exprDependencies[colName]
			deps |= set
		}
		return true, nil
	}, expr)

	return deps
}

func newScope(parent *scope) *scope {
	return &scope{tables: map[string]*sqlparser.AliasedTableExpr{}, parent: parent}
}

func (s *scope) addTable(name string, table *sqlparser.AliasedTableExpr) error {
	_, found := s.tables[name]
	if found {
		return mysql.NewSQLError(mysql.ERNonUniqTable, mysql.SSSyntaxErrorOrAccessViolation, "Not unique table/alias: '%s'", name)
	}
	s.tables[name] = table
	return nil
}

// Analyse analyzes the parsed query.
func Analyse(statement sqlparser.Statement, si schemaInformation) (*SemTable, error) {
	analyzer := newAnalyzer(si)
	// Initial scope
	err := analyzer.analyze(statement)
	if err != nil {
		return nil, err
	}
	return &SemTable{exprDependencies: analyzer.exprDeps, Tables: analyzer.Tables}, nil
}

func log(node sqlparser.SQLNode, format string, args ...interface{}) {
	if debug {
		fmt.Printf(format, args...)
		if node == nil {
			fmt.Println()
		} else {
			fmt.Println(" - " + sqlparser.String(node))
		}
	}
}

// IsOverlapping returns true if at least one table exists in both sets
func IsOverlapping(a, b TableSet) bool { return a&b != 0 }

// IsContainedBy returns true if all of `b` is contained in `a`
func IsContainedBy(a, b TableSet) bool { return a&b == a }
