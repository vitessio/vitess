package schemadiff

import (
	"vitess.io/vitess/go/vt/sqlparser"
)

// DDLActionStr returns the action implied by the given diff: CREATE", "DROP", "ALTER" or empty
func DDLActionStr(diff EntityDiff) (string, error) {
	if diff == nil {
		return "", nil
	}
	if ddl, ok := diff.Statement().(sqlparser.DDLStatement); ok {
		return ddl.GetAction().ToString(), nil
	}
	return "", ErrUnexpectedDiffAction
}

// DiffCreateTablesQueries compares two `CREATE TABLE ...` queries (in string form) and returns the diff from table1 to table2.
// Either or both of the queries can be empty. Based on this, the diff could be
// nil, CreateTable, DropTable or AlterTable
func DiffCreateTablesQueries(query1 string, query2 string, hints *DiffHints) (EntityDiff, error) {
	var fromCreateTable *sqlparser.CreateTable
	var ok bool
	if query1 != "" {
		stmt, err := sqlparser.Parse(query1)
		if err != nil {
			return nil, err
		}
		fromCreateTable, ok = stmt.(*sqlparser.CreateTable)
		if !ok {
			return nil, ErrExpectedCreateTable
		}
	}
	var toCreateTable *sqlparser.CreateTable
	if query2 != "" {
		stmt, err := sqlparser.Parse(query2)
		if err != nil {
			return nil, err
		}
		toCreateTable, ok = stmt.(*sqlparser.CreateTable)
		if !ok {
			return nil, ErrExpectedCreateTable
		}
	}
	return DiffTables(fromCreateTable, toCreateTable, hints)
}

// DiffTables compares two tables and returns the diff from table1 to table2.
// Either or both of the CreateTable statements can be nil. Based on this, the diff could be
// nil, CreateTable, DropTable or AlterTable
func DiffTables(create1 *sqlparser.CreateTable, create2 *sqlparser.CreateTable, hints *DiffHints) (EntityDiff, error) {
	if create1 != nil && !create1.IsFullyParsed() {
		return nil, ErrNotFullyParsed
	}
	if create2 != nil && !create2.IsFullyParsed() {
		return nil, ErrNotFullyParsed
	}
	switch {
	case create1 == nil && create2 == nil:
		return nil, nil
	case create1 == nil:
		return &CreateTableEntityDiff{createTable: create2}, nil
	case create2 == nil:
		dropTable := &sqlparser.DropTable{
			FromTables: []sqlparser.TableName{create1.Table},
		}
		return &DropTableEntityDiff{dropTable: dropTable}, nil
	default:
		c1 := NewCreateTableEntity(create1)
		c2 := NewCreateTableEntity(create2)
		return c1.Diff(c2, hints)
	}
}

// DiffCreateViewsQueries compares two `CREATE TABLE ...` queries (in string form) and returns the diff from table1 to table2.
// Either or both of the queries can be empty. Based on this, the diff could be
// nil, CreateView, DropView or AlterView
func DiffCreateViewsQueries(query1 string, query2 string, hints *DiffHints) (EntityDiff, error) {
	var fromCreateView *sqlparser.CreateView
	var ok bool
	if query1 != "" {
		stmt, err := sqlparser.Parse(query1)
		if err != nil {
			return nil, err
		}
		fromCreateView, ok = stmt.(*sqlparser.CreateView)
		if !ok {
			return nil, ErrExpectedCreateView
		}
	}
	var toCreateView *sqlparser.CreateView
	if query2 != "" {
		stmt, err := sqlparser.Parse(query2)
		if err != nil {
			return nil, err
		}
		toCreateView, ok = stmt.(*sqlparser.CreateView)
		if !ok {
			return nil, ErrExpectedCreateView
		}
	}
	return DiffViews(fromCreateView, toCreateView, hints)
}

// DiffTables compares two views and returns the diff from view1 to view2
// Either or both of the CreateView statements can be nil. Based on this, the diff could be
// nil, CreateView, DropView or AlterView
func DiffViews(create1 *sqlparser.CreateView, create2 *sqlparser.CreateView, hints *DiffHints) (EntityDiff, error) {
	if create1 != nil && !create1.IsFullyParsed() {
		return nil, ErrNotFullyParsed
	}
	if create2 != nil && !create2.IsFullyParsed() {
		return nil, ErrNotFullyParsed
	}
	switch {
	case create1 == nil && create2 == nil:
		return nil, nil
	case create1 == nil:
		return &CreateViewEntityDiff{createView: create2}, nil
	case create2 == nil:
		dropView := &sqlparser.DropView{
			FromTables: []sqlparser.TableName{create1.ViewName},
		}
		return &DropViewEntityDiff{dropView: dropView}, nil
	default:
		c1 := NewCreateViewEntity(create1)
		c2 := NewCreateViewEntity(create2)
		return c1.Diff(c2, hints)
	}
}
