package schemadiff

import "vitess.io/vitess/go/vt/sqlparser"

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

// DiffTables compares two tables and returns the diff from table1 to table2.
// Either or both of the CreateTable statements can be nil. Based on this, the diff could be
// nil, CreateTable, DropTable or AlterTable
func DiffTables(table1 *sqlparser.CreateTable, table2 *sqlparser.CreateTable, hints *DiffHints) (EntityDiff, error) {
	if table1 != nil && !table1.IsFullyParsed() {
		return nil, ErrNotFullyParsed
	}
	if table2 != nil && !table2.IsFullyParsed() {
		return nil, ErrNotFullyParsed
	}
	switch {
	case table1 == nil && table2 == nil:
		return nil, nil
	case table1 == nil:
		return &CreateTableEntityDiff{CreateTable: *table2}, nil
	case table2 == nil:
		dropTable := sqlparser.DropTable{
			FromTables: []sqlparser.TableName{table1.Table},
		}
		return &DropTableEntityDiff{dropTable}, nil
	default:
		c1 := NewCreateTableEntity(table1)
		c2 := NewCreateTableEntity(table2)
		return c1.Diff(c2, hints)
	}
}

// DiffTables compares two views and returns the diff from view1 to view2
// Either or both of the CreateView statements can be nil. Based on this, the diff could be
// nil, CreateView, DropView or AlterView
func DiffViews(view1 *sqlparser.CreateView, view2 *sqlparser.CreateView, hints *DiffHints) (EntityDiff, error) {
	if view1 != nil && !view1.IsFullyParsed() {
		return nil, ErrNotFullyParsed
	}
	if view2 != nil && !view2.IsFullyParsed() {
		return nil, ErrNotFullyParsed
	}
	switch {
	case view1 == nil && view2 == nil:
		return nil, nil
	case view1 == nil:
		return &CreateViewEntityDiff{CreateView: *view2}, nil
	case view2 == nil:
		dropView := sqlparser.DropView{
			FromTables: []sqlparser.TableName{view1.ViewName},
		}
		return &DropViewEntityDiff{dropView}, nil
	default:
		c1 := NewCreateViewEntity(view1)
		c2 := NewCreateViewEntity(view2)
		return c1.Diff(c2, hints)
	}
}
