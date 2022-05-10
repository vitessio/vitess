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
		stmt, err := sqlparser.ParseStrictDDL(query1)
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
		stmt, err := sqlparser.ParseStrictDDL(query2)
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
		return NewCreateTableEntity(create2).Create(), nil
	case create2 == nil:
		return NewCreateTableEntity(create1).Drop(), nil
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
		stmt, err := sqlparser.ParseStrictDDL(query1)
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
		stmt, err := sqlparser.ParseStrictDDL(query2)
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
		return NewCreateViewEntity(create2).Create(), nil
	case create2 == nil:
		return NewCreateViewEntity(create1).Drop(), nil
	default:
		c1 := NewCreateViewEntity(create1)
		c2 := NewCreateViewEntity(create2)
		return c1.Diff(c2, hints)
	}
}

// DiffSchemasSQL compares two schemas and returns the list of diffs that turn
// 1st schema into 2nd. Schemas are build from SQL, each of which can contain an arbitrary number of
// CREATE TABLE and CREATE VIEW statements.
func DiffSchemasSQL(sql1 string, sql2 string, hints *DiffHints) ([]EntityDiff, error) {
	schema1, err := NewSchemaFromSQL(sql1)
	if err != nil {
		return nil, err
	}
	schema2, err := NewSchemaFromSQL(sql2)
	if err != nil {
		return nil, err
	}
	return schema1.Diff(schema2, hints)
}

// DiffSchemasSQL compares two schemas and returns the list of diffs that turn
// 1st schema into 2nd. Any of the schemas may be nil.
func DiffSchemas(schema1 *Schema, schema2 *Schema, hints *DiffHints) ([]EntityDiff, error) {
	if schema1 == nil {
		schema1 = newEmptySchema()
	}
	if schema2 == nil {
		schema2 = newEmptySchema()
	}
	return schema1.Diff(schema2, hints)
}
