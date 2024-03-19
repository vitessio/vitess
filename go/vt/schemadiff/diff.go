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

// AllSubsequent returns a list of diffs starting the given diff and followed by all subsequent diffs, if any
func AllSubsequent(diff EntityDiff) (diffs []EntityDiff) {
	for diff != nil && !diff.IsEmpty() {
		diffs = append(diffs, diff)
		diff = diff.SubsequentDiff()
	}
	return diffs
}

// DiffCreateTablesQueries compares two `CREATE TABLE ...` queries (in string form) and returns the diff from table1 to table2.
// Either or both of the queries can be empty. Based on this, the diff could be
// nil, CreateTable, DropTable or AlterTable
func DiffCreateTablesQueries(env *Environment, query1 string, query2 string, hints *DiffHints) (EntityDiff, error) {
	var fromCreateTable *sqlparser.CreateTable
	var ok bool
	if query1 != "" {
		stmt, err := env.Parser().ParseStrictDDL(query1)
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
		stmt, err := env.Parser().ParseStrictDDL(query2)
		if err != nil {
			return nil, err
		}
		toCreateTable, ok = stmt.(*sqlparser.CreateTable)
		if !ok {
			return nil, ErrExpectedCreateTable
		}
	}
	return DiffTables(env, fromCreateTable, toCreateTable, hints)
}

// DiffTables compares two tables and returns the diff from table1 to table2.
// Either or both of the CreateTable statements can be nil. Based on this, the diff could be
// nil, CreateTable, DropTable or AlterTable
func DiffTables(env *Environment, create1 *sqlparser.CreateTable, create2 *sqlparser.CreateTable, hints *DiffHints) (EntityDiff, error) {
	switch {
	case create1 == nil && create2 == nil:
		return nil, nil
	case create1 == nil:
		c2, err := NewCreateTableEntity(env, create2)
		if err != nil {
			return nil, err
		}
		return c2.Create(), nil
	case create2 == nil:
		c1, err := NewCreateTableEntity(env, create1)
		if err != nil {
			return nil, err
		}
		return c1.Drop(), nil
	default:
		c1, err := NewCreateTableEntity(env, create1)
		if err != nil {
			return nil, err
		}
		c2, err := NewCreateTableEntity(env, create2)
		if err != nil {
			return nil, err
		}
		return c1.Diff(c2, hints)
	}
}

// DiffCreateViewsQueries compares two `CREATE TABLE ...` queries (in string form) and returns the diff from table1 to table2.
// Either or both of the queries can be empty. Based on this, the diff could be
// nil, CreateView, DropView or AlterView
func DiffCreateViewsQueries(env *Environment, query1 string, query2 string, hints *DiffHints) (EntityDiff, error) {
	var fromCreateView *sqlparser.CreateView
	var ok bool
	if query1 != "" {
		stmt, err := env.Parser().ParseStrictDDL(query1)
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
		stmt, err := env.Parser().ParseStrictDDL(query2)
		if err != nil {
			return nil, err
		}
		toCreateView, ok = stmt.(*sqlparser.CreateView)
		if !ok {
			return nil, ErrExpectedCreateView
		}
	}
	return DiffViews(env, fromCreateView, toCreateView, hints)
}

// DiffViews compares two views and returns the diff from view1 to view2
// Either or both of the CreateView statements can be nil. Based on this, the diff could be
// nil, CreateView, DropView or AlterView
func DiffViews(env *Environment, create1 *sqlparser.CreateView, create2 *sqlparser.CreateView, hints *DiffHints) (EntityDiff, error) {
	switch {
	case create1 == nil && create2 == nil:
		return nil, nil
	case create1 == nil:
		c2, err := NewCreateViewEntity(env, create2)
		if err != nil {
			return nil, err
		}
		return c2.Create(), nil
	case create2 == nil:
		c1, err := NewCreateViewEntity(env, create1)
		if err != nil {
			return nil, err
		}
		return c1.Drop(), nil
	default:
		c1, err := NewCreateViewEntity(env, create1)
		if err != nil {
			return nil, err
		}
		c2, err := NewCreateViewEntity(env, create2)
		if err != nil {
			return nil, err
		}
		return c1.Diff(c2, hints)
	}
}

// DiffSchemasSQL compares two schemas and returns the rich diff that turns
// 1st schema into 2nd. Schemas are build from SQL, each of which can contain an arbitrary number of
// CREATE TABLE and CREATE VIEW statements.
func DiffSchemasSQL(env *Environment, sql1 string, sql2 string, hints *DiffHints) (*SchemaDiff, error) {
	schema1, err := NewSchemaFromSQL(env, sql1)
	if err != nil {
		return nil, err
	}
	schema2, err := NewSchemaFromSQL(env, sql2)
	if err != nil {
		return nil, err
	}
	return schema1.SchemaDiff(schema2, hints)
}

// DiffSchemas compares two schemas and returns the list of diffs that turn
// 1st schema into 2nd. Any of the schemas may be nil.
func DiffSchemas(env *Environment, schema1 *Schema, schema2 *Schema, hints *DiffHints) (*SchemaDiff, error) {
	if schema1 == nil {
		schema1 = newEmptySchema(env)
	}
	if schema2 == nil {
		schema2 = newEmptySchema(env)
	}
	return schema1.SchemaDiff(schema2, hints)
}

// EntityDiffByStatement is a helper function that returns a simplified and incomplete EntityDiff based on the given SQL statement.
// It is useful for testing purposes as a quick mean to wrap a statement with a diff.
func EntityDiffByStatement(statement sqlparser.Statement) EntityDiff {
	switch stmt := statement.(type) {
	case *sqlparser.CreateTable:
		return &CreateTableEntityDiff{createTable: stmt}
	case *sqlparser.RenameTable:
		return &RenameTableEntityDiff{renameTable: stmt}
	case *sqlparser.AlterTable:
		return &AlterTableEntityDiff{alterTable: stmt}
	case *sqlparser.DropTable:
		return &DropTableEntityDiff{dropTable: stmt}
	case *sqlparser.CreateView:
		return &CreateViewEntityDiff{createView: stmt}
	case *sqlparser.AlterView:
		return &AlterViewEntityDiff{alterView: stmt}
	case *sqlparser.DropView:
		return &DropViewEntityDiff{dropView: stmt}
	}
	return nil
}
