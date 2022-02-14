package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
)

// GenerateFullQuery generates the full query from the ast.
func GenerateFullQuery(statement sqlparser.Statement) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	statement.Format(buf)
	return buf.ParsedQuery()
}

// GenerateFieldQuery generates a query to just fetch the field info
// by adding impossible where clauses as needed.
func GenerateFieldQuery(statement sqlparser.Statement) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery).WriteNode(statement)

	if buf.HasBindVars() {
		return nil
	}

	return buf.ParsedQuery()
}

// GenerateLimitQuery generates a select query with a limit clause.
func GenerateLimitQuery(selStmt sqlparser.SelectStatement) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	switch sel := selStmt.(type) {
	case *sqlparser.Select:
		limit := sel.Limit
		if limit == nil {
			sel.Limit = execLimit
			defer func() {
				sel.Limit = nil
			}()
		}
	case *sqlparser.Union:
		// Code is identical to *Select, but this one is a *Union.
		limit := sel.Limit
		if limit == nil {
			sel.Limit = execLimit
			defer func() {
				sel.Limit = nil
			}()
		}
	}
	buf.Myprintf("%v", selStmt)
	return buf.ParsedQuery()
}
