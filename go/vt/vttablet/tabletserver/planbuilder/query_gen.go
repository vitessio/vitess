/*
Copyright 2019 The Vitess Authors.

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
