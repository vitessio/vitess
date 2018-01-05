/*
Copyright 2017 Google Inc.

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
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
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

// GenerateInsertOuterQuery generates the outer query for inserts.
func GenerateInsertOuterQuery(ins *sqlparser.Insert) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%s %v%sinto %v%v values %a",
		ins.Action,
		ins.Comments,
		ins.Ignore,
		ins.Table,
		ins.Columns,
		":#values",
	)
	return buf.ParsedQuery()
}

// GenerateUpdateOuterQuery generates the outer query for updates.
// If there is no custom formatting needed, formatter can be nil.
func GenerateUpdateOuterQuery(upd *sqlparser.Update, formatter sqlparser.NodeFormatter) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(formatter)
	buf.Myprintf("update %v%v set %v where %a%v", upd.Comments, upd.TableExprs, upd.Exprs, ":#pk", upd.OrderBy)
	return buf.ParsedQuery()
}

// GenerateDeleteOuterQuery generates the outer query for deletes.
func GenerateDeleteOuterQuery(del *sqlparser.Delete) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("delete %vfrom %v where %a%v", del.Comments, del.TableExprs, ":#pk", del.OrderBy)
	return buf.ParsedQuery()
}

// GenerateUpdateSubquery generates the subquery for updates.
func GenerateUpdateSubquery(upd *sqlparser.Update, table *schema.Table, aliased *sqlparser.AliasedTableExpr) *sqlparser.ParsedQuery {
	return GenerateSubquery(
		table.Indexes[0].Columns,
		aliased,
		upd.Where,
		upd.OrderBy,
		upd.Limit,
		true,
	)
}

// GenerateDeleteSubquery generates the subquery for deletes.
func GenerateDeleteSubquery(del *sqlparser.Delete, table *schema.Table, aliased *sqlparser.AliasedTableExpr) *sqlparser.ParsedQuery {
	return GenerateSubquery(
		table.Indexes[0].Columns,
		aliased,
		del.Where,
		del.OrderBy,
		del.Limit,
		true,
	)
}

// GenerateSubquery generates a subquery based on the input parameters.
func GenerateSubquery(columns []sqlparser.ColIdent, table *sqlparser.AliasedTableExpr, where *sqlparser.Where, order sqlparser.OrderBy, limit *sqlparser.Limit, forUpdate bool) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	if limit == nil {
		limit = execLimit
	}
	buf.WriteString("select ")
	prefix := ""
	for _, c := range columns {
		buf.Myprintf("%s%v", prefix, c)
		prefix = ", "
	}
	buf.Myprintf(" from %v%v%v%v", table, where, order, limit)
	if forUpdate {
		buf.Myprintf(sqlparser.ForUpdateStr)
	}
	return buf.ParsedQuery()
}
