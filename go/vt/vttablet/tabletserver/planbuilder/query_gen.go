// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	buf.Myprintf("insert %v%sinto %v%v values %a",
		ins.Comments,
		ins.Ignore,
		ins.Table,
		ins.Columns,
		":#values",
	)
	return buf.ParsedQuery()
}

// GenerateLoadMessagesQuery generates the query to load messages after insert.
func GenerateLoadMessagesQuery(ins *sqlparser.Insert) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("select time_next, epoch, id, message from %v where %a", ins.Table, ":#pk")
	return buf.ParsedQuery()
}

// GenerateUpdateOuterQuery generates the outer query for updates.
func GenerateUpdateOuterQuery(upd *sqlparser.Update) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("update %v%v set %v where %a", upd.Comments, upd.Table, upd.Exprs, ":#pk")
	return buf.ParsedQuery()
}

// GenerateDeleteOuterQuery generates the outer query for deletes.
func GenerateDeleteOuterQuery(del *sqlparser.Delete) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("delete %vfrom %v where %a", del.Comments, del.Table, ":#pk")
	return buf.ParsedQuery()
}

// GenerateUpdateSubquery generates the subquery for updats.
func GenerateUpdateSubquery(upd *sqlparser.Update, table *schema.Table) *sqlparser.ParsedQuery {
	return GenerateSubquery(
		table.Indexes[0].Columns,
		upd.Table,
		upd.Where,
		upd.OrderBy,
		upd.Limit,
		true,
	)
}

// GenerateDeleteSubquery generates the subquery for deletes.
func GenerateDeleteSubquery(del *sqlparser.Delete, table *schema.Table) *sqlparser.ParsedQuery {
	return GenerateSubquery(
		table.Indexes[0].Columns,
		&sqlparser.AliasedTableExpr{Expr: del.Table},
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
