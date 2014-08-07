// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

func GenerateFullQuery(statement sqlparser.Statement) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	statement.Format(buf)
	return buf.ParsedQuery()
}

func GenerateFieldQuery(statement sqlparser.Statement) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(FormatImpossible)
	buf.Myprintf("%v", statement)
	if buf.HasBindVars() {
		return nil
	}
	return buf.ParsedQuery()
}

// FormatImpossible is a callback function used by TrackedBuffer
// to generate a modified version of the query where all selects
// have impossible where clauses. It overrides a few node types
// and passes the rest down to the default FormatNode.
func FormatImpossible(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch node := node.(type) {
	case *sqlparser.Select:
		buf.Myprintf("select %v from %v where 1 != 1", node.SelectExprs, node.From)
	case *sqlparser.JoinTableExpr:
		if node.Join == sqlparser.AST_LEFT_JOIN || node.Join == sqlparser.AST_RIGHT_JOIN {
			// ON clause is requried
			buf.Myprintf("%v %s %v on 1 != 1", node.LeftExpr, node.Join, node.RightExpr)
		} else {
			buf.Myprintf("%v %s %v", node.LeftExpr, node.Join, node.RightExpr)
		}
	default:
		node.Format(buf)
	}
}

func GenerateSelectLimitQuery(selStmt sqlparser.SelectStatement) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	sel, ok := selStmt.(*sqlparser.Select)
	if ok {
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

func GenerateEqualOuterQuery(sel *sqlparser.Select, tableInfo *schema.Table) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	fmt.Fprintf(buf, "select ")
	writeColumnList(buf, tableInfo.Columns)
	buf.Myprintf(" from %v where ", sel.From)
	generatePKWhere(buf, tableInfo.Indexes[0])
	return buf.ParsedQuery()
}

func GenerateInOuterQuery(sel *sqlparser.Select, tableInfo *schema.Table) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	fmt.Fprintf(buf, "select ")
	writeColumnList(buf, tableInfo.Columns)
	// We assume there is one and only one PK column.
	// A '*' argument name means all variables of the list.
	buf.Myprintf(" from %v where %s in (%a)", sel.From, tableInfo.Indexes[0].Columns[0], "*")
	return buf.ParsedQuery()
}

func GenerateInsertOuterQuery(ins *sqlparser.Insert) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("insert %vinto %v%v values %a%v",
		ins.Comments,
		ins.Table,
		ins.Columns,
		"_rowValues",
		ins.OnDup,
	)
	return buf.ParsedQuery()
}

func GenerateUpdateOuterQuery(upd *sqlparser.Update, pkIndex *schema.Index) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("update %v%v set %v where ", upd.Comments, upd.Table, upd.Exprs)
	generatePKWhere(buf, pkIndex)
	return buf.ParsedQuery()
}

func GenerateDeleteOuterQuery(del *sqlparser.Delete, pkIndex *schema.Index) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("delete %vfrom %v where ", del.Comments, del.Table)
	generatePKWhere(buf, pkIndex)
	return buf.ParsedQuery()
}

func generatePKWhere(buf *sqlparser.TrackedBuffer, pkIndex *schema.Index) {
	for i := 0; i < len(pkIndex.Columns); i++ {
		if i != 0 {
			buf.WriteString(" and ")
		}
		buf.Myprintf("%s = %a", pkIndex.Columns[i], strconv.FormatInt(int64(i), 10))
	}
}

func GenerateSelectSubquery(sel *sqlparser.Select, tableInfo *schema.Table, index string) *sqlparser.ParsedQuery {
	hint := &sqlparser.IndexHints{Type: sqlparser.AST_USE, Indexes: [][]byte{[]byte(index)}}
	table_expr := sel.From[0].(*sqlparser.AliasedTableExpr)
	savedHint := table_expr.Hints
	table_expr.Hints = hint
	defer func() {
		table_expr.Hints = savedHint
	}()
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		table_expr,
		sel.Where,
		sel.OrderBy,
		sel.Limit,
		false,
	)
}

func GenerateUpdateSubquery(upd *sqlparser.Update, tableInfo *schema.Table) *sqlparser.ParsedQuery {
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		&sqlparser.AliasedTableExpr{Expr: upd.Table},
		upd.Where,
		upd.OrderBy,
		upd.Limit,
		true,
	)
}

func GenerateDeleteSubquery(del *sqlparser.Delete, tableInfo *schema.Table) *sqlparser.ParsedQuery {
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		&sqlparser.AliasedTableExpr{Expr: del.Table},
		del.Where,
		del.OrderBy,
		del.Limit,
		true,
	)
}

func GenerateSubquery(columns []string, table *sqlparser.AliasedTableExpr, where *sqlparser.Where, order sqlparser.OrderBy, limit *sqlparser.Limit, for_update bool) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	if limit == nil {
		limit = execLimit
	}
	fmt.Fprintf(buf, "select ")
	i := 0
	for i = 0; i < len(columns)-1; i++ {
		fmt.Fprintf(buf, "%s, ", columns[i])
	}
	fmt.Fprintf(buf, "%s", columns[i])
	buf.Myprintf(" from %v%v%v%v", table, where, order, limit)
	if for_update {
		buf.Myprintf(sqlparser.AST_FOR_UPDATE)
	}
	return buf.ParsedQuery()
}

func writeColumnList(buf *sqlparser.TrackedBuffer, columns []schema.TableColumn) {
	i := 0
	for i = 0; i < len(columns)-1; i++ {
		fmt.Fprintf(buf, "%s, ", columns[i].Name)
	}
	fmt.Fprintf(buf, "%s", columns[i].Name)
}
