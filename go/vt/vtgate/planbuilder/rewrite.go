/*
Copyright 2021 The Vitess Authors.

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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	expandStarInfo struct {
		proceed   bool
		tblColMap map[*sqlparser.AliasedTableExpr]sqlparser.SelectExprs
	}
	rewriter struct {
		err          error
		semTable     *semantics.SemTable
		reservedVars *sqlparser.ReservedVars
	}
)

func (r *rewriter) starRewrite(cursor *sqlparser.Cursor) bool {
	switch node := cursor.Node().(type) {
	case *sqlparser.Select:
		tables := r.semTable.GetSelectTables(node)
		var selExprs sqlparser.SelectExprs
		for _, selectExpr := range node.SelectExprs {
			starExpr, isStarExpr := selectExpr.(*sqlparser.StarExpr)
			if !isStarExpr {
				selExprs = append(selExprs, selectExpr)
				continue
			}
			colNames, expStar, err := expandTableColumns(tables, starExpr)
			if err != nil {
				r.err = err
				return false
			}
			if !expStar.proceed {
				selExprs = append(selExprs, selectExpr)
				continue
			}
			selExprs = append(selExprs, colNames...)
			for tbl, cols := range expStar.tblColMap {
				r.semTable.AddExprs(tbl, cols)
			}
		}
		node.SelectExprs = selExprs
	}
	return true
}

func expandTableColumns(tables []semantics.TableInfo, starExpr *sqlparser.StarExpr) (sqlparser.SelectExprs, *expandStarInfo, error) {
	unknownTbl := true
	var colNames sqlparser.SelectExprs
	expStar := &expandStarInfo{
		tblColMap: map[*sqlparser.AliasedTableExpr]sqlparser.SelectExprs{},
	}

	for _, tbl := range tables {
		if !starExpr.TableName.IsEmpty() && !tbl.Matches(starExpr.TableName) {
			continue
		}
		unknownTbl = false
		if !tbl.Authoritative() {
			expStar.proceed = false
			break
		}
		expStar.proceed = true
		tblName, err := tbl.Name()
		if err != nil {
			return nil, nil, err
		}
		for _, col := range tbl.GetColumns() {
			colNames = append(colNames, &sqlparser.AliasedExpr{
				Expr: sqlparser.NewColNameWithQualifier(col.Name, tblName),
				As:   sqlparser.NewColIdent(col.Name),
			})
		}
		expStar.tblColMap[tbl.GetExpr()] = colNames
	}

	if unknownTbl {
		// This will only happen for case when starExpr has qualifier.
		return nil, nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadDb, "Unknown table '%s'", sqlparser.String(starExpr.TableName))
	}
	return colNames, expStar, nil
}

func (r *rewriter) subqueryRewrite(cursor *sqlparser.Cursor) bool {
	switch node := cursor.Node().(type) {
	case *sqlparser.ExistsExpr:
		semTableSQ, found := r.semTable.SubqueryRef[node.Subquery]
		if !found {
			// should never happen
			return false
		}

		argName := r.reservedVars.ReserveHasValuesSubQuery()
		semTableSQ.ArgName = argName

		cursor.Replace(sqlparser.NewArgument(argName))
		return false
	case *sqlparser.Subquery:
		semTableSQ, found := r.semTable.SubqueryRef[node]
		if !found {
			// should never happen
			return false
		}

		argName := r.reservedVars.ReserveSubQuery()
		semTableSQ.ArgName = argName

		switch semTableSQ.OpCode {
		case engine.PulloutIn, engine.PulloutNotIn:
			cursor.Replace(sqlparser.NewListArg(argName))
		default:
			cursor.Replace(sqlparser.NewArgument(argName))
		}
	}
	return true
}

func (r *rewriter) rewriterPre(cursor *sqlparser.Cursor) bool {
	return r.starRewrite(cursor) && (len(r.semTable.SubqueryMap) == 0 || r.subqueryRewrite(cursor))
}

func rewrite(sel *sqlparser.Select, semTable *semantics.SemTable, reservedVars *sqlparser.ReservedVars) (*sqlparser.Select, error) {
	r := &rewriter{semTable: semTable, reservedVars: reservedVars}

	_ = sqlparser.Rewrite(sel, r.rewriterPre, nil)
	if r.err != nil {
		return nil, r.err
	}
	return sel, nil
}
