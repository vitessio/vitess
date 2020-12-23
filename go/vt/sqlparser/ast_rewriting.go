/*
Copyright 2020 The Vitess Authors.

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

package sqlparser

import (
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"strings"

	"vitess.io/vitess/go/vt/sysvars"
)

// RewriteASTResult contains the rewritten ast and meta information about it
type RewriteASTResult struct {
	*BindVarNeeds
	AST Statement // The rewritten AST
}

// PrepareAST will normalize the query
func PrepareAST(in Statement, bindVars map[string]*querypb.BindVariable, prefix string, parameterize bool, keyspace string) (*RewriteASTResult, error) {
	if parameterize {
		Normalize(in, bindVars, prefix)
	}
	return RewriteAST(in, keyspace)
}

// RewriteAST rewrites the whole AST, replacing function calls and adding column aliases to queries
func RewriteAST(in Statement, keyspace string) (*RewriteASTResult, error) {
	er := newExpressionRewriter(keyspace)
	er.shouldRewriteDatabaseFunc = shouldRewriteDatabaseFunc(in)
	setRewriter := &setNormalizer{}
	out, ok := Rewrite(in, er.rewrite, setRewriter.rewriteSetComingUp).(Statement)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "statement rewriting returned a non statement: %s", String(out))
	}
	if setRewriter.err != nil {
		return nil, setRewriter.err
	}

	r := &RewriteASTResult{
		AST:          out,
		BindVarNeeds: er.bindVars,
	}
	return r, nil
}

func shouldRewriteDatabaseFunc(in Statement) bool {
	selct, ok := in.(*Select)
	if !ok {
		return false
	}
	if len(selct.From) != 1 {
		return false
	}
	aliasedTable, ok := selct.From[0].(*AliasedTableExpr)
	if !ok {
		return false
	}
	tableName, ok := aliasedTable.Expr.(TableName)
	if !ok {
		return false
	}
	return tableName.Name.String() == "dual"
}

type expressionRewriter struct {
	bindVars                  *BindVarNeeds
	shouldRewriteDatabaseFunc bool
	err                       error

	// we need to know this to make a decision if we can safely rewrite JOIN USING => JOIN ON
	hasStarInSelect bool

	keyspace string
}

func newExpressionRewriter(keyspace string) *expressionRewriter {
	return &expressionRewriter{bindVars: &BindVarNeeds{}, keyspace: keyspace}
}

const (
	//LastInsertIDName is a reserved bind var name for last_insert_id()
	LastInsertIDName = "__lastInsertId"

	//DBVarName is a reserved bind var name for database()
	DBVarName = "__vtdbname"

	//FoundRowsName is a reserved bind var name for found_rows()
	FoundRowsName = "__vtfrows"

	//RowCountName is a reserved bind var name for row_count()
	RowCountName = "__vtrcount"

	//UserDefinedVariableName is what we prepend bind var names for user defined variables
	UserDefinedVariableName = "__vtudv"
)

func (er *expressionRewriter) rewriteAliasedExpr(node *AliasedExpr) (*BindVarNeeds, error) {
	inner := newExpressionRewriter(er.keyspace)
	inner.shouldRewriteDatabaseFunc = er.shouldRewriteDatabaseFunc
	tmp := Rewrite(node.Expr, inner.rewrite, nil)
	newExpr, ok := tmp.(Expr)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to rewrite AST. function expected to return Expr returned a %s", String(tmp))
	}
	node.Expr = newExpr
	return inner.bindVars, nil
}

func (er *expressionRewriter) rewrite(cursor *Cursor) bool {
	switch node := cursor.Node().(type) {
	// select last_insert_id() -> select :__lastInsertId as `last_insert_id()`
	case *Select:
		for _, col := range node.SelectExprs {
			_, hasStar := col.(*StarExpr)
			if hasStar {
				er.hasStarInSelect = true
			}

			aliasedExpr, ok := col.(*AliasedExpr)
			if ok && aliasedExpr.As.IsEmpty() {
				buf := NewTrackedBuffer(nil)
				aliasedExpr.Expr.Format(buf)
				innerBindVarNeeds, err := er.rewriteAliasedExpr(aliasedExpr)
				if err != nil {
					er.err = err
					return false
				}
				if innerBindVarNeeds.HasRewrites() {
					aliasedExpr.As = NewColIdent(buf.String())
				}
				er.bindVars.MergeWith(innerBindVarNeeds)
			}
		}
	case *FuncExpr:
		er.funcRewrite(cursor, node)
	case *ColName:
		switch node.Name.at {
		case SingleAt:
			er.udvRewrite(cursor, node)
		case DoubleAt:
			er.sysVarRewrite(cursor, node)
		}
	case *Subquery:
		er.unnestSubQueries(cursor, node)
	case JoinCondition:
		er.rewriteJoinCondition(cursor, node)
	case *AliasedTableExpr:
		if !SystemSchema(er.keyspace) {
			break
		}
		aliasTableName, ok := node.Expr.(TableName)
		if !ok {
			return true
		}
		// Qualifier should not be added to dual table
		if aliasTableName.Name.String() == "dual" {
			break
		}
		if er.keyspace != "" && aliasTableName.Qualifier.IsEmpty() {
			aliasTableName.Qualifier = NewTableIdent(er.keyspace)
			node.Expr = aliasTableName
			cursor.Replace(node)
		}
	}
	return true
}

func (er *expressionRewriter) rewriteJoinCondition(cursor *Cursor, node JoinCondition) {
	if node.Using != nil && !er.hasStarInSelect {
		joinTableExpr, ok := cursor.Parent().(*JoinTableExpr)
		if !ok {
			// this is not possible with the current AST
			return
		}
		leftTable, leftOk := joinTableExpr.LeftExpr.(*AliasedTableExpr)
		rightTable, rightOk := joinTableExpr.RightExpr.(*AliasedTableExpr)
		if !(leftOk && rightOk) {
			// we only deal with simple FROM A JOIN B USING queries at the moment
			return
		}
		lft, err := leftTable.TableName()
		if err != nil {
			er.err = err
			return
		}
		rgt, err := rightTable.TableName()
		if err != nil {
			er.err = err
			return
		}
		newCondition := JoinCondition{}
		for _, colIdent := range node.Using {
			lftCol := NewColNameWithQualifier(colIdent.String(), lft)
			rgtCol := NewColNameWithQualifier(colIdent.String(), rgt)
			cmp := &ComparisonExpr{
				Operator: EqualOp,
				Left:     lftCol,
				Right:    rgtCol,
			}
			if newCondition.On == nil {
				newCondition.On = cmp
			} else {
				newCondition.On = &AndExpr{Left: newCondition.On, Right: cmp}
			}
		}
		cursor.Replace(newCondition)
	}
}

func (er *expressionRewriter) sysVarRewrite(cursor *Cursor, node *ColName) {
	lowered := node.Name.Lowered()
	switch lowered {
	case sysvars.Autocommit.Name,
		sysvars.ClientFoundRows.Name,
		sysvars.SkipQueryPlanCache.Name,
		sysvars.SQLSelectLimit.Name,
		sysvars.TransactionMode.Name,
		sysvars.Workload.Name,
		sysvars.DDLStrategy.Name,
		sysvars.ReadAfterWriteGTID.Name,
		sysvars.ReadAfterWriteTimeOut.Name,
		sysvars.SessionTrackGTIDs.Name:
		cursor.Replace(bindVarExpression("__vt" + lowered))
		er.bindVars.AddSysVar(lowered)
	}
}

func (er *expressionRewriter) udvRewrite(cursor *Cursor, node *ColName) {
	udv := strings.ToLower(node.Name.CompliantName())
	cursor.Replace(bindVarExpression(UserDefinedVariableName + udv))
	er.bindVars.AddUserDefVar(udv)
}

var funcRewrites = map[string]string{
	"last_insert_id": LastInsertIDName,
	"database":       DBVarName,
	"schema":         DBVarName,
	"found_rows":     FoundRowsName,
	"row_count":      RowCountName,
}

func (er *expressionRewriter) funcRewrite(cursor *Cursor, node *FuncExpr) {
	bindVar, found := funcRewrites[node.Name.Lowered()]
	if found {
		if bindVar == DBVarName && !er.shouldRewriteDatabaseFunc {
			return
		}
		if len(node.Exprs) > 0 {
			er.err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Argument to %s() not supported", node.Name.Lowered())
			return
		}
		cursor.Replace(bindVarExpression(bindVar))
		er.bindVars.AddFuncResult(bindVar)
	}
}

func (er *expressionRewriter) unnestSubQueries(cursor *Cursor, subquery *Subquery) {
	sel, isSimpleSelect := subquery.Select.(*Select)
	if !isSimpleSelect {
		return
	}

	if !(len(sel.SelectExprs) != 1 ||
		len(sel.OrderBy) != 0 ||
		len(sel.GroupBy) != 0 ||
		len(sel.From) != 1 ||
		sel.Where == nil ||
		sel.Having == nil ||
		sel.Limit == nil) && sel.Lock == NoLock {
		return
	}
	aliasedTable, ok := sel.From[0].(*AliasedTableExpr)
	if !ok {
		return
	}
	table, ok := aliasedTable.Expr.(TableName)
	if !ok || table.Name.String() != "dual" {
		return
	}
	expr, ok := sel.SelectExprs[0].(*AliasedExpr)
	if !ok {
		return
	}
	er.bindVars.NoteRewrite()
	// we need to make sure that the inner expression also gets rewritten,
	// so we fire off another rewriter traversal here
	rewrittenExpr := Rewrite(expr.Expr, er.rewrite, nil)
	cursor.Replace(rewrittenExpr)
}

func bindVarExpression(name string) Expr {
	return NewArgument([]byte(":" + name))
}

// SystemSchema returns true if the schema passed is system schema
func SystemSchema(schema string) bool {
	return strings.EqualFold(schema, "information_schema") ||
		strings.EqualFold(schema, "performance_schema") ||
		strings.EqualFold(schema, "sys") ||
		strings.EqualFold(schema, "mysql")
}
