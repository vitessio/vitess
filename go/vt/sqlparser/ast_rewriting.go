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
func PrepareAST(in Statement, reservedVars BindVars, bindVars map[string]*querypb.BindVariable, prefix string, parameterize bool, keyspace string) (*RewriteASTResult, error) {
	if parameterize {
		err := Normalize(in, reservedVars, bindVars, prefix)
		if err != nil {
			return nil, err
		}
	}
	return RewriteAST(in, keyspace)
}

// RewriteAST rewrites the whole AST, replacing function calls and adding column aliases to queries
func RewriteAST(in Statement, keyspace string) (*RewriteASTResult, error) {
	er := newExpressionRewriter(keyspace)
	er.shouldRewriteDatabaseFunc = shouldRewriteDatabaseFunc(in)
	setRewriter := &setNormalizer{}
	result := Rewrite(in, er.rewrite, setRewriter.rewriteSetComingUp)
	if setRewriter.err != nil {
		return nil, setRewriter.err
	}

	out, ok := result.(Statement)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "statement rewriting returned a non statement: %s", String(out))
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
	case *ShowBasic:
		if node.Command == VariableGlobal || node.Command == VariableSession {
			varsToAdd := sysvars.GetInterestingVariables()
			for _, sysVar := range varsToAdd {
				er.bindVars.AddSysVar(sysVar)
			}
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
		sysvars.DDLStrategy.Name,
		sysvars.TransactionMode.Name,
		sysvars.ReadAfterWriteGTID.Name,
		sysvars.ReadAfterWriteTimeOut.Name,
		sysvars.SessionEnableSystemSettings.Name,
		sysvars.SessionTrackGTIDs.Name,
		sysvars.SessionUUID.Name,
		sysvars.SkipQueryPlanCache.Name,
		sysvars.Socket.Name,
		sysvars.SQLSelectLimit.Name,
		sysvars.Version.Name,
		sysvars.VersionComment.Name,
		sysvars.Workload.Name:
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
	return NewArgument(":" + name)
}

// SystemSchema returns true if the schema passed is system schema
func SystemSchema(schema string) bool {
	return strings.EqualFold(schema, "information_schema") ||
		strings.EqualFold(schema, "performance_schema") ||
		strings.EqualFold(schema, "sys") ||
		strings.EqualFold(schema, "mysql")
}

// RewriteToCNF walks the input AST and rewrites any boolean logic into CNF
// Note: In order to re-plan, we need to empty the accumulated metadata in the AST,
// so ColName.Metadata will be nil:ed out as part of this rewrite
func RewriteToCNF(ast SQLNode) SQLNode {
	for {
		finishedRewrite := true
		ast = Rewrite(ast, func(cursor *Cursor) bool {
			if e, isExpr := cursor.node.(Expr); isExpr {
				rewritten, didRewrite := rewriteToCNFExpr(e)
				if didRewrite {
					finishedRewrite = false
					cursor.Replace(rewritten)
				}
			}
			if col, isCol := cursor.node.(*ColName); isCol {
				col.Metadata = nil
			}
			return true
		}, nil)

		if finishedRewrite {
			return ast
		}
	}
}

func distinctOr(in *OrExpr) (Expr, bool) {
	todo := []*OrExpr{in}
	var leaves []Expr
	for len(todo) > 0 {
		curr := todo[0]
		todo = todo[1:]
		addAnd := func(in Expr) {
			and, ok := in.(*OrExpr)
			if ok {
				todo = append(todo, and)
			} else {
				leaves = append(leaves, in)
			}
		}
		addAnd(curr.Left)
		addAnd(curr.Right)
	}
	original := len(leaves)
	var predicates []Expr

outer1:
	for len(leaves) > 0 {
		curr := leaves[0]
		leaves = leaves[1:]
		for _, alreadyIn := range predicates {
			if EqualsExpr(alreadyIn, curr) {
				continue outer1
			}
		}
		predicates = append(predicates, curr)
	}
	if original == len(predicates) {
		return in, false
	}
	var result Expr
	for i, curr := range predicates {
		if i == 0 {
			result = curr
			continue
		}
		result = &OrExpr{Left: result, Right: curr}
	}
	return result, true
}
func distinctAnd(in *AndExpr) (Expr, bool) {
	todo := []*AndExpr{in}
	var leaves []Expr
	for len(todo) > 0 {
		curr := todo[0]
		todo = todo[1:]
		addAnd := func(in Expr) {
			and, ok := in.(*AndExpr)
			if ok {
				todo = append(todo, and)
			} else {
				leaves = append(leaves, in)
			}
		}
		addAnd(curr.Left)
		addAnd(curr.Right)
	}
	original := len(leaves)
	var predicates []Expr

outer1:
	for len(leaves) > 0 {
		curr := leaves[0]
		leaves = leaves[1:]
		for _, alreadyIn := range predicates {
			if EqualsExpr(alreadyIn, curr) {
				continue outer1
			}
		}
		predicates = append(predicates, curr)
	}
	if original == len(predicates) {
		return in, false
	}
	var result Expr
	for i, curr := range predicates {
		if i == 0 {
			result = curr
			continue
		}
		result = &AndExpr{Left: result, Right: curr}
	}
	return result, true
}

func rewriteToCNFExpr(expr Expr) (Expr, bool) {
	switch expr := expr.(type) {
	case *NotExpr:
		switch child := expr.Expr.(type) {
		case *NotExpr:
			// NOT NOT A => A
			return child.Expr, true
		case *OrExpr:
			// DeMorgan Rewriter
			// NOT (A OR B) => NOT A AND NOT B
			return &AndExpr{Right: &NotExpr{Expr: child.Right}, Left: &NotExpr{Expr: child.Left}}, true
		case *AndExpr:
			// DeMorgan Rewriter
			// NOT (A AND B) => NOT A OR NOT B
			return &OrExpr{Right: &NotExpr{Expr: child.Right}, Left: &NotExpr{Expr: child.Left}}, true
		}
	case *OrExpr:
		or := expr
		if and, ok := or.Left.(*AndExpr); ok {
			// Simplification
			// (A AND B) OR A => A
			if EqualsExpr(or.Right, and.Left) || EqualsExpr(or.Right, and.Right) {
				return or.Right, true
			}
			// Distribution Law
			// (A AND B) OR C => (A OR C) AND (B OR C)
			return &AndExpr{Left: &OrExpr{Left: and.Left, Right: or.Right}, Right: &OrExpr{Left: and.Right, Right: or.Right}}, true
		}
		if and, ok := or.Right.(*AndExpr); ok {
			// Simplification
			// A OR (A AND B) => A
			if EqualsExpr(or.Left, and.Left) || EqualsExpr(or.Left, and.Right) {
				return or.Left, true
			}
			// Distribution Law
			// C OR (A AND B) => (C OR A) AND (C OR B)
			return &AndExpr{Left: &OrExpr{Left: or.Left, Right: and.Left}, Right: &OrExpr{Left: or.Left, Right: and.Right}}, true
		}
		// Try to make distinct
		return distinctOr(expr)

	case *XorExpr:
		// DeMorgan Rewriter
		// (A XOR B) => (A OR B) AND NOT (A AND B)
		return &AndExpr{Left: &OrExpr{Left: expr.Left, Right: expr.Right}, Right: &NotExpr{Expr: &AndExpr{Left: expr.Left, Right: expr.Right}}}, true
	case *AndExpr:
		res, rewritten := distinctAnd(expr)
		if rewritten {
			return res, rewritten
		}
		and := expr
		if or, ok := and.Left.(*OrExpr); ok {
			// Simplification
			// (A OR B) AND A => A
			if EqualsExpr(or.Left, and.Right) || EqualsExpr(or.Right, and.Right) {
				return and.Right, true
			}
		}
		if or, ok := and.Right.(*OrExpr); ok {
			// Simplification
			// A OR (A AND B) => A
			if EqualsExpr(or.Left, and.Left) || EqualsExpr(or.Right, and.Left) {
				return or.Left, true
			}
		}

	}
	return expr, false
}
