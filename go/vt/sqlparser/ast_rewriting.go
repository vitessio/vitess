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
	"strconv"

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

// ReservedVars keeps track of the bind variable names that have already been used
// in a parsed query.
type ReservedVars struct {
	prefix       string
	reserved     BindVars
	next         []byte
	counter      int
	fast, static bool
}

// ReserveAll tries to reserve all the given variable names. If they're all available,
// they are reserved and the function returns true. Otherwise the function returns false.
func (r *ReservedVars) ReserveAll(names ...string) bool {
	for _, name := range names {
		if _, ok := r.reserved[name]; ok {
			return false
		}
	}
	for _, name := range names {
		r.reserved[name] = struct{}{}
	}
	return true
}

// ReserveColName reserves a variable name for the given column; if a variable
// with the same name already exists, it'll be suffixed with a numberic identifier
// to make it unique.
func (r *ReservedVars) ReserveColName(col *ColName) string {
	compliantName := col.CompliantName()
	if r.fast && strings.HasPrefix(compliantName, r.prefix) {
		compliantName = "_" + compliantName
	}

	joinVar := []byte(compliantName)
	baseLen := len(joinVar)
	i := int64(1)

	for {
		if _, ok := r.reserved[string(joinVar)]; !ok {
			return string(joinVar)
		}
		joinVar = strconv.AppendInt(joinVar[:baseLen], i, 10)
		i++
	}
}

const staticBvar10 = "vtg0vtg1vtg2vtg3vtg4vtg5vtg6vtg7vtg8vtg9"
const staticBvar100 = "vtg10vtg11vtg12vtg13vtg14vtg15vtg16vtg17vtg18vtg19vtg20vtg21vtg22vtg23vtg24vtg25vtg26vtg27vtg28vtg29vtg30vtg31vtg32vtg33vtg34vtg35vtg36vtg37vtg38vtg39vtg40vtg41vtg42vtg43vtg44vtg45vtg46vtg47vtg48vtg49vtg50vtg51vtg52vtg53vtg54vtg55vtg56vtg57vtg58vtg59vtg60vtg61vtg62vtg63vtg64vtg65vtg66vtg67vtg68vtg69vtg70vtg71vtg72vtg73vtg74vtg75vtg76vtg77vtg78vtg79vtg80vtg81vtg82vtg83vtg84vtg85vtg86vtg87vtg88vtg89vtg90vtg91vtg92vtg93vtg94vtg95vtg96vtg97vtg98vtg99"

func (r *ReservedVars) nextUnusedVar() string {
	if r.fast {
		r.counter++

		if r.static {
			switch {
			case r.counter < 10:
				ofs := r.counter * 4
				return staticBvar10[ofs : ofs+4]
			case r.counter < 100:
				ofs := (r.counter - 10) * 5
				return staticBvar100[ofs : ofs+5]
			}
		}

		r.next = strconv.AppendInt(r.next[:len(r.prefix)], int64(r.counter), 10)
		return string(r.next)
	}

	for {
		r.counter++
		r.next = strconv.AppendInt(r.next[:len(r.prefix)], int64(r.counter), 10)

		if _, ok := r.reserved[string(r.next)]; !ok {
			bvar := string(r.next)
			r.reserved[bvar] = struct{}{}
			return bvar
		}
	}
}

// NewReservedVars allocates a ReservedVar instance that will generate unique
// variable names starting with the given `prefix` and making sure that they
// don't conflict with the given set of `known` variables.
func NewReservedVars(prefix string, known BindVars) *ReservedVars {
	rv := &ReservedVars{
		prefix:   prefix,
		counter:  0,
		reserved: known,
		fast:     true,
		next:     []byte(prefix),
	}

	if prefix != "" && prefix[0] == '_' {
		panic("cannot reserve variables with a '_' prefix")
	}

	for bvar := range known {
		if strings.HasPrefix(bvar, prefix) {
			rv.fast = false
			break
		}
	}

	if prefix == "vtg" {
		rv.static = true
	}
	return rv
}

// PrepareAST will normalize the query
func PrepareAST(in Statement, reservedVars *ReservedVars, bindVars map[string]*querypb.BindVariable, parameterize bool, keyspace string) (*RewriteASTResult, error) {
	if parameterize {
		err := Normalize(in, reservedVars, bindVars)
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
	return NewArgument(name)
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
