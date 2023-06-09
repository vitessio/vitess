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
	"strings"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sysvars"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	subQueryBaseArgName = []byte("__sq")

	// HasValueSubQueryBaseName is the prefix of each parameter representing an EXISTS subquery
	HasValueSubQueryBaseName = []byte("__sq_has_values")
)

// SQLSelectLimitUnset default value for sql_select_limit not set.
const SQLSelectLimitUnset = -1

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
	sqNext       int64
}

type VSchemaViews interface {
	FindView(name TableName) SelectStatement
}

// ReserveAll tries to reserve all the given variable names. If they're all available,
// they are reserved and the function returns true. Otherwise, the function returns false.
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
	reserveName := col.CompliantName()
	if r.fast && strings.HasPrefix(reserveName, r.prefix) {
		reserveName = "_" + reserveName
	}

	return r.ReserveVariable(reserveName)
}

func (r *ReservedVars) ReserveVariable(compliantName string) string {
	joinVar := []byte(compliantName)
	baseLen := len(joinVar)
	i := int64(1)

	for {
		if _, ok := r.reserved[string(joinVar)]; !ok {
			bvar := string(joinVar)
			r.reserved[bvar] = struct{}{}
			return bvar
		}
		joinVar = strconv.AppendInt(joinVar[:baseLen], i, 10)
		i++
	}
}

// ReserveSubQuery returns the next argument name to replace subquery with pullout value.
func (r *ReservedVars) ReserveSubQuery() string {
	for {
		r.sqNext++
		joinVar := strconv.AppendInt(subQueryBaseArgName, r.sqNext, 10)
		if _, ok := r.reserved[string(joinVar)]; !ok {
			r.reserved[string(joinVar)] = struct{}{}
			return string(joinVar)
		}
	}
}

// ReserveSubQueryWithHasValues returns the next argument name to replace subquery with pullout value.
func (r *ReservedVars) ReserveSubQueryWithHasValues() (string, string) {
	for {
		r.sqNext++
		joinVar := strconv.AppendInt(subQueryBaseArgName, r.sqNext, 10)
		hasValuesJoinVar := strconv.AppendInt(HasValueSubQueryBaseName, r.sqNext, 10)
		_, joinVarOK := r.reserved[string(joinVar)]
		_, hasValuesJoinVarOK := r.reserved[string(hasValuesJoinVar)]
		if !joinVarOK && !hasValuesJoinVarOK {
			r.reserved[string(joinVar)] = struct{}{}
			r.reserved[string(hasValuesJoinVar)] = struct{}{}
			return string(joinVar), string(hasValuesJoinVar)
		}
	}
}

// ReserveHasValuesSubQuery returns the next argument name to replace subquery with has value.
func (r *ReservedVars) ReserveHasValuesSubQuery() string {
	for {
		r.sqNext++
		joinVar := strconv.AppendInt(HasValueSubQueryBaseName, r.sqNext, 10)
		if _, ok := r.reserved[string(joinVar)]; !ok {
			bvar := string(joinVar)
			r.reserved[bvar] = struct{}{}
			return bvar
		}
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
func PrepareAST(
	in Statement,
	reservedVars *ReservedVars,
	bindVars map[string]*querypb.BindVariable,
	parameterize bool,
	keyspace string,
	selectLimit int,
	setVarComment string,
	sysVars map[string]string,
	views VSchemaViews,
) (*RewriteASTResult, error) {
	if parameterize {
		err := Normalize(in, reservedVars, bindVars)
		if err != nil {
			return nil, err
		}
	}
	return RewriteAST(in, keyspace, selectLimit, setVarComment, sysVars, views)
}

// RewriteAST rewrites the whole AST, replacing function calls and adding column aliases to queries.
// SET_VAR comments are also added to the AST if required.
func RewriteAST(
	in Statement,
	keyspace string,
	selectLimit int,
	setVarComment string,
	sysVars map[string]string,
	views VSchemaViews,
) (*RewriteASTResult, error) {
	er := newASTRewriter(keyspace, selectLimit, setVarComment, sysVars, views)
	er.shouldRewriteDatabaseFunc = shouldRewriteDatabaseFunc(in)
	result := SafeRewrite(in, er.rewriteDown, er.rewriteUp)
	if er.err != nil {
		return nil, er.err
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

type astRewriter struct {
	bindVars                  *BindVarNeeds
	shouldRewriteDatabaseFunc bool
	err                       error

	// we need to know this to make a decision if we can safely rewrite JOIN USING => JOIN ON
	hasStarInSelect bool

	keyspace      string
	selectLimit   int
	setVarComment string
	sysVars       map[string]string
	views         VSchemaViews
}

func newASTRewriter(keyspace string, selectLimit int, setVarComment string, sysVars map[string]string, views VSchemaViews) *astRewriter {
	return &astRewriter{
		bindVars:      &BindVarNeeds{},
		keyspace:      keyspace,
		selectLimit:   selectLimit,
		setVarComment: setVarComment,
		sysVars:       sysVars,
		views:         views,
	}
}

const (
	// LastInsertIDName is a reserved bind var name for last_insert_id()
	LastInsertIDName = "__lastInsertId"

	// DBVarName is a reserved bind var name for database()
	DBVarName = "__vtdbname"

	// FoundRowsName is a reserved bind var name for found_rows()
	FoundRowsName = "__vtfrows"

	// RowCountName is a reserved bind var name for row_count()
	RowCountName = "__vtrcount"

	// UserDefinedVariableName is what we prepend bind var names for user defined variables
	UserDefinedVariableName = "__vtudv"
)

func (er *astRewriter) rewriteAliasedExpr(node *AliasedExpr) (*BindVarNeeds, error) {
	inner := newASTRewriter(er.keyspace, er.selectLimit, er.setVarComment, er.sysVars, er.views)
	inner.shouldRewriteDatabaseFunc = er.shouldRewriteDatabaseFunc
	tmp := SafeRewrite(node.Expr, inner.rewriteDown, inner.rewriteUp)
	newExpr, ok := tmp.(Expr)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to rewrite AST. function expected to return Expr returned a %s", String(tmp))
	}
	node.Expr = newExpr
	return inner.bindVars, nil
}

func (er *astRewriter) rewriteDown(node SQLNode, _ SQLNode) bool {
	switch node := node.(type) {
	case *Select:
		er.visitSelect(node)
	case *PrepareStmt, *ExecuteStmt:
		return false // nothing to rewrite here.
	}
	return true
}

func (er *astRewriter) rewriteUp(cursor *Cursor) bool {
	// Add SET_VAR comment to this node if it supports it and is needed
	if supportOptimizerHint, supportsOptimizerHint := cursor.Node().(SupportOptimizerHint); supportsOptimizerHint && er.setVarComment != "" {
		newComments, err := supportOptimizerHint.GetParsedComments().AddQueryHint(er.setVarComment)
		if err != nil {
			er.err = err
			return false
		}
		supportOptimizerHint.SetComments(newComments)
	}

	switch node := cursor.Node().(type) {
	case *Union:
		er.rewriteUnion(node)
	case *FuncExpr:
		er.funcRewrite(cursor, node)
	case *Variable:
		er.rewriteVariable(cursor, node)
	case *Subquery:
		er.unnestSubQueries(cursor, node)
	case *NotExpr:
		er.rewriteNotExpr(cursor, node)
	case *AliasedTableExpr:
		er.rewriteAliasedTable(cursor, node)
	case *ShowBasic:
		er.rewriteShowBasic(node)
	case *ExistsExpr:
		er.existsRewrite(cursor, node)
	}
	return true
}

func (er *astRewriter) rewriteUnion(node *Union) {
	// set select limit if explicitly not set when sql_select_limit is set on the connection.
	if er.selectLimit > 0 && node.Limit == nil {
		node.Limit = &Limit{Rowcount: NewIntLiteral(strconv.Itoa(er.selectLimit))}
	}
}

func (er *astRewriter) rewriteAliasedTable(cursor *Cursor, node *AliasedTableExpr) {
	aliasTableName, ok := node.Expr.(TableName)
	if !ok {
		return
	}

	// Qualifier should not be added to dual table
	tblName := aliasTableName.Name.String()
	if tblName == "dual" {
		return
	}

	if SystemSchema(er.keyspace) {
		if aliasTableName.Qualifier.IsEmpty() {
			aliasTableName.Qualifier = NewIdentifierCS(er.keyspace)
			node.Expr = aliasTableName
			cursor.Replace(node)
		}
		return
	}

	// Could we be dealing with a view?
	if er.views == nil {
		return
	}
	view := er.views.FindView(aliasTableName)
	if view == nil {
		return
	}

	// Aha! It's a view. Let's replace it with a derived table
	node.Expr = &DerivedTable{Select: CloneSelectStatement(view)}
	if node.As.IsEmpty() {
		node.As = NewIdentifierCS(tblName)
	}
}

func (er *astRewriter) rewriteShowBasic(node *ShowBasic) {
	if node.Command == VariableGlobal || node.Command == VariableSession {
		varsToAdd := sysvars.GetInterestingVariables()
		for _, sysVar := range varsToAdd {
			er.bindVars.AddSysVar(sysVar)
		}
	}
}

func (er *astRewriter) rewriteNotExpr(cursor *Cursor, node *NotExpr) {
	switch inner := node.Expr.(type) {
	case *ComparisonExpr:
		// not col = 42 => col != 42
		// not col > 42 => col <= 42
		// etc
		canChange, inverse := inverseOp(inner.Operator)
		if canChange {
			inner.Operator = inverse
			cursor.Replace(inner)
		}
	case *NotExpr:
		// not not true => true
		cursor.Replace(inner.Expr)
	case BoolVal:
		// not true => false
		inner = !inner
		cursor.Replace(inner)
	}
}

func (er *astRewriter) rewriteVariable(cursor *Cursor, node *Variable) {
	// Iff we are in SET, we want to change the scope of variables if a modifier has been set
	// and only on the lhs of the assignment:
	// set session sql_mode = @someElse
	// here we need to change the scope of `sql_mode` and not of `@someElse`
	if v, isSet := cursor.Parent().(*SetExpr); isSet && v.Var == node {
		return
	}
	switch node.Scope {
	case VariableScope:
		er.udvRewrite(cursor, node)
	case GlobalScope, SessionScope, NextTxScope:
		er.sysVarRewrite(cursor, node)
	}
}

func (er *astRewriter) visitSelect(node *Select) {
	for _, col := range node.SelectExprs {
		if _, hasStar := col.(*StarExpr); hasStar {
			er.hasStarInSelect = true
			continue
		}

		aliasedExpr, ok := col.(*AliasedExpr)
		if !ok || !aliasedExpr.As.IsEmpty() {
			continue
		}
		buf := NewTrackedBuffer(nil)
		aliasedExpr.Expr.Format(buf)
		// select last_insert_id() -> select :__lastInsertId as `last_insert_id()`
		innerBindVarNeeds, err := er.rewriteAliasedExpr(aliasedExpr)
		if err != nil {
			er.err = err
			return
		}
		if innerBindVarNeeds.HasRewrites() {
			aliasedExpr.As = NewIdentifierCI(buf.String())
		}
		er.bindVars.MergeWith(innerBindVarNeeds)

	}
	// set select limit if explicitly not set when sql_select_limit is set on the connection.
	if er.selectLimit > 0 && node.Limit == nil {
		node.Limit = &Limit{Rowcount: NewIntLiteral(strconv.Itoa(er.selectLimit))}
	}
}

func inverseOp(i ComparisonExprOperator) (bool, ComparisonExprOperator) {
	switch i {
	case EqualOp:
		return true, NotEqualOp
	case LessThanOp:
		return true, GreaterEqualOp
	case GreaterThanOp:
		return true, LessEqualOp
	case LessEqualOp:
		return true, GreaterThanOp
	case GreaterEqualOp:
		return true, LessThanOp
	case NotEqualOp:
		return true, EqualOp
	case InOp:
		return true, NotInOp
	case NotInOp:
		return true, InOp
	case LikeOp:
		return true, NotLikeOp
	case NotLikeOp:
		return true, LikeOp
	case RegexpOp:
		return true, NotRegexpOp
	case NotRegexpOp:
		return true, RegexpOp
	}

	return false, i
}

func (er *astRewriter) sysVarRewrite(cursor *Cursor, node *Variable) {
	lowered := node.Name.Lowered()

	var found bool
	if er.sysVars != nil {
		_, found = er.sysVars[lowered]
	}

	switch lowered {
	case sysvars.Autocommit.Name,
		sysvars.Charset.Name,
		sysvars.ClientFoundRows.Name,
		sysvars.DDLStrategy.Name,
		sysvars.Names.Name,
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
		sysvars.QueryTimeout.Name,
		sysvars.Workload.Name:
		found = true
	}

	if found {
		cursor.Replace(bindVarExpression("__vt" + lowered))
		er.bindVars.AddSysVar(lowered)
	}
}

func (er *astRewriter) udvRewrite(cursor *Cursor, node *Variable) {
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

func (er *astRewriter) funcRewrite(cursor *Cursor, node *FuncExpr) {
	lowered := node.Name.Lowered()
	if lowered == "last_insert_id" && len(node.Exprs) > 0 {
		// if we are dealing with is LAST_INSERT_ID() with an argument, we don't need to rewrite it.
		// with an argument, this is an identity function that will update the session state and
		// sets the correct fields in the OK TCP packet that we send back
		return
	}
	bindVar, found := funcRewrites[lowered]
	if !found || (bindVar == DBVarName && !er.shouldRewriteDatabaseFunc) {
		return
	}
	if len(node.Exprs) > 0 {
		er.err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Argument to %s() not supported", lowered)
		return
	}
	cursor.Replace(bindVarExpression(bindVar))
	er.bindVars.AddFuncResult(bindVar)
}

func (er *astRewriter) unnestSubQueries(cursor *Cursor, subquery *Subquery) {
	if _, isExists := cursor.Parent().(*ExistsExpr); isExists {
		return
	}
	sel, isSimpleSelect := subquery.Select.(*Select)
	if !isSimpleSelect {
		return
	}

	if len(sel.SelectExprs) != 1 ||
		len(sel.OrderBy) != 0 ||
		len(sel.GroupBy) != 0 ||
		len(sel.From) != 1 ||
		sel.Where != nil ||
		sel.Having != nil ||
		sel.Limit != nil || sel.Lock != NoLock {
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
	_, isColName := expr.Expr.(*ColName)
	if isColName {
		// If we find a single col-name in a `dual` subquery, we can be pretty sure the user is returning a column
		// already projected.
		// `select 1 as x, (select x)`
		// is perfectly valid - any aliased columns to the left are available inside subquery scopes
		return
	}
	er.bindVars.NoteRewrite()
	// we need to make sure that the inner expression also gets rewritten,
	// so we fire off another rewriter traversal here
	rewritten := SafeRewrite(expr.Expr, er.rewriteDown, er.rewriteUp)

	// Here we need to handle the subquery rewrite in case in occurs in an IN clause
	// For example, SELECT id FROM user WHERE id IN (SELECT 1 FROM DUAL)
	// Here we cannot rewrite the query to SELECT id FROM user WHERE id IN 1, since that is syntactically wrong
	// We must rewrite it to SELECT id FROM user WHERE id IN (1)
	// Find more cases in the test file
	rewrittenExpr, isExpr := rewritten.(Expr)
	_, isColTuple := rewritten.(ColTuple)
	comparisonExpr, isCompExpr := cursor.Parent().(*ComparisonExpr)
	// Check that the parent is a comparison operator with IN or NOT IN operation.
	// Also, if rewritten is already a ColTuple (like a subquery), then we do not need this
	// We also need to check that rewritten is an Expr, if it is then we can rewrite it as a ValTuple
	if isCompExpr && (comparisonExpr.Operator == InOp || comparisonExpr.Operator == NotInOp) && !isColTuple && isExpr {
		cursor.Replace(ValTuple{rewrittenExpr})
		return
	}

	cursor.Replace(rewritten)
}

func (er *astRewriter) existsRewrite(cursor *Cursor, node *ExistsExpr) {
	sel, ok := node.Subquery.Select.(*Select)
	if !ok {
		return
	}

	if sel.Limit == nil {
		sel.Limit = &Limit{}
	}
	sel.Limit.Rowcount = NewIntLiteral("1")

	if sel.Having != nil {
		// If the query has HAVING, we can't take any shortcuts
		return
	}

	if len(sel.GroupBy) == 0 && sel.SelectExprs.AllAggregation() {
		// in these situations, we are guaranteed to always get a non-empty result,
		// so we can replace the EXISTS with a literal true
		cursor.Replace(BoolVal(true))
	}

	// If we are not doing HAVING, we can safely replace all select expressions with a
	// single `1` and remove any grouping
	sel.SelectExprs = SelectExprs{
		&AliasedExpr{Expr: NewIntLiteral("1")},
	}
	sel.GroupBy = nil
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
