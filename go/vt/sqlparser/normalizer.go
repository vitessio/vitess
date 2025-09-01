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

package sqlparser

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sysvars"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// BindVars represents a set of reserved bind variables extracted from a SQL statement.
	BindVars map[string]struct{}
	// normalizer transforms SQL statements to support parameterization and streamline query planning.
	//
	// It serves two primary purposes:
	//  1. **Parameterization:** Allows multiple invocations of the same query with different literals by converting literals
	//     to bind variables. This enables efficient reuse of execution plans with varying parameters.
	//  2. **Simplified Planning:** Reduces the complexity for the query planner by standardizing SQL patterns. For example,
	//     it ensures that table columns are consistently placed on the left side of comparison expressions. This uniformity
	//     minimizes the number of distinct patterns the planner must handle, enhancing planning efficiency.
	normalizer struct {
		bindVars  map[string]*querypb.BindVariable
		reserved  *ReservedVars
		vals      map[Literal]string
		tupleVals map[string]string
		err       error
		inDerived int
		inSelect  int

		bindVarNeeds              *BindVarNeeds
		shouldRewriteDatabaseFunc bool
		hasStarInSelect           bool

		keyspace      string
		selectLimit   int
		setVarComment string
		fkChecksState *bool
		sysVars       map[string]string
		views         VSchemaViews

		onLeave      map[*AliasedExpr]func(*AliasedExpr)
		parameterize bool
		useASTQuery  bool
	}
	// RewriteASTResult holds the result of rewriting the AST, including bind variable needs.
	RewriteASTResult struct {
		*BindVarNeeds
		AST                Statement // The rewritten AST
		UpdateQueryFromAST bool
	}
	// VSchemaViews provides access to view definitions within the VSchema.
	VSchemaViews interface {
		FindView(name TableName) TableStatement
	}
)

const (
	// SQLSelectLimitUnset indicates that sql_select_limit is not set.
	SQLSelectLimitUnset = -1
	// LastInsertIDName is the bind variable name for LAST_INSERT_ID().
	LastInsertIDName = "__lastInsertId"
	// DBVarName is the bind variable name for DATABASE().
	DBVarName = "__vtdbname"
	// FoundRowsName is the bind variable name for FOUND_ROWS().
	FoundRowsName = "__vtfrows"
	// RowCountName is the bind variable name for ROW_COUNT().
	RowCountName = "__vtrcount"
	// UserDefinedVariableName is the prefix for user-defined variable bind names.
	UserDefinedVariableName = "__vtudv"
)

// funcRewrites lists all functions that must be rewritten. we don't want these to make it down to mysql,
// we need to handle these in the vtgate
var funcRewrites = map[string]string{
	"last_insert_id": LastInsertIDName,
	"database":       DBVarName,
	"schema":         DBVarName,
	"found_rows":     FoundRowsName,
	"row_count":      RowCountName,
}

// Normalize normalizes the input SQL statement and returns the rewritten AST along with bind variable information.
func Normalize(
	in Statement,
	reservedVars *ReservedVars,
	bindVars map[string]*querypb.BindVariable,
	parameterize bool,
	keyspace string,
	selectLimit int,
	setVarComment string,
	sysVars map[string]string,
	fkChecksState *bool,
	views VSchemaViews,
) (*RewriteASTResult, error) {
	nz := newNormalizer(reservedVars, bindVars, keyspace, selectLimit, setVarComment, sysVars, fkChecksState, views, parameterize)
	nz.shouldRewriteDatabaseFunc = shouldRewriteDatabaseFunc(in)
	nz.determineQueryRewriteStrategy(in)

	out := SafeRewrite(in, nz.walkDown, nz.walkUp)
	if nz.err != nil {
		return nil, nz.err
	}

	return &RewriteASTResult{
		AST:                out.(Statement),
		BindVarNeeds:       nz.bindVarNeeds,
		UpdateQueryFromAST: nz.useASTQuery,
	}, nil
}

func newNormalizer(
	reserved *ReservedVars,
	bindVars map[string]*querypb.BindVariable,
	keyspace string,
	selectLimit int,
	setVarComment string,
	sysVars map[string]string,
	fkChecksState *bool,
	views VSchemaViews,
	parameterize bool,
) *normalizer {
	return &normalizer{
		bindVars:      bindVars,
		reserved:      reserved,
		vals:          make(map[Literal]string),
		tupleVals:     make(map[string]string),
		bindVarNeeds:  &BindVarNeeds{},
		keyspace:      keyspace,
		selectLimit:   selectLimit,
		setVarComment: setVarComment,
		fkChecksState: fkChecksState,
		sysVars:       sysVars,
		views:         views,
		onLeave:       make(map[*AliasedExpr]func(*AliasedExpr)),
		parameterize:  parameterize,
	}
}

func (nz *normalizer) determineQueryRewriteStrategy(in Statement) {
	switch in.(type) {
	case *Select, *Union, *Insert, *Update, *Delete, *CallProc, *Stream, *VExplainStmt:
		nz.useASTQuery = true
	case *Set:
		nz.useASTQuery = true
		nz.parameterize = false
	default:
		nz.parameterize = false
	}
}

// walkDown processes nodes when traversing down the AST.
// It handles normalization logic based on node types.
func (nz *normalizer) walkDown(node, _ SQLNode) bool {
	switch node := node.(type) {
	case *Begin, *Commit, *Rollback, *Savepoint, *SRollback, *Release, *OtherAdmin, *Analyze,
		*PrepareStmt, *ExecuteStmt, *FramePoint, *ColName, TableName, *ConvertType, *CreateProcedure:
		// These statement do not need normalizing
		return false
	case *AssignmentExpr:
		nz.err = vterrors.VT12001("Assignment expression")
		return false
	case *DerivedTable:
		nz.inDerived++
	case *Select:
		nz.inSelect++
		if nz.selectLimit > 0 && node.Limit == nil {
			node.Limit = &Limit{Rowcount: NewIntLiteral(strconv.Itoa(nz.selectLimit))}
		}
	case *AliasedExpr:
		nz.noteAliasedExprName(node)
	case *ComparisonExpr:
		nz.convertComparison(node)
	case *UpdateExpr:
		nz.convertUpdateExpr(node)
	case *StarExpr:
		nz.hasStarInSelect = true
		// No rewriting needed for prepare or execute statements.
		return false
	case *ShowBasic:
		if node.Command != VariableGlobal && node.Command != VariableSession {
			break
		}
		varsToAdd := sysvars.GetInterestingVariables()
		for _, sysVar := range varsToAdd {
			nz.bindVarNeeds.AddSysVar(sysVar)
		}
	}
	b := nz.err == nil
	if !b {
		fmt.Println(1)
	}
	return b
}

// noteAliasedExprName tracks expressions without aliases to add alias if expression is rewritten
func (nz *normalizer) noteAliasedExprName(node *AliasedExpr) {
	if node.As.NotEmpty() {
		return
	}
	buf := NewTrackedBuffer(nil)
	node.Expr.Format(buf)
	rewrites := nz.bindVarNeeds.NumberOfRewrites()
	nz.onLeave[node] = func(newAliasedExpr *AliasedExpr) {
		if nz.bindVarNeeds.NumberOfRewrites() > rewrites {
			newAliasedExpr.As = NewIdentifierCI(buf.String())
		}
	}
}

// walkUp processes nodes when traversing up the AST.
// It finalizes normalization logic based on node types.
func (nz *normalizer) walkUp(cursor *Cursor) bool {
	// Add SET_VAR comments if applicable.
	if stmt, supports := cursor.Node().(SupportOptimizerHint); supports {
		if nz.setVarComment != "" {
			newComments, err := stmt.GetParsedComments().AddQueryHint(nz.setVarComment)
			if err != nil {
				nz.err = err
				return false
			}
			stmt.SetComments(newComments)
			nz.useASTQuery = true
		}

		// use foreign key checks of normalizer and set the query hint in the query.
		if nz.fkChecksState != nil {
			newComments := stmt.GetParsedComments().SetMySQLSetVarValue(sysvars.ForeignKeyChecks, FkChecksStateString(nz.fkChecksState))
			stmt.SetComments(newComments)
			nz.useASTQuery = true
		}
	}

	if nz.err != nil {
		return false
	}

	switch node := cursor.node.(type) {
	case *DerivedTable:
		nz.inDerived--
	case *Select:
		nz.inSelect--
	case *AliasedExpr:
		// if we are tracking this node for changes, this is the time to add the alias if needed
		if onLeave, ok := nz.onLeave[node]; ok {
			onLeave(node)
			delete(nz.onLeave, node)
		}
	case *Union:
		nz.rewriteUnion(node)
	case *FuncExpr:
		nz.funcRewrite(cursor, node)
	case *Variable:
		nz.rewriteVariable(cursor, node)
	case *Subquery:
		nz.unnestSubQueries(cursor, node)
	case *NotExpr:
		nz.rewriteNotExpr(cursor, node)
	case *AliasedTableExpr:
		nz.rewriteAliasedTable(cursor, node)
	case *ShowBasic:
		nz.rewriteShowBasic(node)
	case *ExistsExpr:
		nz.existsRewrite(cursor, node)
	case DistinctableAggr:
		nz.rewriteDistinctableAggr(node)
	case *Literal:
		nz.visitLiteral(cursor, node)
	}
	return nz.err == nil
}

func (nz *normalizer) visitLiteral(cursor *Cursor, node *Literal) {
	if !nz.shouldParameterize() {
		return
	}
	if nz.inSelect == 0 {
		nz.convertLiteral(node, cursor)
		return
	}
	switch cursor.Parent().(type) {
	case *Order, *GroupBy:
		return
	case *Limit:
		nz.convertLiteral(node, cursor)
	default:
		nz.convertLiteralDedup(node, cursor)
	}
}

// validateLiteral ensures that a Literal node has a valid value based on its type.
func validateLiteral(node *Literal) error {
	switch node.Type {
	case DateVal:
		if _, ok := datetime.ParseDate(node.Val); !ok {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Incorrect DATE value: '%s'", node.Val)
		}
	case TimeVal:
		if _, _, state := datetime.ParseTime(node.Val, -1); state != datetime.TimeOK {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Incorrect TIME value: '%s'", node.Val)
		}
	case TimestampVal:
		if _, _, ok := datetime.ParseDateTime(node.Val, -1); !ok {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Incorrect DATETIME value: '%s'", node.Val)
		}
	}
	return nil
}

// convertLiteralDedup converts a Literal node to a bind variable with deduplication.
func (nz *normalizer) convertLiteralDedup(node *Literal, cursor *Cursor) {
	if err := validateLiteral(node); err != nil {
		nz.err = err
		return
	}

	// Skip deduplication for long values.
	if len(node.Val) > 256 {
		nz.convertLiteral(node, cursor)
		return
	}

	bval := literalToBindvar(node)
	if bval == nil {
		return
	}

	bvname, exists := nz.vals[*node]
	if !exists {
		bvname = nz.reserved.nextUnusedVar()
		nz.vals[*node] = bvname
		nz.bindVars[bvname] = bval
	}

	arg, err := NewTypedArgumentFromLiteral(bvname, node)
	if err != nil {
		nz.err = err
		return
	}
	cursor.Replace(arg)
}

// convertLiteral converts a Literal node to a bind variable without deduplication.
func (nz *normalizer) convertLiteral(node *Literal, cursor *Cursor) {
	if err := validateLiteral(node); err != nil {
		nz.err = err
		return
	}

	bval := literalToBindvar(node)
	if bval == nil {
		return
	}

	bvname := nz.reserved.nextUnusedVar()
	nz.bindVars[bvname] = bval
	arg, err := NewTypedArgumentFromLiteral(bvname, node)
	if err != nil {
		nz.err = err
		return
	}
	cursor.Replace(arg)
}

// convertComparison handles the conversion of comparison expressions to use bind variables.
func (nz *normalizer) convertComparison(node *ComparisonExpr) {
	switch node.Operator {
	case InOp, NotInOp:
		nz.rewriteInComparisons(node)
	default:
		nz.rewriteOtherComparisons(node)
	}
}

// rewriteOtherComparisons parameterizes non-IN comparison expressions.
func (nz *normalizer) rewriteOtherComparisons(node *ComparisonExpr) {
	newR := nz.normalizeComparisonWithBindVar(node.Left, node.Right)
	if newR != nil {
		node.Right = newR
	}
}

// normalizeComparisonWithBindVar attempts to replace a literal in a comparison with a bind variable.
func (nz *normalizer) normalizeComparisonWithBindVar(left, right Expr) Expr {
	if !nz.shouldParameterize() {
		return nil
	}
	col, ok := left.(*ColName)
	if !ok {
		return nil
	}
	lit, ok := right.(*Literal)
	if !ok {
		return nil
	}
	if err := validateLiteral(lit); err != nil {
		nz.err = err
		return nil
	}

	bval := literalToBindvar(lit)
	if bval == nil {
		return nil
	}
	bvname := nz.decideBindVarName(lit, col, bval)
	arg, err := NewTypedArgumentFromLiteral(bvname, lit)
	if err != nil {
		nz.err = err
		return nil
	}
	return arg
}

// decideBindVarName determines the appropriate bind variable name for a given literal and column.
func (nz *normalizer) decideBindVarName(lit *Literal, col *ColName, bval *querypb.BindVariable) string {
	if len(lit.Val) <= 256 {
		if bvname, ok := nz.vals[*lit]; ok {
			return bvname
		}
	}

	bvname := nz.reserved.ReserveColName(col)
	nz.vals[*lit] = bvname
	nz.bindVars[bvname] = bval

	return bvname
}

// rewriteInComparisons converts IN and NOT IN expressions to use list bind variables.
func (nz *normalizer) rewriteInComparisons(node *ComparisonExpr) {
	if !nz.shouldParameterize() {
		return
	}
	tupleVals, ok := node.Right.(ValTuple)
	if !ok {
		return
	}

	// Create a list bind variable for the tuple.
	bvals := &querypb.BindVariable{
		Type: querypb.Type_TUPLE,
	}
	for _, val := range tupleVals {
		bval := literalToBindvar(val)
		if bval == nil {
			return
		}
		bvals.Values = append(bvals.Values, &querypb.Value{
			Type:  bval.Type,
			Value: bval.Value,
		})
	}

	var bvname string

	if key, err := bvals.MarshalVT(); err != nil {
		bvname = nz.reserved.nextUnusedVar()
		nz.bindVars[bvname] = bvals
	} else {
		// Check if we can find key in tuplevals
		if bvname, ok = nz.tupleVals[string(key)]; !ok {
			bvname = nz.reserved.nextUnusedVar()
		}

		nz.bindVars[bvname] = bvals
		nz.tupleVals[string(key)] = bvname
	}

	node.Right = ListArg(bvname)
}

// convertUpdateExpr parameterizes expressions in UPDATE statements.
func (nz *normalizer) convertUpdateExpr(node *UpdateExpr) {
	newR := nz.normalizeComparisonWithBindVar(node.Name, node.Expr)
	if newR != nil {
		node.Expr = newR
	}
}

// literalToBindvar converts a SQLNode to a BindVariable if possible.
func literalToBindvar(node SQLNode) *querypb.BindVariable {
	lit, ok := node.(*Literal)
	if !ok {
		return nil
	}
	var v sqltypes.Value
	var err error
	switch lit.Type {
	case StrVal:
		v, err = sqltypes.NewValue(sqltypes.VarChar, lit.Bytes())
	case IntVal:
		v, err = sqltypes.NewValue(sqltypes.Int64, lit.Bytes())
	case FloatVal:
		v, err = sqltypes.NewValue(sqltypes.Float64, lit.Bytes())
	case DecimalVal:
		v, err = sqltypes.NewValue(sqltypes.Decimal, lit.Bytes())
	case HexNum:
		buf := make([]byte, 0, len(lit.Bytes()))
		buf = append(buf, "0x"...)
		buf = append(buf, bytes.ToUpper(lit.Bytes()[2:])...)
		v, err = sqltypes.NewValue(sqltypes.HexNum, buf)
	case HexVal:
		// Re-encode hex string literals to original MySQL format.
		buf := make([]byte, 0, len(lit.Bytes())+3)
		buf = append(buf, 'x', '\'')
		buf = append(buf, bytes.ToUpper(lit.Bytes())...)
		buf = append(buf, '\'')
		v, err = sqltypes.NewValue(sqltypes.HexVal, buf)
	case BitNum:
		out := make([]byte, 0, len(lit.Bytes())+2)
		out = append(out, '0', 'b')
		out = append(out, lit.Bytes()[2:]...)
		v, err = sqltypes.NewValue(sqltypes.BitNum, out)
	case DateVal:
		v, err = sqltypes.NewValue(sqltypes.Date, lit.Bytes())
	case TimeVal:
		v, err = sqltypes.NewValue(sqltypes.Time, lit.Bytes())
	case TimestampVal:
		// Use DATETIME type for TIMESTAMP literals.
		v, err = sqltypes.NewValue(sqltypes.Datetime, lit.Bytes())
	default:
		return nil
	}
	if err != nil {
		return nil
	}
	return sqltypes.ValueBindVariable(v)
}

// getBindvars extracts bind variables from a SQL statement.
func getBindvars(stmt Statement) map[string]struct{} {
	bindvars := make(map[string]struct{})
	_ = Walk(func(node SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *ColName, TableName:
			// These node types do not contain expressions.
			return false, nil
		case *Argument:
			bindvars[node.Name] = struct{}{}
		case ListArg:
			bindvars[string(node)] = struct{}{}
		}
		return true, nil
	}, stmt)
	return bindvars
}

var HasValueSubQueryBaseName = []byte("__sq_has_values")

// shouldRewriteDatabaseFunc determines if the database function should be rewritten based on the statement.
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

// rewriteUnion sets the SELECT limit for UNION statements if not already set.
func (nz *normalizer) rewriteUnion(node *Union) {
	if nz.selectLimit > 0 && node.Limit == nil {
		node.Limit = &Limit{Rowcount: NewIntLiteral(strconv.Itoa(nz.selectLimit))}
	}
}

// rewriteAliasedTable handles the rewriting of aliased tables, including view substitutions.
func (nz *normalizer) rewriteAliasedTable(cursor *Cursor, node *AliasedTableExpr) {
	aliasTableName, ok := node.Expr.(TableName)
	if !ok {
		return
	}

	// Do not add qualifiers to the dual table.
	tblName := aliasTableName.Name.String()
	if tblName == "dual" {
		return
	}

	if SystemSchema(nz.keyspace) {
		if aliasTableName.Qualifier.IsEmpty() {
			aliasTableName.Qualifier = NewIdentifierCS(nz.keyspace)
			node.Expr = aliasTableName
			cursor.Replace(node)
		}
		return
	}

	// Replace views with their underlying definitions.
	if nz.views == nil {
		return
	}
	view := nz.views.FindView(aliasTableName)
	if view == nil {
		return
	}

	// Substitute the view with a derived table.
	node.Expr = &DerivedTable{Select: Clone(view)}
	if node.As.IsEmpty() {
		node.As = NewIdentifierCS(tblName)
	}
}

// rewriteShowBasic handles the rewriting of SHOW statements, particularly for system variables.
func (nz *normalizer) rewriteShowBasic(node *ShowBasic) {
	if node.Command == VariableGlobal || node.Command == VariableSession {
		varsToAdd := sysvars.GetInterestingVariables()
		for _, sysVar := range varsToAdd {
			nz.bindVarNeeds.AddSysVar(sysVar)
		}
	}
}

// rewriteNotExpr simplifies NOT expressions where possible.
func (nz *normalizer) rewriteNotExpr(cursor *Cursor, node *NotExpr) {
	switch inner := node.Expr.(type) {
	case *ComparisonExpr:
		// Invert comparison operators.
		if canChange, inverse := inverseOp(inner.Operator); canChange {
			inner.Operator = inverse
			cursor.Replace(inner)
		}
	case *NotExpr:
		// Simplify double negation.
		cursor.Replace(inner.Expr)
	case BoolVal:
		// Negate boolean values.
		cursor.Replace(!inner)
	}
}

// rewriteVariable handles the rewriting of variable expressions to bind variables.
func (nz *normalizer) rewriteVariable(cursor *Cursor, node *Variable) {
	// Only rewrite scope for variables on the left side of SET assignments.
	if v, isSet := cursor.Parent().(*SetExpr); isSet && v.Var == node {
		if node.Scope == NoScope {
			// We rewrite the NoScope to session scope for SET statements
			// that we plan. Previously we used to do this during parsing itself,
			// but we needed to change that to allow for set statements in a
			// create procedure statement that sometimes set locally defined variables
			// that aren't in the session scope.
			node.Scope = SessionScope
		}
		return
	}
	switch node.Scope {
	case VariableScope:
		nz.udvRewrite(cursor, node)
	case SessionScope, NextTxScope, NoScope:
		nz.sysVarRewrite(cursor, node)
	}
}

// inverseOp returns the inverse operator for a given comparison operator.
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

// sysVarRewrite replaces system variables with corresponding bind variables.
func (nz *normalizer) sysVarRewrite(cursor *Cursor, node *Variable) {
	lowered := node.Name.Lowered()

	var found bool
	if nz.sysVars != nil {
		_, found = nz.sysVars[lowered]
	}

	switch lowered {
	case sysvars.Autocommit.Name,
		sysvars.Charset.Name,
		sysvars.ClientFoundRows.Name,
		sysvars.DDLStrategy.Name,
		sysvars.MigrationContext.Name,
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
		sysvars.TransactionTimeout.Name,
		sysvars.Workload.Name:
		found = true
	}

	if found {
		cursor.Replace(NewArgument("__vt" + lowered))
		nz.bindVarNeeds.AddSysVar(lowered)
	}
}

// udvRewrite replaces user-defined variables with corresponding bind variables.
func (nz *normalizer) udvRewrite(cursor *Cursor, node *Variable) {
	udv := strings.ToLower(node.Name.CompliantName())
	cursor.Replace(NewArgument(UserDefinedVariableName + udv))
	nz.bindVarNeeds.AddUserDefVar(udv)
}

// funcRewrite replaces certain function expressions with bind variables.
func (nz *normalizer) funcRewrite(cursor *Cursor, node *FuncExpr) {
	lowered := node.Name.Lowered()
	if lowered == "last_insert_id" && len(node.Exprs) > 0 {
		// Do not rewrite LAST_INSERT_ID() when it has arguments.
		return
	}
	bindVar, found := funcRewrites[lowered]
	if !found || (bindVar == DBVarName && !nz.shouldRewriteDatabaseFunc) {
		return
	}
	if len(node.Exprs) > 0 {
		nz.err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Argument to %s() not supported", lowered)
		return
	}
	cursor.Replace(NewArgument(bindVar))
	nz.bindVarNeeds.AddFuncResult(bindVar)
}

// unnestSubQueries attempts to simplify dual subqueries where possible.
// select (select database() from dual) from test
// =>
// select database() from test
func (nz *normalizer) unnestSubQueries(cursor *Cursor, subquery *Subquery) {
	if _, isExists := cursor.Parent().(*ExistsExpr); isExists {
		return
	}
	sel, isSimpleSelect := subquery.Select.(*Select)
	if !isSimpleSelect {
		return
	}

	if len(sel.SelectExprs.Exprs) != 1 ||
		len(sel.OrderBy) != 0 ||
		sel.GroupBy != nil ||
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
	expr, ok := sel.SelectExprs.Exprs[0].(*AliasedExpr)
	if !ok {
		return
	}
	_, isColName := expr.Expr.(*ColName)
	if isColName {
		// Skip if the subquery already returns a column name.
		return
	}
	nz.bindVarNeeds.NoteRewrite()
	rewritten := SafeRewrite(expr.Expr, nz.walkDown, nz.walkUp)

	// Handle special cases for IN clauses.
	rewrittenExpr, isExpr := rewritten.(Expr)
	_, isColTuple := rewritten.(ColTuple)
	comparisonExpr, isCompExpr := cursor.Parent().(*ComparisonExpr)
	if isCompExpr && (comparisonExpr.Operator == InOp || comparisonExpr.Operator == NotInOp) && !isColTuple && isExpr {
		cursor.Replace(ValTuple{rewrittenExpr})
		return
	}

	cursor.Replace(rewritten)
}

// existsRewrite optimizes EXISTS expressions where possible.
func (nz *normalizer) existsRewrite(cursor *Cursor, node *ExistsExpr) {
	sel, ok := node.Subquery.Select.(*Select)
	if !ok {
		return
	}

	if sel.Having != nil {
		// Cannot optimize if HAVING clause is present.
		return
	}

	if sel.GroupBy == nil && sel.SelectExprs.AllAggregation() {
		// Replace EXISTS with a boolean true if guaranteed to be non-empty.
		cursor.Replace(BoolVal(true))
		return
	}

	// Simplify the subquery by selecting a constant.
	// WHERE EXISTS(SELECT 1 FROM ...)
	sel.SelectExprs = &SelectExprs{
		Exprs: []SelectExpr{&AliasedExpr{Expr: NewIntLiteral("1")}},
	}
	sel.GroupBy = nil
}

// rewriteDistinctableAggr removes DISTINCT from certain aggregations to simplify the plan.
func (nz *normalizer) rewriteDistinctableAggr(node DistinctableAggr) {
	if !node.IsDistinct() {
		return
	}
	switch aggr := node.(type) {
	case *Max, *Min:
		aggr.SetDistinct(false)
		nz.bindVarNeeds.NoteRewrite()
	}
}

func (nz *normalizer) shouldParameterize() bool {
	return !(nz.inDerived > 0 && len(nz.onLeave) > 0) && nz.parameterize
}

// SystemSchema checks if the given schema is a system schema.
func SystemSchema(schema string) bool {
	return strings.EqualFold(schema, "information_schema") ||
		strings.EqualFold(schema, "performance_schema") ||
		strings.EqualFold(schema, "sys") ||
		strings.EqualFold(schema, "mysql")
}
