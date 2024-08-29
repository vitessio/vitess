/*
Copyright 2022 The Vitess Authors.

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

package plancontext

import (
	"io"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type PlanningContext struct {
	ReservedVars *sqlparser.ReservedVars
	SemTable     *semantics.SemTable
	VSchema      VSchema

	// joinPredicates maps each original join predicate (key) to a slice of
	// variations of the RHS predicates (value). This map is used to handle
	// different scenarios in join planning, where the RHS predicates are
	// modified to accommodate dependencies from the LHS, represented as Arguments.
	joinPredicates map[sqlparser.Expr][]sqlparser.Expr

	// skipPredicates tracks predicates that should be skipped, typically when
	// a join predicate is reverted to its original form during planning.
	skipPredicates map[sqlparser.Expr]any

	PlannerVersion querypb.ExecuteOptions_PlannerVersion

	// If we during planning have turned this expression into an argument name,
	// we can continue using the same argument name
	ReservedArguments map[sqlparser.Expr]string

	// VerifyAllFKs tells whether we need verification for all the fk constraints on VTGate.
	// This is required for queries we are running with /*+ SET_VAR(foreign_key_checks=OFF) */
	VerifyAllFKs bool

	// Projected subqueries that have been merged
	MergedSubqueries []*sqlparser.Subquery

	// CurrentPhase keeps track of how far we've gone in the planning process
	// The type should be operators.Phase, but depending on that would lead to circular dependencies
	CurrentPhase int

	// Statement contains the originally parsed statement
	Statement sqlparser.Statement

	// OuterTables contains the tables that are outer to the current query
	// Used to set the nullable flag on the columns
	OuterTables semantics.TableSet

	// This is a stack of CTEs being built. It's used when we have CTEs inside CTEs,
	// to remember which is the CTE currently being assembled
	CurrentCTE []*ContextCTE

	// mirror contains a mirrored clone of this planning context.
	mirror *PlanningContext

	// isMirrored indicates that mirrored tables should be used.
	isMirrored bool
}

// CreatePlanningContext initializes a new PlanningContext with the given parameters.
// It analyzes the SQL statement within the given virtual schema context,
// handling default keyspace settings and semantic analysis.
// Returns an error if semantic analysis fails.
func CreatePlanningContext(stmt sqlparser.Statement,
	reservedVars *sqlparser.ReservedVars,
	vschema VSchema,
	version querypb.ExecuteOptions_PlannerVersion,
) (*PlanningContext, error) {
	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}

	semTable, err := semantics.Analyze(stmt, ksName, vschema)
	if err != nil {
		return nil, err
	}

	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)

	return &PlanningContext{
		ReservedVars:      reservedVars,
		SemTable:          semTable,
		VSchema:           vschema,
		joinPredicates:    map[sqlparser.Expr][]sqlparser.Expr{},
		skipPredicates:    map[sqlparser.Expr]any{},
		PlannerVersion:    version,
		ReservedArguments: map[sqlparser.Expr]string{},
		Statement:         stmt,
	}, nil
}

// GetReservedArgumentFor retrieves a reserved argument name for a given expression.
// If the expression already has a reserved argument, it returns that name;
// otherwise, it reserves a new name based on the expression type.
func (ctx *PlanningContext) GetReservedArgumentFor(expr sqlparser.Expr) string {
	for key, name := range ctx.ReservedArguments {
		if ctx.SemTable.EqualsExpr(key, expr) {
			return name
		}
	}
	var bvName string
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		bvName = ctx.ReservedVars.ReserveColName(expr)
	case *sqlparser.Subquery:
		bvName = ctx.ReservedVars.ReserveSubQuery()
	default:
		bvName = ctx.ReservedVars.ReserveVariable(sqlparser.CompliantString(expr))
	}
	ctx.ReservedArguments[expr] = bvName

	return bvName
}

// ShouldSkip determines if a given expression should be ignored in the SQL output building.
// It checks against expressions that have been marked to be excluded from further processing.
func (ctx *PlanningContext) ShouldSkip(expr sqlparser.Expr) bool {
	for k := range ctx.skipPredicates {
		if ctx.SemTable.EqualsExpr(expr, k) {
			return true
		}
	}
	return false
}

// AddJoinPredicates associates additional RHS predicates with an existing join predicate.
// This is used to dynamically adjust the RHS predicates based on evolving join conditions.
func (ctx *PlanningContext) AddJoinPredicates(joinPred sqlparser.Expr, predicates ...sqlparser.Expr) {
	fn := func(original sqlparser.Expr, rhsExprs []sqlparser.Expr) {
		ctx.joinPredicates[original] = append(rhsExprs, predicates...)
	}
	if ctx.execOnJoinPredicateEqual(joinPred, fn) {
		return
	}

	// we didn't find an existing entry
	ctx.joinPredicates[joinPred] = predicates
}

// SkipJoinPredicates marks the predicates related to a specific join predicate as irrelevant
// for the current planning stage. This is used when a join has been pushed under a route and
// the original predicate will be used.
func (ctx *PlanningContext) SkipJoinPredicates(joinPred sqlparser.Expr) error {
	fn := func(_ sqlparser.Expr, rhsExprs []sqlparser.Expr) {
		ctx.skipThesePredicates(rhsExprs...)
	}
	if ctx.execOnJoinPredicateEqual(joinPred, fn) {
		return nil
	}
	return vterrors.VT13001("predicate does not exist: " + sqlparser.String(joinPred))
}

// KeepPredicateInfo transfers join predicate information from another context.
// This is useful when nesting queries, ensuring consistent predicate handling across contexts.
func (ctx *PlanningContext) KeepPredicateInfo(other *PlanningContext) {
	for k, v := range other.joinPredicates {
		ctx.AddJoinPredicates(k, v...)
	}
	for expr := range other.skipPredicates {
		ctx.skipThesePredicates(expr)
	}
}

// skipThesePredicates is a utility function to exclude certain predicates from SQL building
func (ctx *PlanningContext) skipThesePredicates(preds ...sqlparser.Expr) {
outer:
	for _, expr := range preds {
		for k := range ctx.skipPredicates {
			if ctx.SemTable.EqualsExpr(expr, k) {
				// already skipped
				continue outer
			}
		}
		ctx.skipPredicates[expr] = nil
	}
}

func (ctx *PlanningContext) execOnJoinPredicateEqual(joinPred sqlparser.Expr, fn func(original sqlparser.Expr, rhsExprs []sqlparser.Expr)) bool {
	for key, values := range ctx.joinPredicates {
		if ctx.SemTable.EqualsExpr(joinPred, key) {
			fn(key, values)
			return true
		}
	}
	return false
}

func (ctx *PlanningContext) RewriteDerivedTableExpression(expr sqlparser.Expr, tableInfo semantics.TableInfo) sqlparser.Expr {
	modifiedExpr := semantics.RewriteDerivedTableExpression(expr, tableInfo)
	for key, exprs := range ctx.joinPredicates {
		for _, rhsExpr := range exprs {
			if ctx.SemTable.EqualsExpr(expr, rhsExpr) {
				ctx.joinPredicates[key] = append(ctx.joinPredicates[key], modifiedExpr)
				return modifiedExpr
			}
		}
	}
	return modifiedExpr
}

// TypeForExpr returns the type of the given expression, with nullable set if the expression is from an outer table.
func (ctx *PlanningContext) TypeForExpr(e sqlparser.Expr) (evalengine.Type, bool) {
	t, found := ctx.SemTable.TypeForExpr(e)
	if !found {
		typ := ctx.calculateTypeFor(e)
		if typ.Valid() {
			ctx.SemTable.ExprTypes[e] = typ
			return typ, true
		}
		return evalengine.NewUnknownType(), false
	}
	deps := ctx.SemTable.RecursiveDeps(e)
	// If the expression is from an outer table, it should be nullable
	// There are some exceptions to this, where an expression depending on the outer side
	// will never return NULL, but it's better to be conservative here.
	if deps.IsOverlapping(ctx.OuterTables) {
		t.SetNullability(true)
	}
	return t, true
}

func (ctx *PlanningContext) calculateTypeFor(e sqlparser.Expr) evalengine.Type {
	cfg := &evalengine.Config{
		ResolveType: func(expr sqlparser.Expr) (evalengine.Type, bool) {
			col, isCol := expr.(*sqlparser.ColName)
			if !isCol {
				return evalengine.NewUnknownType(), false
			}
			return ctx.SemTable.TypeForExpr(col)
		},
		Collation:   ctx.SemTable.Collation,
		Environment: ctx.VSchema.Environment(),
		ResolveColumn: func(name *sqlparser.ColName) (int, error) {
			// We don't need to resolve the column for type calculation
			return 0, nil
		},
	}
	env := evalengine.EmptyExpressionEnv(ctx.VSchema.Environment())

	// We need to rewrite the aggregate functions to their corresponding types
	// The evaluation engine compiler doesn't handle them, so we replace them with Arguments before
	// asking the compiler for the type

	// TODO: put this back in when we can calculate the aggregation types correctly
	// expr, unknown := ctx.replaceAggrWithArg(e, cfg, env)
	// if unknown {
	// 	return evalengine.NewUnknownType()
	// }

	translatedExpr, err := evalengine.Translate(e, cfg)
	if err != nil {
		return evalengine.NewUnknownType()
	}

	typ, err := env.TypeOf(translatedExpr)
	if err != nil {
		return evalengine.NewUnknownType()
	}
	return typ
}

// replaceAggrWithArg replaces aggregate functions with Arguments in the given expression.
// this is to prepare for sending the expression to the evalengine compiler to figure out the type
func (ctx *PlanningContext) replaceAggrWithArg(e sqlparser.Expr, cfg *evalengine.Config, env *evalengine.ExpressionEnv) (expr sqlparser.Expr, unknown bool) {
	expr = sqlparser.CopyOnRewrite(e, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		agg, ok := cursor.Node().(sqlparser.AggrFunc)
		if !ok {
			return
		}
		code, ok := opcode.SupportedAggregates[agg.AggrName()]
		if !ok {
			// We don't know the type of this aggregate function
			// The type calculation will be set to unknown
			unknown = true
			cursor.StopTreeWalk()
			return
		}
		var inputType evalengine.Type
		if arg := agg.GetArg(); arg != nil {
			translatedExpr, err := evalengine.Translate(arg, cfg)
			if err != nil {
				unknown = true
				cursor.StopTreeWalk()
				return
			}

			inputType, err = env.TypeOf(translatedExpr)
			if err != nil {
				unknown = true
				cursor.StopTreeWalk()
				return
			}
		}
		typ := code.ResolveType(inputType, ctx.VSchema.Environment().CollationEnv())
		cursor.Replace(&sqlparser.Argument{
			Name:  "arg",
			Type:  typ.Type(),
			Size:  typ.Size(),
			Scale: typ.Scale(),
		})
	}, nil).(sqlparser.Expr)
	return expr, unknown
}

// SQLTypeForExpr returns the sql type of the given expression, with nullable set if the expression is from an outer table.
func (ctx *PlanningContext) SQLTypeForExpr(e sqlparser.Expr) sqltypes.Type {
	t, found := ctx.TypeForExpr(e)
	if !found {
		return sqltypes.Unknown
	}
	return t.Type()
}

func (ctx *PlanningContext) NeedsWeightString(e sqlparser.Expr) bool {
	switch e := e.(type) {
	case *sqlparser.WeightStringFuncExpr, *sqlparser.Literal:
		return false
	default:
		typ, found := ctx.TypeForExpr(e)
		if !found {
			return true
		}

		if !sqltypes.IsText(typ.Type()) {
			return false
		}

		return !ctx.VSchema.Environment().CollationEnv().IsSupported(typ.Collation())
	}
}

func (ctx *PlanningContext) IsAggr(e sqlparser.SQLNode) bool {
	switch node := e.(type) {
	case sqlparser.AggrFunc:
		return true
	case *sqlparser.FuncExpr:
		return node.Name.EqualsAnyString(ctx.VSchema.GetAggregateUDFs())
	}

	return false
}

func (ctx *PlanningContext) ContainsAggr(e sqlparser.SQLNode) (hasAggr bool) {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		case *sqlparser.Offset:
			// offsets here indicate that a possible aggregation has already been handled by an input,
			// so we don't need to worry about aggregation in the original
			return false, nil
		case sqlparser.AggrFunc:
			hasAggr = true
			return false, io.EOF
		case *sqlparser.Subquery:
			return false, nil
		case *sqlparser.FuncExpr:
			if ctx.IsAggr(node) {
				hasAggr = true
				return false, io.EOF
			}
		}

		return true, nil
	}, e)
	return
}

func (ctx *PlanningContext) IsMirrored() bool {
	return ctx.isMirrored
}

type ContextCTE struct {
	*semantics.CTE
	Id         semantics.TableSet
	Predicates []*RecurseExpression
}

type RecurseExpression struct {
	Original  sqlparser.Expr
	RightExpr sqlparser.Expr
	LeftExprs []BindVarExpr
}

type BindVarExpr struct {
	Name string
	Expr *sqlparser.ColName
}

func (ctx *PlanningContext) PushCTE(def *semantics.CTE, id semantics.TableSet) {
	ctx.CurrentCTE = append(ctx.CurrentCTE, &ContextCTE{
		CTE: def,
		Id:  id,
	})
}

func (ctx *PlanningContext) PopCTE() (*ContextCTE, error) {
	if len(ctx.CurrentCTE) == 0 {
		return nil, vterrors.VT13001("no CTE to pop")
	}
	activeCTE := ctx.CurrentCTE[len(ctx.CurrentCTE)-1]
	ctx.CurrentCTE = ctx.CurrentCTE[:len(ctx.CurrentCTE)-1]
	return activeCTE, nil
}

func (ctx *PlanningContext) ActiveCTE() *ContextCTE {
	if len(ctx.CurrentCTE) == 0 {
		return nil
	}
	return ctx.CurrentCTE[len(ctx.CurrentCTE)-1]
}

func (ctx *PlanningContext) UseMirror() *PlanningContext {
	if ctx.isMirrored {
		panic(vterrors.VT13001("cannot mirror already mirrored planning context"))
	}
	if ctx.mirror != nil {
		return ctx.mirror
	}
	ctx.mirror = &PlanningContext{
		ctx.ReservedVars,
		ctx.SemTable,
		ctx.VSchema,
		map[sqlparser.Expr][]sqlparser.Expr{},
		map[sqlparser.Expr]any{},
		ctx.PlannerVersion,
		map[sqlparser.Expr]string{},
		ctx.VerifyAllFKs,
		ctx.MergedSubqueries,
		ctx.CurrentPhase,
		ctx.Statement,
		ctx.OuterTables,
		ctx.CurrentCTE,
		nil,
		true,
	}
	return ctx.mirror
}
