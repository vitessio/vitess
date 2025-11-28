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
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/predicates"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type PlanningContext struct {
	ReservedVars *sqlparser.ReservedVars
	SemTable     *semantics.SemTable
	VSchema      VSchema

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

	emptyEnv    *evalengine.ExpressionEnv
	constantCfg *evalengine.Config

	PredTracker *predicates.Tracker

	Conditions []engine.Condition
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
	if ks, _ := vschema.SelectedKeyspace(); ks != nil {
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
		PlannerVersion:    version,
		ReservedArguments: map[sqlparser.Expr]string{},
		Statement:         stmt,
		PredTracker:       predicates.NewTracker(),
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
		switch node := node.(type) {
		case *sqlparser.Offset:
			// offsets here indicate that a possible aggregation has already been handled by an input,
			// so we don't need to worry about aggregation in the original
			return false, nil
		case sqlparser.AggrFunc:
			if wf, ok := node.(sqlparser.WindowFunc); ok && wf.GetOverClause() != nil {
				return true, nil
			}
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

func (ctx *PlanningContext) ContainsWindowFunc(e sqlparser.SQLNode) (hasWindow bool) {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.Offset:
			// offsets here indicate that a possible window function has already been handled by an input,
			// so we don't need to worry about it in the original
			return false, nil
		case sqlparser.WindowFunc:
			if node.GetOverClause() != nil {
				hasWindow = true
				return false, io.EOF
			}
		case *sqlparser.Subquery:
			return false, nil
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
	JoinPredicateID *predicates.ID
	Original        sqlparser.Expr
	RightExpr       sqlparser.Expr
	LeftExprs       []BindVarExpr
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
		ReservedVars:      ctx.ReservedVars,
		SemTable:          ctx.SemTable,
		VSchema:           ctx.VSchema,
		PlannerVersion:    ctx.PlannerVersion,
		ReservedArguments: map[sqlparser.Expr]string{},
		VerifyAllFKs:      ctx.VerifyAllFKs,
		MergedSubqueries:  ctx.MergedSubqueries,
		CurrentPhase:      ctx.CurrentPhase,
		Statement:         ctx.Statement,
		OuterTables:       ctx.OuterTables,
		CurrentCTE:        ctx.CurrentCTE,
		emptyEnv:          ctx.emptyEnv,
		PredTracker:       ctx.PredTracker,
		isMirrored:        true,
	}
	return ctx.mirror
}

// IsConstantBool checks whether this predicate can be evaluated at plan-time.
// If it can, it returns the constant value.
func (ctx *PlanningContext) IsConstantBool(expr sqlparser.Expr) *bool {
	if !ctx.SemTable.RecursiveDeps(expr).IsEmpty() {
		// we have column dependencies, so we can be pretty sure
		// we won't be able to use the evalengine to check if this is constant false
		return nil
	}
	env := ctx.VSchema.Environment()
	collation := ctx.VSchema.ConnCollation()
	if ctx.constantCfg == nil {
		ctx.constantCfg = &evalengine.Config{
			Collation:     collation,
			Environment:   env,
			NoCompilation: true,
		}
	}
	eexpr, err := evalengine.Translate(expr, ctx.constantCfg)
	if ctx.emptyEnv == nil {
		ctx.emptyEnv = evalengine.EmptyExpressionEnv(env)
	}
	if err != nil {
		return nil
	}
	eres, err := ctx.emptyEnv.Evaluate(eexpr)
	if err != nil {
		return nil
	}
	if eres.Value(collation).IsNull() {
		return nil
	}
	b, err := eres.ToBooleanStrict()
	if err != nil {
		return nil
	}
	return &b
}

func (ctx *PlanningContext) CollectConditions(conditions []engine.Condition) {
	ctx.Conditions = append(ctx.Conditions, conditions...)
}
