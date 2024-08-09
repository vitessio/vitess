/*
Copyright 2024 The Vitess Authors.

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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func TestTyping(t *testing.T) {
	// this test checks that PlanningContext can take an expression with only columns typed and
	// return the type of the full expression
	// col1 is a bigint, and col2 is a varchar
	expr, err := sqlparser.NewTestParser().ParseExpr("sum(length(col1)) + avg(acos(col2))")
	require.NoError(t, err)
	semTable := semantics.EmptySemTable()
	var sum, avg, col1, col2, length, acos sqlparser.Expr

	// here we walk the expression tree and fetch the two aggregate functions, and set the types for the columns
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			switch node.Name.String() {
			case "col1":
				semTable.ExprTypes[node] = evalengine.NewType(sqltypes.Int64, collations.Unknown)
				col1 = node
			case "col2":
				semTable.ExprTypes[node] = evalengine.NewType(sqltypes.VarChar, collations.Unknown)
				col2 = node
			}
		case *sqlparser.FuncExpr:
			switch node.Name.Lowered() {
			case "length":
				length = node
			case "acos":
				acos = node
			}

		case *sqlparser.Sum:
			sum = node
		case *sqlparser.Avg:
			avg = node
		}

		return true, nil
	}, expr)

	ctx := createPlanContext(semTable)

	expectations := map[sqlparser.Expr]sqltypes.Type{
		// TODO: re-enable these tests once we can calculate aggregation types
		// sum:    sqltypes.Decimal,
		// avg:    sqltypes.Float64,
		// expr:   sqltypes.Float64,
		col1:   sqltypes.Int64,
		col2:   sqltypes.VarChar,
		length: sqltypes.Int64,
		acos:   sqltypes.Float64,
	}
	fmt.Println(sum, avg, expr, acos, col1, col2)

	for expr, expected := range expectations {
		t.Run(sqlparser.String(expr), func(t *testing.T) {
			typ, found := ctx.TypeForExpr(expr)
			require.True(t, found)
			require.Equal(t, expected, typ.Type())
		})
	}
}

func TestOuterTableNullability(t *testing.T) {
	// Tests that columns from outer tables are nullable,
	// even though the semantic state says that they are not nullable.
	// This is because the outer table may not have a matching row.
	// All columns are marked as NOT NULL in the schema.
	query := "select * from t1 left join t2 on t1.a = t2.a where t1.a+t2.a/abs(t2.boing)"
	ctx, columns := prepareContextAndFindColumns(t, query)

	// Check if the columns are correctly marked as nullable.
	for _, col := range columns {
		colName := "column: " + sqlparser.String(col)
		t.Run(colName, func(t *testing.T) {
			// Extract the column type from the context and the semantic state.
			// The context should mark the column as nullable.
			ctxType, found := ctx.TypeForExpr(col)
			require.True(t, found, colName)
			stType, found := ctx.SemTable.TypeForExpr(col)
			require.True(t, found, colName)
			ctxNullable := ctxType.Nullable()
			stNullable := stType.Nullable()

			switch col.Qualifier.Name.String() {
			case "t1":
				assert.False(t, ctxNullable, colName)
				assert.False(t, stNullable, colName)
			case "t2":
				assert.True(t, ctxNullable, colName)

				// The semantic state says that the column is not nullable. Don't trust it.
				assert.False(t, stNullable, colName)
			}
		})
	}
}

func prepareContextAndFindColumns(t *testing.T, query string) (ctx *PlanningContext, columns []*sqlparser.ColName) {
	parser := sqlparser.NewTestParser()
	ast, err := parser.Parse(query)
	require.NoError(t, err)
	semTable := semantics.EmptySemTable()
	t1 := semTable.NewTableId()
	t2 := semTable.NewTableId()
	stmt := ast.(*sqlparser.Select)
	expr := stmt.Where.Expr

	// Instead of using the semantic analysis, we manually set the types for the columns.
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		col, ok := node.(*sqlparser.ColName)
		if !ok {
			return true, nil
		}

		switch col.Qualifier.Name.String() {
		case "t1":
			semTable.Recursive[col] = t1
		case "t2":
			semTable.Recursive[col] = t2
		}

		intNotNull := evalengine.NewType(sqltypes.Int64, collations.Unknown)
		intNotNull.SetNullability(false)
		semTable.ExprTypes[col] = intNotNull
		columns = append(columns, col)
		return false, nil
	}, nil, expr)

	ctx = createPlanContext(semTable)
	ctx.Statement = stmt
	ctx.OuterTables = t2

	return
}

func createPlanContext(st *semantics.SemTable) *PlanningContext {
	return &PlanningContext{
		SemTable:          st,
		joinPredicates:    map[sqlparser.Expr][]sqlparser.Expr{},
		skipPredicates:    map[sqlparser.Expr]any{},
		ReservedArguments: map[sqlparser.Expr]string{},
		VSchema:           &vschema{},
	}
}

type vschema struct{}

func (v *vschema) FindTable(tablename sqlparser.TableName) (*vindexes.Table, string, topodatapb.TabletType, key.Destination, error) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) FindView(name sqlparser.TableName) sqlparser.SelectStatement {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) DefaultKeyspace() (*vindexes.Keyspace, error) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) TargetString() string {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) Destination() key.Destination {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) TabletType() topodatapb.TabletType {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) TargetDestination(qualifier string) (key.Destination, *vindexes.Keyspace, topodatapb.TabletType, error) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) AnyKeyspace() (*vindexes.Keyspace, error) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) FirstSortedKeyspace() (*vindexes.Keyspace, error) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) SysVarSetEnabled() bool {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) KeyspaceExists(keyspace string) bool {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) AllKeyspace() ([]*vindexes.Keyspace, error) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) FindKeyspace(keyspace string) (*vindexes.Keyspace, error) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) GetSemTable() *semantics.SemTable {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) Planner() PlannerVersion {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) SetPlannerVersion(pv PlannerVersion) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) ConnCollation() collations.ID {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) Environment() *vtenv.Environment {
	return vtenv.NewTestEnv()
}

func (v *vschema) ErrorIfShardedF(keyspace *vindexes.Keyspace, warn, errFmt string, params ...any) error {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) WarnUnshardedOnly(format string, params ...any) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) PlannerWarning(message string) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) ForeignKeyMode(keyspace string) (vschemapb.Keyspace_ForeignKeyMode, error) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) KeyspaceError(keyspace string) error {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) GetForeignKeyChecksState() *bool {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) GetVSchema() *vindexes.VSchema {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) GetSrvVschema() *vschemapb.SrvVSchema {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) FindRoutedShard(keyspace, shard string) (string, error) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) IsShardRoutingEnabled() bool {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) IsViewsEnabled() bool {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) GetUDV(name string) *querypb.BindVariable {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) PlanPrepareStatement(ctx context.Context, query string) (*engine.Plan, sqlparser.Statement, error) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) ClearPrepareData(stmtName string) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) GetPrepareData(stmtName string) *vtgatepb.PrepareData {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) StorePrepareData(name string, pd *vtgatepb.PrepareData) {
	// TODO implement me
	panic("implement me")
}

func (v *vschema) GetAggregateUDFs() []string {
	// TODO implement me
	panic("implement me")
}

// FindMirrorRule implements VSchema.
func (v *vschema) FindMirrorRule(tablename sqlparser.TableName) (*vindexes.MirrorRule, error) {
	panic("unimplemented")
}

var _ VSchema = (*vschema)(nil)
