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

package planbuilder

import (
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"

	"github.com/stretchr/testify/assert"
)

func TestDoesNotRewrite(t *testing.T) {
	// SELECT 1
	literal := newIntVal("1")
	result, err := Rewrite(literal)
	assert.NoError(t, err)
	assert.Equal(t, literal, result.Expression)
	assert.False(t, result.NeedLastInsertID, "should not need last insert id")
}

func liidBindVar() sqlparser.Expr {
	return sqlparser.NewValArg([]byte(":" + engine.LastInsertIDName))
}

func TestRewriteLIID(t *testing.T) {
	// SELECT last_insert_id()
	lastInsertID := &sqlparser.FuncExpr{
		Qualifier: sqlparser.TableIdent{},
		Name:      sqlparser.NewColIdent("last_insert_id"),
		Distinct:  false,
		Exprs:     nil,
	}
	result, err := Rewrite(lastInsertID)
	assert.NoError(t, err)
	assert.Equal(t, liidBindVar(), result.Expression)
	assert.True(t, result.NeedLastInsertID, "should need last insert id")
}

func TestRewriteLIIDComplex(t *testing.T) {
	// SELECT 1 + last_insert_id()
	lastInsertID := &sqlparser.FuncExpr{
		Qualifier: sqlparser.TableIdent{},
		Name:      sqlparser.NewColIdent("last_insert_id"),
		Distinct:  false,
		Exprs:     nil,
	}
	expr := &sqlparser.BinaryExpr{
		Operator: "+",
		Left:     sqlparser.NewIntVal([]byte("1")),
		Right:    lastInsertID,
	}

	result, err := Rewrite(expr)

	assert.NoError(t, err)

	expected := &sqlparser.BinaryExpr{
		Operator: "+",
		Left:     sqlparser.NewIntVal([]byte("1")),
		Right:    liidBindVar(),
	}

	assert.Equal(t, expected, result.Expression)
	assert.True(t, result.NeedLastInsertID, "should need last insert id")
}

func TestRewriteLIIDComplex2(t *testing.T) {
	// SELECT last_insert_id() + last_insert_id()
	lastInsertID1 := &sqlparser.FuncExpr{
		Qualifier: sqlparser.TableIdent{},
		Name:      sqlparser.NewColIdent("last_insert_id"),
		Distinct:  false,
		Exprs:     nil,
	}
	lastInsertID2 := &sqlparser.FuncExpr{
		Qualifier: sqlparser.TableIdent{},
		Name:      sqlparser.NewColIdent("last_insert_id"),
		Distinct:  false,
		Exprs:     nil,
	}
	expr := &sqlparser.BinaryExpr{
		Operator: "+",
		Left:     lastInsertID1,
		Right:    lastInsertID2,
	}

	result, err := Rewrite(expr)

	assert.NoError(t, err)

	expected := &sqlparser.BinaryExpr{
		Operator: "+",
		Left:     liidBindVar(),
		Right:    liidBindVar(),
	}

	assert.Equal(t, expected, result.Expression)
	assert.True(t, result.NeedLastInsertID, "should need last insert id")
}

func TestRewriteDatabaseFunc(t *testing.T) {
	// SELECT database()
	database := &sqlparser.FuncExpr{
		Qualifier: sqlparser.TableIdent{},
		Name:      sqlparser.NewColIdent("database"),
		Distinct:  false,
		Exprs:     nil,
	}
	result, err := Rewrite(database)
	assert.NoError(t, err)
	assert.Equal(t, databaseBindVar(), result.Expression)
	assert.True(t, result.NeedDatabase, "should need database name")
}

func TestRewriteSelectExpr(t *testing.T) {
	// SELECT database() => SELECT :__vtdbname AS `database()`

	stmt, err := sqlparser.Parse("SELECT database()")
	assert.NoError(t, err)

	result, err := RewriteAST(stmt)
	assert.NoError(t, err)

	expected, err := sqlparser.Parse("SELECT :__vtdbname as `database()`")
	assert.NoError(t, err)

	assert.Equal(t, expected, result.AST)
	assert.True(t, result.NeedDatabase, "should need database name")
}

func TestDoesNotOverwriteALias(t *testing.T) {
	// SELECT last_insert_id() as test => SELECT :__lastInsertId AS as test

	stmt, err := sqlparser.Parse("SELECT last_insert_id() as test")
	assert.NoError(t, err)

	result, err := RewriteAST(stmt)
	assert.NoError(t, err)

	expected, err := sqlparser.Parse("SELECT :__lastInsertId as test")
	assert.NoError(t, err)

	assert.Equal(t, expected, result.AST)
	assert.True(t, result.NeedLastInsertID, "should need database name")
}

func databaseBindVar() sqlparser.Expr {
	return sqlparser.NewValArg([]byte(":" + engine.DBVarName))
}
