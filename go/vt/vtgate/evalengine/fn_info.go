/*
Copyright 2023 The Vitess Authors.

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

package evalengine

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/servenv"
)

type builtinUser struct {
	CallExpr
}

var _ Expr = (*builtinUser)(nil)

func (call *builtinUser) eval(env *ExpressionEnv) (eval, error) {
	return newEvalText([]byte(env.currentUser()), collationUtf8mb3), nil
}

func (call *builtinUser) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.VarChar, 0
}

func (*builtinUser) compile(c *compiler) (ctype, error) {
	c.asm.Fn_User()
	return ctype{Type: sqltypes.VarChar, Col: collationUtf8mb3}, nil
}

func (call *builtinUser) constant() bool {
	return false
}

type builtinVersion struct {
	CallExpr
}

var _ Expr = (*builtinVersion)(nil)

func (call *builtinVersion) eval(env *ExpressionEnv) (eval, error) {
	return newEvalText([]byte(servenv.MySQLServerVersion()), collationUtf8mb3), nil
}

func (call *builtinVersion) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.VarChar, 0
}

func (*builtinVersion) compile(c *compiler) (ctype, error) {
	c.asm.Fn_Version()
	return ctype{Type: sqltypes.Datetime, Col: collationUtf8mb3}, nil
}

type builtinDatabase struct {
	CallExpr
}

var _ Expr = (*builtinDatabase)(nil)

func (call *builtinDatabase) eval(env *ExpressionEnv) (eval, error) {
	db := env.currentDatabase()
	if db == "" {
		return nil, nil
	}
	return newEvalText([]byte(db), collationUtf8mb3), nil
}

func (call *builtinDatabase) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.VarChar, 0
}

func (*builtinDatabase) compile(c *compiler) (ctype, error) {
	c.asm.Fn_Database()
	return ctype{Type: sqltypes.Datetime, Col: collationUtf8mb3}, nil
}

func (call *builtinDatabase) constant() bool {
	return false
}
