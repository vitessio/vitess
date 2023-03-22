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
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
)

type (
	Column struct {
		Offset    int
		Type      sqltypes.Type
		Collation collations.TypedCollation
		typed     bool
	}
)

var _ Expr = (*Column)(nil)

// eval implements the Expr interface
func (c *Column) eval(env *ExpressionEnv) (eval, error) {
	return valueToEval(env.Row[c.Offset], c.Collation)
}

func (c *Column) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	// if we have an active row in the expression Env, use that as an authoritative source
	if c.Offset < len(env.Row) {
		value := env.Row[c.Offset]
		if value.IsNull() {
			return sqltypes.Null, flagNull | flagNullable
		}
		return value.Type(), typeFlag(0)
	}
	return c.Type, flagNullable
}
