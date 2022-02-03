/*
Copyright 2021 The Vitess Authors.

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

import querypb "vitess.io/vitess/go/vt/proto/query"

type (
	UnaryExpr struct {
		Inner Expr
	}

	NegateExpr struct {
		UnaryExpr
	}
)

func (c *UnaryExpr) typeof(env *ExpressionEnv) querypb.Type {
	return c.Inner.typeof(env)
}

func (n *NegateExpr) eval(env *ExpressionEnv, result *EvalResult) {
	result.init(env, n.Inner)
	result.negateNumeric()
}

func (n *NegateExpr) typeof(env *ExpressionEnv) querypb.Type {
	// the type of a NegateExpr is not known beforehand because negating
	// a large enough value can cause it to be upcasted into a larger type
	return -1
}
