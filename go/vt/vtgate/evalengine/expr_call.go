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

type (
	callable interface {
		Expr
		callable() []Expr
	}

	CallExpr struct {
		Arguments TupleExpr
		Method    string
	}
)

func (c *CallExpr) callable() []Expr {
	return c.Arguments
}

func (c *CallExpr) args(env *ExpressionEnv) ([]eval, error) {
	args := make([]eval, 0, len(c.Arguments))
	for _, arg := range c.Arguments {
		e, err := arg.eval(env)
		if err != nil {
			return nil, err
		}
		args = append(args, e)
	}
	return args, nil
}

func (c *CallExpr) arg1(env *ExpressionEnv) (eval, error) {
	return c.Arguments[0].eval(env)
}

func (c *CallExpr) arg2(env *ExpressionEnv) (left eval, right eval, err error) {
	left, err = c.Arguments[0].eval(env)
	if err != nil {
		return
	}
	right, err = c.Arguments[1].eval(env)
	return
}

func (c *CallExpr) arg3(env *ExpressionEnv) (arg1 eval, arg2 eval, arg3 eval, err error) {
	arg1, err = c.Arguments[0].eval(env)
	if err != nil {
		return
	}
	arg2, err = c.Arguments[1].eval(env)
	if err != nil {
		return
	}
	arg3, err = c.Arguments[2].eval(env)
	return
}
