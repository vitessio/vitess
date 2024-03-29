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
	"errors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vthash"
)

var errDeoptimize = errors.New("de-optimize")

type vmstate struct {
	stack []eval
	sp    int

	arena Arena
	hash  vthash.Hasher
	err   error

	flags struct {
		cmp  int
		null bool
	}
}

type CompiledExpr struct {
	code  []frame
	typed ctype
	stack int
	ir    IR
}

func (c *CompiledExpr) IR() IR {
	return c.ir
}

func (c *CompiledExpr) IsExpr() {}

func (p *CompiledExpr) typeof(*ExpressionEnv) (ctype, error) {
	return p.typed, nil
}

func (p *CompiledExpr) eval(env *ExpressionEnv) (eval, error) {
	return p.ir.eval(env)
}

func (p *CompiledExpr) Format(buf *sqlparser.TrackedBuffer) {
	p.ir.format(buf)
}

func (p *CompiledExpr) FormatFast(buf *sqlparser.TrackedBuffer) {
	p.ir.format(buf)
}

var _ Expr = (*CompiledExpr)(nil)

func (env *ExpressionEnv) EvaluateVM(p *CompiledExpr) (EvalResult, error) {
	env.vm.arena.reset()
	env.vm.sp = 0
	env.vm.err = nil
	if len(env.vm.stack) < p.stack {
		env.vm.stack = make([]eval, p.stack)
	}

	code := p.code
	ip := 0

	for ip < len(code) {
		ip += code[ip](env)
		if env.vm.err != nil {
			goto err
		}
	}
	return EvalResult{v: env.vm.stack[env.vm.sp-1], collationEnv: env.collationEnv}, nil

err:
	if env.vm.err == errDeoptimize {
		e, err := p.ir.eval(env)
		return EvalResult{v: e, collationEnv: env.collationEnv}, err
	}
	return EvalResult{collationEnv: env.collationEnv}, env.vm.err
}
