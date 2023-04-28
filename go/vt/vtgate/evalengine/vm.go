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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
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

func Deoptimize(expr Expr) Expr {
	switch expr := expr.(type) {
	case *CompiledExpr:
		return expr.original
	default:
		return expr
	}
}

type CompiledExpr struct {
	code     []frame
	typed    sqltypes.Type
	stack    int
	original Expr
}

func (p *CompiledExpr) eval(env *ExpressionEnv) (eval, error) {
	return p.original.eval(env)
}

func (p *CompiledExpr) typeof(*ExpressionEnv, []*querypb.Field) (sqltypes.Type, typeFlag) {
	return p.typed, 0
}

func (p *CompiledExpr) format(buf *formatter, depth int) {
	p.original.format(buf, depth)
}

func (p *CompiledExpr) constant() bool {
	return p.original.constant()
}

func (p *CompiledExpr) simplify(env *ExpressionEnv) error {
	// No-op
	return nil
}

func (p *CompiledExpr) compile(c *compiler) (ctype, error) {
	panic("called compile() on already compiled Expr")
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
	return EvalResult{env.vm.stack[env.vm.sp-1]}, nil

err:
	if env.vm.err == errDeoptimize {
		e, err := p.original.eval(env)
		return EvalResult{e}, err
	}
	return EvalResult{}, env.vm.err
}
