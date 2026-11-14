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

// typeof returns the type of the expression. When the expression is a string
// whose length depends on bind variables, its type is decided from their
// values in env: REPEAT(s, :n) is a VARCHAR for n = 2 and a TEXT for
// n = 1639 on a utf8mb4 VARCHAR(10).
func (p *CompiledExpr) typeof(env *ExpressionEnv) (ctype, error) {
	// A bind variable by itself is a literal for MySQL, and a literal is
	// typed from its own type whatever its length: only the string functions
	// build a length from it.
	ct := p.typed
	if !ct.StrLen.symbolic() || ct.StrLen.bv != nil || !sqltypes.IsTextOrBinary(ct.Type) {
		return ct, nil
	}
	if r := ct.StrLen.get(env); r.ok {
		ct.Type = stringTypeOf(ct.Type, r.n, ct.Col.Collation)
		ct.StrLen = newStrLenConst(r.n)
	}
	return ct, nil
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
