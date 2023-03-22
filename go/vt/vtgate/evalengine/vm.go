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
	"vitess.io/vitess/go/vt/vthash"
)

var errDeoptimize = errors.New("de-optimize")

type VirtualMachine struct {
	stack []eval
	sp    int

	row   []sqltypes.Value
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
	stack    int
	original Expr
}

func (p *CompiledExpr) eval(env *ExpressionEnv) (eval, error) {
	if env.vm == nil {
		env.vm = new(VirtualMachine)
	}
	return env.vm.eval(p, env.Row)
}

func (p *CompiledExpr) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	return p.original.typeof(env)
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

var _ Expr = (*CompiledExpr)(nil)

func (vm *VirtualMachine) eval(p *CompiledExpr, row []sqltypes.Value) (eval, error) {
	vm.arena.reset()
	vm.row = row
	vm.sp = 0
	vm.err = nil
	if len(vm.stack) < p.stack {
		vm.stack = make([]eval, p.stack)
	}
	e, err := vm.run(p)
	if err != nil {
		if err == errDeoptimize {
			var env ExpressionEnv
			env.Row = row
			return p.original.eval(&env)
		}
		return nil, err
	}
	return e, nil
}

func (vm *VirtualMachine) Evaluate(p *CompiledExpr, row []sqltypes.Value) (EvalResult, error) {
	e, err := vm.eval(p, row)
	return EvalResult{e}, err
}

func (vm *VirtualMachine) run(p *CompiledExpr) (eval, error) {
	code := p.code
	ip := 0

	for ip < len(code) {
		ip += code[ip](vm)
		if vm.err != nil {
			return nil, vm.err
		}
	}
	return vm.stack[vm.sp-1], nil
}
