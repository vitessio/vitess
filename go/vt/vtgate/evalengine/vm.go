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
	arena Arena
	hash  vthash.Hasher
	row   []sqltypes.Value
	stack []eval
	sp    int

	err   error
	flags struct {
		cmp  int
		null bool
	}
}

type Program struct {
	code     []frame
	original Expr
	stack    int
}

func (vm *VirtualMachine) Run(p *Program, row []sqltypes.Value) (EvalResult, error) {
	vm.arena.reset()
	vm.row = row
	vm.sp = 0
	if len(vm.stack) < p.stack {
		vm.stack = make([]eval, p.stack)
	}
	e, err := vm.execute(p)
	if err != nil {
		if err == errDeoptimize {
			var env ExpressionEnv
			env.Row = row
			return env.Evaluate(p.original)
		}
		return EvalResult{}, err
	}
	return EvalResult{e}, nil
}

func (vm *VirtualMachine) execute(p *Program) (eval, error) {
	code := p.code
	ip := 0

	for ip < len(code) {
		ip += code[ip](vm)
		if vm.err != nil {
			return nil, vm.err
		}
	}
	if vm.sp == 0 {
		return nil, nil
	}
	return vm.stack[vm.sp-1], nil
}
