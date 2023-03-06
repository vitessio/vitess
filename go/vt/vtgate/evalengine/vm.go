package evalengine

import (
	"errors"

	"vitess.io/vitess/go/sqltypes"
)

var errDeoptimize = errors.New("de-optimize")

type VirtualMachine struct {
	arena Arena
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
