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
)

func (c *compiler) compileFn_rounding(arg0 Expr, asm_ins_f, asm_ins_d func()) (ctype, error) {
	arg, err := arg0.compile(c)
	if err != nil {
		return ctype{}, err
	}

	if arg.Type == sqltypes.Int64 || arg.Type == sqltypes.Uint64 {
		// No-op for integers.
		return arg, nil
	}

	skip := c.compileNullCheck1(arg)

	convt := ctype{Type: arg.Type, Col: collationNumeric, Flag: arg.Flag}
	switch arg.Type {
	case sqltypes.Float64:
		asm_ins_f()
	case sqltypes.Decimal:
		// We assume here the most common case here is that
		// the decimal fits into an integer.
		convt.Type = sqltypes.Int64
		asm_ins_d()
	default:
		convt.Type = sqltypes.Float64
		c.asm.Convert_xf(1)
		asm_ins_f()
	}

	c.asm.jumpDestination(skip)
	return convt, nil
}

func (c *compiler) compileFn_math1(arg0 Expr, asm_ins func(), nullable typeFlag) (ctype, error) {
	arg, err := arg0.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)
	c.compileToFloat(arg, 1)
	asm_ins()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Float64, Col: collationNumeric, Flag: arg.Flag | nullable}, nil
}

func (c *compiler) compileFn_length(arg Expr, asm_ins func()) (ctype, error) {
	str, err := arg.compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xc(1, sqltypes.VarChar, c.cfg.Collation, 0, false)
	}

	asm_ins()
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
}
