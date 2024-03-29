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
	"math/bits"

	"vitess.io/vitess/go/sqltypes"
)

type builtinBitCount struct {
	CallExpr
}

var _ IR = (*builtinBitCount)(nil)

func (call *builtinBitCount) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	var count int
	if b, ok := arg.(*evalBytes); ok && b.isBinary() && !b.isHexOrBitLiteral() {
		binary := b.bytes
		for _, b := range binary {
			count += bits.OnesCount8(b)
		}
	} else {
		u := evalToInt64(arg)
		count = bits.OnesCount64(uint64(u.i))
	}
	return newEvalInt64(int64(count)), nil
}

func (expr *builtinBitCount) compile(c *compiler) (ctype, error) {
	ct, err := expr.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(ct)

	if ct.Type == sqltypes.VarBinary && !ct.isHexOrBitLiteral() {
		c.asm.BitCount_b()
		c.asm.jumpDestination(skip)
		return ctype{Type: sqltypes.Int64, Flag: nullableFlags(ct.Flag), Col: collationBinary}, nil
	}

	_ = c.compileToBitwiseUint64(ct, 1)
	c.asm.BitCount_u()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Flag: nullableFlags(ct.Flag), Col: collationBinary}, nil
}
