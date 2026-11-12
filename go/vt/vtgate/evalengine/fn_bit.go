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

	"vitess.io/vitess/go/mysql/collations"
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

	return newEvalInt64(int64(bitCount(arg))), nil
}

// bitCount counts the bits of a binary string byte by byte, and those of any
// other value as a number.
func bitCount(e eval) int {
	if b, ok := e.(*evalBytes); ok && (b.isBinary() || isBinaryText(b.SQLType(), b.col.Collation)) && !b.isHexOrBitLiteral() {
		return bitCountBytes(b.bytes)
	}
	return bits.OnesCount64(uint64(evalToInt64(e).i))
}

func bitCountBytes(raw []byte) int {
	var count int
	for _, c := range raw {
		count += bits.OnesCount8(c)
	}
	return count
}

// isBinaryText reports whether a character string type has the binary
// collation. Several string functions return such a value for a binary string
// argument, and BIT_COUNT counts its bits byte by byte like those of any other
// binary string.
func isBinaryText(tt sqltypes.Type, col collations.ID) bool {
	return sqltypes.IsText(tt) && col == collations.CollationBinaryID
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

	if isBinaryText(ct.Type, ct.Col.Collation) && !ct.isHexOrBitLiteral() {
		// A character string with the binary collation is also the declared
		// type of expressions whose value is a number, such as ROUND of such
		// a string or COALESCE of one and a number, so the instruction checks
		// that the value is a string.
		c.asm.BitCount_x()
		c.asm.jumpDestination(skip)
		return ctype{Type: sqltypes.Int64, Flag: nullableFlags(ct.Flag), Col: collationBinary}, nil
	}

	_ = c.compileToBitwiseUint64(ct, 1)
	c.asm.BitCount_u()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Flag: nullableFlags(ct.Flag), Col: collationBinary}, nil
}
