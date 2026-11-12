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

// bitCount returns the number of bits that are set in e.
//
// For a binary string it counts the bits of every byte:
// BIT_COUNT(_binary 'a') is 3. It converts any other value to a 64-bit
// integer first: BIT_COUNT('a') is 0, because 'a' is the number 0.
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

// isBinaryText reports whether tt and col describe a character string with
// the binary collation.
//
// Several string functions declare that type for a binary string argument,
// such as REVERSE(_binary 'a'). BIT_COUNT treats it like a binary string.
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
		// The declared type can be wrong here. COALESCE of such a string
		// and a number declares this type, but returns the argument that
		// it selects as it is, which can be the number. So BitCount_x
		// looks at the value when it runs.
		c.asm.BitCount_x()
		c.asm.jumpDestination(skip)
		return ctype{Type: sqltypes.Int64, Flag: nullableFlags(ct.Flag), Col: collationBinary}, nil
	}

	_ = c.compileToBitwiseUint64(ct, 1)
	c.asm.BitCount_u()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Flag: nullableFlags(ct.Flag), Col: collationBinary}, nil
}
