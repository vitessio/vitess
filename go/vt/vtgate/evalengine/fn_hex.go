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

type builtinHex struct {
	CallExpr
}

var _ Expr = (*builtinHex)(nil)

func (call *builtinHex) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	var encoded []byte
	switch arg := arg.(type) {
	case *evalBytes:
		encoded = hexEncodeBytes(arg.bytes)
	case evalNumeric:
		encoded = hexEncodeUint(uint64(arg.toInt64().i))
	default:
		encoded = hexEncodeBytes(arg.ToRawBytes())
	}
	if arg.SQLType() == sqltypes.Blob || arg.SQLType() == sqltypes.TypeJSON {
		return newEvalRaw(sqltypes.Text, encoded, env.collation()), nil
	}
	return newEvalText(encoded, env.collation()), nil
}

func (call *builtinHex) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	tt, f := call.Arguments[0].typeof(env)
	if tt == sqltypes.Blob || tt == sqltypes.TypeJSON {
		return sqltypes.Text, f
	}
	return sqltypes.VarChar, f
}

const hextable = "0123456789ABCDEF"

func hexEncodeBytes(src []byte) []byte {
	j := 0
	dst := make([]byte, len(src)*2)
	for _, v := range src {
		dst[j] = hextable[v>>4]
		dst[j+1] = hextable[v&0x0f]
		j += 2
	}
	return dst
}

func hexEncodeUint(u uint64) []byte {
	var a [16 + 1]byte
	i := len(a)
	shift := uint(bits.TrailingZeros(uint(16))) & 7
	b := uint64(16)
	m := uint(16) - 1 // == 1<<shift - 1

	for u >= b {
		i--
		a[i] = hextable[uint(u)&m]
		u >>= shift
	}

	// u < base
	i--
	a[i] = hextable[uint(u)]
	return a[i:]
}
