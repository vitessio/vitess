/*
Copyright 2022 The Vitess Authors.

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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

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

type builtinHex struct{}

func (builtinHex) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	tohex := &args[0]
	if tohex.isNull() {
		result.setNull()
	}

	var encoded []byte
	switch tt := tohex.typeof(); {
	case sqltypes.IsQuoted(tt):
		encoded = hexEncodeBytes(tohex.bytes())
	case sqltypes.IsNumber(tt):
		tohex.makeUnsignedIntegral()
		encoded = hexEncodeUint(tohex.uint64())
	default:
		throwEvalError(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported HEX argument: %s", tt.String()))
	}

	result.setRaw(sqltypes.VarChar, encoded, collations.TypedCollation{
		Collation:    env.DefaultCollation,
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireASCII,
	})
}

func (builtinHex) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("HEX")
	}
	_, f := args[0].typeof(env)
	return sqltypes.VarChar, f
}
