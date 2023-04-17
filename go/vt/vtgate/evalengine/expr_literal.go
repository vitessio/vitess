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
	"math"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	Literal struct {
		inner eval
	}
)

var _ Expr = (*Literal)(nil)

// eval implements the Expr interface
func (l *Literal) eval(_ *ExpressionEnv) (eval, error) {
	return l.inner, nil
}

// typeof implements the Expr interface
func (l *Literal) typeof(*ExpressionEnv, []*querypb.Field) (sqltypes.Type, typeFlag) {
	var f typeFlag
	switch e := l.inner.(type) {
	case nil:
		return sqltypes.Null, flagNull | flagNullable
	case *evalBytes:
		if e.isBitLiteral {
			f |= flagBit
		}
		if e.isHexLiteral {
			f |= flagHex
		}
	case *evalInt64:
		if e.i == math.MinInt64 {
			f |= flagIntegerUdf
		}
		if e == evalBoolTrue || e == evalBoolFalse {
			f |= flagIsBoolean
		}
	case *evalUint64:
		if e.hexLiteral {
			f |= flagHex
		}
		if e.u == math.MaxInt64+1 {
			f |= flagIntegerCap
		}
		if e.u > math.MaxInt64+1 {
			f |= flagIntegerOvf
		}
	}
	return l.inner.SQLType(), f
}

func (l *Literal) compile(c *compiler) (ctype, error) {
	if l.inner == nil {
		c.asm.PushNull()
	} else if err := c.asm.PushLiteral(l.inner); err != nil {
		return ctype{}, err
	}

	t, f := l.typeof(nil, nil)
	return ctype{t, f, evalCollation(l.inner)}, nil

}
