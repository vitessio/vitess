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
)

type (
	Literal struct {
		inner eval
	}
)

var _ IR = (*Literal)(nil)
var _ Expr = (*Literal)(nil)

func (l *Literal) IR() IR {
	return l
}

func (l *Literal) IsExpr() {}

// eval implements the expression interface
func (l *Literal) eval(_ *ExpressionEnv) (eval, error) {
	return l.inner, nil
}

// typeof implements the Expr interface
func (l *Literal) typeof(*ExpressionEnv) (ctype, error) {
	var f typeFlag
	switch e := l.inner.(type) {
	case nil:
		return ctype{Type: sqltypes.Null, Flag: flagNull | flagNullable, Col: collationNull}, nil
	case *evalBytes:
		f = e.flag
	case *evalInt64:
		if e.i == math.MinInt64 {
			f |= flagIntegerUdf
		}
		if e == evalBoolTrue || e == evalBoolFalse {
			f |= flagIsBoolean
		}
		if e.bitLiteral {
			f |= flagBit
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
	case *evalTemporal:
		return ctype{Type: e.t, Col: collationNumeric, Size: int32(e.prec)}, nil
	case *evalDecimal:
		return ctype{Type: sqltypes.Decimal, Col: collationNumeric, Size: e.length, Scale: -e.dec.Exponent()}, nil
	}
	return ctype{Type: l.inner.SQLType(), Flag: f, Col: evalCollation(l.inner)}, nil
}

func (l *Literal) compile(c *compiler) (ctype, error) {
	if l.inner == nil {
		c.asm.PushNull()
	} else if err := c.asm.PushLiteral(l.inner); err != nil {
		return ctype{}, err
	}
	return l.typeof(nil)
}
