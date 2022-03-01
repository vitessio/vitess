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
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	ConvertExpr struct {
		UnaryExpr
		Type                string
		Length, Scale       int
		HasLength, HasScale bool
		Collation           collations.ID
	}

	ConvertUsingExpr struct {
		UnaryExpr
		Collation collations.ID
	}
)

func (c *ConvertExpr) unsupported() {
	var err error
	switch {
	case c.HasLength && c.HasScale:
		err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported type conversion: %s(%d,%d)", c.Type, c.Length, c.Scale)
	case c.HasLength:
		err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported type conversion: %s(%d)", c.Type, c.Length)
	default:
		err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported type conversion: %s", c.Type)
	}
	throwEvalError(err)
}

func (c *ConvertExpr) eval(env *ExpressionEnv, result *EvalResult) {
	result.init(env, c.Inner)
	if result.isNull() {
		result.resolve()
		return
	}

	switch c.Type {
	case "BINARY":
		result.makeBinary()
		if c.HasLength {
			result.truncate(c.Length)
		}
	case "CHAR", "NCHAR":
		if result.makeTextualAndConvert(c.Collation) && c.HasLength {
			result.truncate(c.Length)
		}
	case "DECIMAL":
		m := 10
		d := 0
		if c.HasLength {
			m = c.Length
		}
		if c.HasScale {
			d = c.Scale
		}
		if m == 0 && d == 0 {
			m = 10
		}
		result.makeDecimal(int32(m), int32(d))
	case "DOUBLE", "REAL":
		result.makeFloat()
	case "FLOAT":
		if c.HasLength {
			switch p := c.Length; {
			case p <= 24:
				c.unsupported()
			case p <= 53:
				result.makeFloat()
			default:
				throwEvalError(vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Too-big precision %d specified for 'CONVERT'. Maximum is 53.", p))
			}
		} else {
			c.unsupported()
		}
		c.unsupported()
	case "SIGNED", "SIGNED INTEGER":
		result.makeSignedIntegral()
	case "UNSIGNED", "UNSIGNED INTEGER":
		result.makeUnsignedIntegral()
	case "DATE", "DATETIME", "YEAR", "JSON", "TIME":
		c.unsupported()
	default:
		panic("BUG: sqlparser emitted unknown type")
	}
}

func (c *ConvertExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	_, f := c.Inner.typeof(env)

	switch c.Type {
	case "BINARY":
		return sqltypes.VarBinary, f
	case "CHAR", "NCHAR":
		return sqltypes.VarChar, f | flagNullable
	case "DECIMAL":
		return sqltypes.Decimal, f
	case "DOUBLE", "REAL":
		return sqltypes.Float64, f
	case "FLOAT":
		c.unsupported()
		return sqltypes.Float32, f
	case "SIGNED", "SIGNED INTEGER":
		return sqltypes.Int64, f
	case "UNSIGNED", "UNSIGNED INTEGER":
		return sqltypes.Uint64, f
	case "DATE", "DATETIME", "YEAR", "JSON", "TIME":
		c.unsupported()
		return sqltypes.Null, f
	default:
		panic("BUG: sqlparser emitted unknown type")
	}
}

func (c *ConvertUsingExpr) eval(env *ExpressionEnv, result *EvalResult) {
	result.init(env, c.Inner)
	if result.isNull() {
		result.resolve()
	} else {
		result.makeTextualAndConvert(c.Collation)
	}
}

func (c *ConvertUsingExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	_, f := c.Inner.typeof(env)
	return sqltypes.VarChar, f | flagNullable
}
