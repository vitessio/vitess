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

var _ Expr = (*ConvertExpr)(nil)
var _ Expr = (*ConvertUsingExpr)(nil)

func (c *ConvertExpr) returnUnsupportedError() error {
	var err error
	switch {
	case c.HasLength && c.HasScale:
		err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported type conversion: %s(%d,%d)", c.Type, c.Length, c.Scale)
	case c.HasLength:
		err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported type conversion: %s(%d)", c.Type, c.Length)
	default:
		err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported type conversion: %s", c.Type)
	}
	return err
}

func (c *ConvertExpr) eval(env *ExpressionEnv) (eval, error) {
	e, err := c.Inner.eval(env)
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, nil
	}

	switch c.Type {
	case "BINARY":
		b := evalToBinary(e)
		if c.HasLength {
			b.truncateInPlace(c.Length)
		}
		return b, nil

	case "CHAR", "NCHAR":
		t, err := evalToText(e, c.Collation, true)
		if err != nil {
			// return NULL on error
			return nil, nil
		}
		if c.HasLength {
			t.truncateInPlace(c.Length)
		}
		return t, nil
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
		return evalToNumeric(e).toDecimal(int32(m), int32(d)), nil
	case "DOUBLE", "REAL":
		f, _ := evalToNumeric(e).toFloat()
		return f, nil
	case "FLOAT":
		if c.HasLength {
			switch p := c.Length; {
			case p > 53:
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Too-big precision %d specified for 'CONVERT'. Maximum is 53.", p)
			}
		}
		return nil, c.returnUnsupportedError()
	case "SIGNED", "SIGNED INTEGER":
		return evalToNumeric(e).toInt64(), nil
	case "UNSIGNED", "UNSIGNED INTEGER":
		return evalToNumeric(e).toUint64(), nil
	case "DATE", "DATETIME", "YEAR", "JSON", "TIME":
		return nil, c.returnUnsupportedError()
	default:
		panic("BUG: sqlparser emitted unknown type")
	}
}

func (c *ConvertExpr) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
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
		return sqltypes.Float32, f
	case "SIGNED", "SIGNED INTEGER":
		return sqltypes.Int64, f
	case "UNSIGNED", "UNSIGNED INTEGER":
		return sqltypes.Uint64, f
	case "DATE", "DATETIME", "YEAR", "JSON", "TIME":
		return sqltypes.Null, f
	default:
		panic("BUG: sqlparser emitted unknown type")
	}
}

func (c *ConvertUsingExpr) eval(env *ExpressionEnv) (eval, error) {
	e, err := c.Inner.eval(env)
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, nil
	}
	e, err = evalToText(e, c.Collation, true)
	if err != nil {
		// return NULL instead of error
		return nil, nil
	}
	return e, nil
}

func (c *ConvertUsingExpr) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := c.Inner.typeof(env)
	return sqltypes.VarChar, f | flagNullable
}
