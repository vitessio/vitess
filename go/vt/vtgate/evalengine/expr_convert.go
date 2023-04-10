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
	querypb "vitess.io/vitess/go/vt/proto/query"
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

func (c *ConvertExpr) decimalPrecision() (int32, int32) {
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
	return int32(m), int32(d)
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
		b.tt = int16(c.convertToBinaryType(e.SQLType()))
		return b, nil

	case "CHAR", "NCHAR":
		t, err := evalToVarchar(e, c.Collation, true)
		if err != nil {
			// return NULL on error
			return nil, nil
		}
		if c.HasLength {
			t.truncateInPlace(c.Length)
		}
		t.tt = int16(c.convertToCharType(e.SQLType()))
		return t, nil
	case "DECIMAL":
		m, d := c.decimalPrecision()
		return evalToNumeric(e).toDecimal(m, d), nil
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
		return evalToInt64(e), nil
	case "UNSIGNED", "UNSIGNED INTEGER":
		return evalToInt64(e).toUint64(), nil
	case "JSON":
		return evalToJSON(e)
	case "DATE", "DATETIME", "YEAR", "TIME":
		return nil, c.returnUnsupportedError()
	default:
		panic("BUG: sqlparser emitted unknown type")
	}
}

func (c *ConvertExpr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	tt, f := c.Inner.typeof(env, fields)

	switch c.Type {
	case "BINARY":
		return c.convertToBinaryType(tt), f
	case "CHAR", "NCHAR":
		return c.convertToCharType(tt), f | flagNullable
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
	case "JSON":
		return sqltypes.TypeJSON, f
	case "DATE", "DATETIME", "YEAR", "TIME":
		return sqltypes.Null, f
	default:
		panic("BUG: sqlparser emitted unknown type")
	}
}

func (c *ConvertExpr) convertToBinaryType(tt sqltypes.Type) sqltypes.Type {
	if c.HasLength {
		if c.Length > 64*1024 {
			return sqltypes.Blob
		}
	} else if tt == sqltypes.Blob || tt == sqltypes.TypeJSON {
		return sqltypes.Blob
	}
	return sqltypes.VarBinary
}

func (c *ConvertExpr) convertToCharType(tt sqltypes.Type) sqltypes.Type {
	if c.HasLength {
		col := c.Collation.Get()
		length := c.Length * col.Charset().MaxWidth()
		if length > 64*1024 {
			return sqltypes.Text
		}
	} else if tt == sqltypes.Blob || tt == sqltypes.TypeJSON {
		return sqltypes.Text
	}
	return sqltypes.VarChar
}

func (c *ConvertUsingExpr) eval(env *ExpressionEnv) (eval, error) {
	e, err := c.Inner.eval(env)
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, nil
	}
	e, err = evalToVarchar(e, c.Collation, true)
	if err != nil {
		// return NULL instead of error
		return nil, nil
	}
	return e, nil
}

func (c *ConvertUsingExpr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := c.Inner.typeof(env, fields)
	return sqltypes.VarChar, f | flagNullable
}
