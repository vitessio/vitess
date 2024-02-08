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
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	Column struct {
		Offset    int
		Type      sqltypes.Type
		Size      int32
		Scale     int32
		Collation collations.TypedCollation
		Original  sqlparser.Expr
		Nullable  bool

		// dynamicTypeOffset is set when the type of this column cannot be calculated
		// at translation time. Since expressions with dynamic types cannot be compiled ahead of time,
		// compilation will be delayed until the expression is first executed with the bind variables
		// sent by the user. See: UntypedExpr
		dynamicTypeOffset int
	}
)

var _ IR = (*Column)(nil)
var _ Expr = (*Column)(nil)

func (c *Column) IR() IR {
	return c
}

func (c *Column) IsExpr() {}

// eval implements the expression interface
func (c *Column) eval(env *ExpressionEnv) (eval, error) {
	return valueToEval(env.Row[c.Offset], c.Collation)
}

func (c *Column) typeof(env *ExpressionEnv) (ctype, error) {
	if c.typed() {
		var nullable typeFlag
		if c.Nullable {
			nullable = flagNullable
		}
		return ctype{Type: c.Type, Size: c.Size, Scale: c.Scale, Flag: nullable, Col: c.Collation}, nil
	}
	if c.Offset < len(env.Fields) {
		field := env.Fields[c.Offset]

		var f typeFlag
		if field.Flags&uint32(querypb.MySqlFlag_NOT_NULL_FLAG) == 0 {
			f = flagNullable
		}

		return ctype{
			Type: field.Type,
			Col:  typedCoercionCollation(field.Type, collations.ID(field.Charset)),
			Flag: f,
		}, nil
	}
	if c.Offset < len(env.Row) {
		value := env.Row[c.Offset]
		return ctype{Type: value.Type(), Flag: 0, Col: c.Collation}, nil
	}
	return ctype{}, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "no column at offset %d", c.Offset)
}

func (column *Column) compile(c *compiler) (ctype, error) {
	var typ ctype

	if column.typed() {
		typ.Type = column.Type
		typ.Col = column.Collation
		if column.Nullable {
			typ.Flag = flagNullable
		}
		typ.Size = column.Size
		typ.Scale = column.Scale
	} else if c.dynamicTypes != nil {
		typ = c.dynamicTypes[column.dynamicTypeOffset]
	} else {
		return ctype{}, c.unsupported(column)
	}

	if typ.Col.Collation != collations.CollationBinaryID {
		typ.Col.Repertoire = collations.RepertoireUnicode
	}

	switch tt := typ.Type; {
	case sqltypes.IsSigned(tt):
		c.asm.PushColumn_i(column.Offset)
		typ.Type = sqltypes.Int64
	case sqltypes.IsUnsigned(tt):
		c.asm.PushColumn_u(column.Offset)
		typ.Type = sqltypes.Uint64
	case sqltypes.IsFloat(tt):
		c.asm.PushColumn_f(column.Offset)
		typ.Type = sqltypes.Float64
	case sqltypes.IsDecimal(tt):
		c.asm.PushColumn_d(column.Offset)
	case sqltypes.IsText(tt):
		if tt == sqltypes.HexNum {
			c.asm.PushColumn_hexnum(column.Offset)
			typ.Type = sqltypes.VarBinary
		} else if tt == sqltypes.HexVal {
			c.asm.PushColumn_hexval(column.Offset)
			typ.Type = sqltypes.VarBinary
		} else {
			c.asm.PushColumn_text(column.Offset, typ.Col)
			typ.Type = sqltypes.VarChar
		}
	case sqltypes.IsBinary(tt):
		c.asm.PushColumn_bin(column.Offset)
		typ.Type = sqltypes.VarBinary
	case sqltypes.IsNull(tt):
		c.asm.PushNull()
	case tt == sqltypes.TypeJSON:
		c.asm.PushColumn_json(column.Offset)
	case tt == sqltypes.Datetime || tt == sqltypes.Timestamp:
		c.asm.PushColumn_datetime(column.Offset)
	case tt == sqltypes.Date:
		c.asm.PushColumn_date(column.Offset)
	case tt == sqltypes.Time:
		c.asm.PushColumn_time(column.Offset)
	default:
		return ctype{}, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "Type is not supported: %s", tt)
	}

	return typ, nil
}

func (column *Column) typed() bool {
	return column.Type >= 0
}
