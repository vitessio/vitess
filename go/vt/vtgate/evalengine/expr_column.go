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
		Values    *EnumSetValues // For ENUM and SET types

		// dynamicTypeOffset is set when the type of this column cannot be calculated
		// at translation time. Since expressions with dynamic types cannot be compiled ahead of time,
		// compilation will be delayed until the expression is first executed with the bind variables
		// sent by the user. See: UntypedExpr
		dynamicTypeOffset int
	}
)

var (
	_ IR   = (*Column)(nil)
	_ Expr = (*Column)(nil)
)

func (c *Column) IR() IR {
	return c
}

func (c *Column) IsExpr() {}

// eval implements the expression interface
func (c *Column) eval(env *ExpressionEnv) (eval, error) {
	e, err := valueToEval(env.Row[c.Offset], c.Collation, c.Values)
	if b, ok := e.(*evalBytes); ok && b.isBinary() && b.flag&(flagHex|flagBit) == 0 {
		// valueToEval gives a binary string the literal's coercibility:
		// a binary column has the implicit one, like a text column.
		b.col = collationBinaryImplicit
	}
	return e, err
}

func (c *Column) typeof(env *ExpressionEnv) (ctype, error) {
	if c.typed() {
		var nullable typeFlag
		if c.Nullable {
			nullable = flagNullable
		}
		return ctype{Type: c.Type, Size: c.Size, Scale: c.Scale, Flag: nullable, Col: c.Collation, Values: c.Values}, nil
	}
	if c.Offset < len(env.Fields) {
		field := env.Fields[c.Offset]

		var f typeFlag
		if field.Flags&uint32(querypb.MySqlFlag_NOT_NULL_FLAG) == 0 {
			f = flagNullable
		}

		return ctype{
			Type:  field.Type,
			Col:   typedColumnCollation(field.Type, collations.ID(field.Charset)),
			Flag:  f,
			Size:  int32(field.ColumnLength),
			Scale: int32(field.Decimals),
		}, nil
	}
	if c.Offset < len(env.Row) {
		value := env.Row[c.Offset]
		var f typeFlag
		if value.IsNull() {
			f = flagNull | flagNullable
		}
		col := c.Collation
		if value.Type() == sqltypes.TypeJSON {
			// as evalCollation does for the value in the interpreter
			col = collationJSON
		}
		return ctype{Type: value.Type(), Flag: f, Col: col, Values: c.Values}, nil
	}
	return ctype{}, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "no column at offset %d", c.Offset)
}

func (column *Column) compile(c *compiler) (ctype, error) {
	var typ ctype

	if column.typed() {
		typ = column.typedType()
	} else if c.dynamicTypes != nil {
		typ = c.dynamicTypes[column.dynamicTypeOffset]
	} else {
		return ctype{}, c.unsupported(column)
	}

	switch tt := typ.Type; {
	case sqltypes.IsSigned(tt):
		c.asm.PushColumn_i(column.Offset)
	case sqltypes.IsUnsigned(tt):
		c.asm.PushColumn_u(column.Offset)
	case sqltypes.IsFloat(tt):
		c.asm.PushColumn_f(column.Offset)
	case sqltypes.IsDecimal(tt):
		c.asm.PushColumn_d(column.Offset)
	case tt == sqltypes.Enum:
		c.asm.PushColumn_enum(column.Offset, column.Values)
	case tt == sqltypes.Set:
		c.asm.PushColumn_set(column.Offset, column.Values)
	case sqltypes.IsText(tt):
		switch tt {
		case sqltypes.HexNum:
			c.asm.PushColumn_hexnum(column.Offset)
		case sqltypes.HexVal:
			c.asm.PushColumn_hexval(column.Offset)
		default:
			c.asm.PushColumn_text(column.Offset, compiledColumnType(typ).Col)
		}
	case sqltypes.IsBinary(tt):
		c.asm.PushColumn_bin(column.Offset)
	case sqltypes.IsNull(tt):
		c.asm.PushNull()
	case tt == sqltypes.TypeJSON:
		c.asm.PushColumn_json(column.Offset)
	case tt == sqltypes.Datetime:
		c.asm.PushColumn_datetime(column.Offset)
	case tt == sqltypes.Timestamp:
		c.asm.PushColumn_timestamp(column.Offset)
	case tt == sqltypes.Date:
		c.asm.PushColumn_date(column.Offset)
	case tt == sqltypes.Time:
		c.asm.PushColumn_time(column.Offset)
	case tt == sqltypes.Vector:
		c.asm.PushColumn_vector(column.Offset)
	default:
		return ctype{}, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "Type is not supported: %s", tt)
	}

	return compiledColumnType(typ), nil
}

// typedType returns the declared type of a typed column.
func (column *Column) typedType() ctype {
	typ := ctype{
		Type:   column.Type,
		Col:    column.Collation,
		Size:   column.Size,
		Scale:  column.Scale,
		Values: column.Values,
	}
	if column.Nullable {
		typ.Flag = flagNullable
	}
	return typ
}

// compiledColumnType returns the type of the value that the compiled
// program pushes for a column of type typ: an INT column is pushed as an
// INT64 and a TEXT column as a VARCHAR.
func compiledColumnType(typ ctype) ctype {
	if typ.Col.Collation != collations.CollationBinaryID {
		typ.Col.Repertoire = collations.RepertoireUnicode
	}
	switch tt := typ.Type; {
	case sqltypes.IsSigned(tt):
		typ.Type = sqltypes.Int64
	case sqltypes.IsUnsigned(tt):
		typ.Type = sqltypes.Uint64
	case sqltypes.IsFloat(tt):
		typ.Type = sqltypes.Float64
	case tt == sqltypes.HexNum, tt == sqltypes.HexVal:
		typ.Type = sqltypes.VarBinary
	case sqltypes.IsText(tt) && tt != sqltypes.Enum && tt != sqltypes.Set:
		typ.Type = sqltypes.VarChar
	case sqltypes.IsBinary(tt):
		typ.Type = sqltypes.VarBinary
		typ.Col = collationBinaryImplicit
	}
	return typ
}

func (column *Column) typed() bool {
	return column.Type >= 0
}
