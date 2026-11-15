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
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
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
	value := env.Row[c.Offset]
	collation := c.rowCollation(env)
	if env.FieldCharsets && sqltypes.IsText(value.Type()) {
		value = sqltypes.MakeTrusted(value.Type(), env.columnText(c.Offset, value.Raw(), collation.Collation))
	}
	return valueToEval(value, collation, c.Values)
}

// rowCollation returns the collation that the value of the column is
// evaluated in. It is the one of the column when the planner knew its type.
//
// A column without a type has no collation, and its text is evaluated as
// bytes. That gives the right answer for an exact match while the text is in
// the connection character set, as the literals are. When the field of the
// column says that the text is in another character set, as it is in a row
// that MySQL sent with character_set_results = NULL, the bytes of 'Müller' in
// a latin1 column, 4D FC 6C 6C 65 72, do not match the literal 'Müller', and
// CONCAT() with a literal fails. The column is then evaluated in the
// collation of its field. Only an evaluation that was given the fields of its
// rows can do this.
func (c *Column) rowCollation(env *ExpressionEnv) collations.TypedCollation {
	if c.typed() || c.Offset >= len(env.Fields) {
		return c.Collation
	}
	field := env.Fields[c.Offset]
	id := collations.ID(field.Charset)
	if id == collations.Unknown || id == collations.CollationBinaryID {
		return c.Collation
	}
	coll := colldata.Lookup(id)
	if coll == nil {
		return c.Collation
	}
	switch coll.Charset().(type) {
	case charset.Charset_utf8mb4, charset.Charset_utf8mb3:
		return c.Collation
	}
	return typedColumnCollation(field.Type, id)
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
		return ctype{Type: value.Type(), Flag: 0, Col: c.Collation, Values: c.Values}, nil
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
		typ.Values = column.Values
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
	case tt == sqltypes.Enum:
		c.asm.PushColumn_enum(column.Offset, column.Values)
	case tt == sqltypes.Set:
		c.asm.PushColumn_set(column.Offset, column.Values)
	case sqltypes.IsText(tt):
		switch tt {
		case sqltypes.HexNum:
			c.asm.PushColumn_hexnum(column.Offset)
			typ.Type = sqltypes.VarBinary
		case sqltypes.HexVal:
			c.asm.PushColumn_hexval(column.Offset)
			typ.Type = sqltypes.VarBinary
		default:
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

	return typ, nil
}

func (column *Column) typed() bool {
	return column.Type >= 0
}
