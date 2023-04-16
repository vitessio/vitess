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
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type frame func(env *ExpressionEnv) int

type compiler struct {
	cfg *Config
	asm assembler
}

type CompilerLog interface {
	Instruction(ins string, args ...any)
	Stack(old, new int)
}

type compiledCoercion struct {
	col   collations.Collation
	left  collations.Coercion
	right collations.Coercion
}

type ctype struct {
	Type sqltypes.Type
	Flag typeFlag
	Col  collations.TypedCollation
}

func (ct ctype) nullable() bool {
	return ct.Flag&flagNullable != 0
}

func (ct ctype) isTextual() bool {
	return sqltypes.IsText(ct.Type) || sqltypes.IsBinary(ct.Type)
}

func (ct ctype) isHexOrBitLiteral() bool {
	return ct.Flag&flagBit != 0 || ct.Flag&flagHex != 0
}

func (c *compiler) unsupported(expr Expr) error {
	return vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "unsupported compilation for expression '%s'", FormatExpr(expr))
}

func (c *compiler) compile(expr Expr) (ctype, error) {
	ct, err := c.compileExpr(expr)
	if err != nil {
		return ctype{}, err
	}
	if c.asm.stack.cur != 1 {
		return ctype{}, vterrors.Errorf(vtrpc.Code_INTERNAL, "bad compilation: stack pointer at %d after compilation", c.asm.stack.cur)
	}
	return ct, nil
}

func (c *compiler) compileExpr(expr Expr) (ctype, error) {
	switch expr := expr.(type) {
	case *Literal:
		if expr.inner == nil {
			c.asm.PushNull()
		} else if err := c.asm.PushLiteral(expr.inner); err != nil {
			return ctype{}, err
		}

		t, f := expr.typeof(nil, nil)
		return ctype{t, f, evalCollation(expr.inner)}, nil

	case *BindVariable:
		return c.compileBindVar(expr)

	case *Column:
		return c.compileColumn(expr)

	case *ArithmeticExpr:
		return c.compileArithmetic(expr)

	case *BitwiseExpr:
		return c.compileBitwise(expr)

	case *BitwiseNotExpr:
		return c.compileBitwiseNot(expr)

	case *NegateExpr:
		return c.compileNegate(expr)

	case *ComparisonExpr:
		return c.compileComparison(expr)

	case *CollateExpr:
		return c.compileCollate(expr)

	case *ConvertExpr:
		return c.compileConvert(expr)

	case *ConvertUsingExpr:
		return c.compileConvertUsing(expr)

	case *CaseExpr:
		return c.compileCase(expr)

	case *LikeExpr:
		return c.compileLike(expr)

	case *IsExpr:
		return c.compileIs(expr)

	case *InExpr:
		return c.compileIn(expr)

	case *NotExpr:
		return c.compileNot(expr)

	case *LogicalExpr:
		return c.compileLogical(expr)

	case callable:
		return c.compileFn(expr)

	case TupleExpr:
		return c.compileTuple(expr)

	default:
		return ctype{}, c.unsupported(expr)
	}
}

func (c *compiler) compileBindVar(bvar *BindVariable) (ctype, error) {
	if !bvar.typed {
		return ctype{}, c.unsupported(bvar)
	}

	switch tt := bvar.Type; {
	case sqltypes.IsSigned(tt):
		c.asm.PushBVar_i(bvar.Key)
	case sqltypes.IsUnsigned(tt):
		c.asm.PushBVar_u(bvar.Key)
	case sqltypes.IsFloat(tt):
		c.asm.PushBVar_f(bvar.Key)
	case sqltypes.IsDecimal(tt):
		c.asm.PushBVar_d(bvar.Key)
	case sqltypes.IsText(tt):
		if tt == sqltypes.HexNum {
			c.asm.PushBVar_hexnum(bvar.Key)
		} else if tt == sqltypes.HexVal {
			c.asm.PushBVar_hexval(bvar.Key)
		} else {
			c.asm.PushBVar_text(bvar.Key, bvar.Collation)
		}
	case sqltypes.IsBinary(tt):
		c.asm.PushBVar_bin(bvar.Key)
	case sqltypes.IsNull(tt):
		c.asm.PushNull()
	case tt == sqltypes.TypeJSON:
		c.asm.PushBVar_json(bvar.Key)
	default:
		return ctype{}, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "Type is not supported: %s", tt)
	}

	return ctype{
		Type: bvar.Type,
		Col:  bvar.Collation,
	}, nil
}

func (c *compiler) compileColumn(column *Column) (ctype, error) {
	if !column.typed {
		return ctype{}, c.unsupported(column)
	}

	col := column.Collation
	if col.Collation != collations.CollationBinaryID {
		col.Repertoire = collations.RepertoireUnicode
	}

	switch tt := column.Type; {
	case sqltypes.IsSigned(tt):
		c.asm.PushColumn_i(column.Offset)
	case sqltypes.IsUnsigned(tt):
		c.asm.PushColumn_u(column.Offset)
	case sqltypes.IsFloat(tt):
		c.asm.PushColumn_f(column.Offset)
	case sqltypes.IsDecimal(tt):
		c.asm.PushColumn_d(column.Offset)
	case sqltypes.IsText(tt):
		if tt == sqltypes.HexNum {
			c.asm.PushColumn_hexnum(column.Offset)
		} else if tt == sqltypes.HexVal {
			c.asm.PushColumn_hexval(column.Offset)
		} else {
			c.asm.PushColumn_text(column.Offset, col)
		}
	case sqltypes.IsBinary(tt):
		c.asm.PushColumn_bin(column.Offset)
	case sqltypes.IsNull(tt):
		c.asm.PushNull()
	case tt == sqltypes.TypeJSON:
		c.asm.PushColumn_json(column.Offset)
	default:
		return ctype{}, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "Type is not supported: %s", tt)
	}

	return ctype{
		Type: column.Type,
		Flag: flagNullable,
		Col:  col,
	}, nil
}

func (c *compiler) compileTuple(tuple TupleExpr) (ctype, error) {
	for _, arg := range tuple {
		_, err := c.compileExpr(arg)
		if err != nil {
			return ctype{}, err
		}
	}
	c.asm.PackTuple(len(tuple))
	return ctype{Type: sqltypes.Tuple, Col: collationBinary}, nil
}

func (c *compiler) compileToNumeric(ct ctype, offset int, fallback sqltypes.Type) ctype {
	if sqltypes.IsNumber(ct.Type) {
		return ct
	}
	if ct.Type == sqltypes.VarBinary && (ct.Flag&flagHex) != 0 {
		c.asm.Convert_hex(offset)
		return ctype{sqltypes.Uint64, ct.Flag, collationNumeric}
	}

	if sqltypes.IsDateOrTime(ct.Type) {
		c.asm.Convert_Ti(offset)
		return ctype{sqltypes.Int64, ct.Flag, collationNumeric}
	}

	switch fallback {
	case sqltypes.Int64:
		c.asm.Convert_xi(offset)
		return ctype{sqltypes.Int64, ct.Flag, collationNumeric}
	case sqltypes.Uint64:
		c.asm.Convert_xu(offset)
		return ctype{sqltypes.Uint64, ct.Flag, collationNumeric}
	case sqltypes.Decimal:
		c.asm.Convert_xd(offset, 0, 0)
		return ctype{sqltypes.Decimal, ct.Flag, collationNumeric}
	}
	c.asm.Convert_xf(offset)
	return ctype{sqltypes.Float64, ct.Flag, collationNumeric}
}

func (c *compiler) compileToInt64(ct ctype, offset int) ctype {
	switch ct.Type {
	case sqltypes.Int64:
		return ct
	case sqltypes.Uint64:
		c.asm.Convert_ui(offset)
	// TODO: specialization
	default:
		c.asm.Convert_xi(offset)
	}
	return ctype{sqltypes.Int64, ct.Flag, collationNumeric}
}

func (c *compiler) compileToUint64(ct ctype, offset int) ctype {
	switch ct.Type {
	case sqltypes.Uint64:
		return ct
	case sqltypes.Int64:
		c.asm.Convert_iu(offset)
	// TODO: specialization
	default:
		c.asm.Convert_xu(offset)
	}
	return ctype{sqltypes.Uint64, ct.Flag, collationNumeric}
}

func (c *compiler) compileToBitwiseUint64(ct ctype, offset int) ctype {
	switch ct.Type {
	case sqltypes.Uint64:
		return ct
	case sqltypes.Int64:
		c.asm.Convert_iu(offset)
	case sqltypes.Decimal:
		c.asm.Convert_dbit(offset)
	// TODO: specialization
	default:
		c.asm.Convert_xu(offset)
	}
	return ctype{sqltypes.Uint64, ct.Flag, collationNumeric}
}

func (c *compiler) compileToFloat(ct ctype, offset int) ctype {
	if sqltypes.IsFloat(ct.Type) {
		return ct
	}
	switch ct.Type {
	case sqltypes.Int64:
		c.asm.Convert_if(offset)
	case sqltypes.Uint64:
		// only emit u->f conversion if this is not a hex value; hex values
		// will already be converted
		c.asm.Convert_uf(offset)
	default:
		c.asm.Convert_xf(offset)
	}
	return ctype{sqltypes.Float64, ct.Flag, collationNumeric}
}

func (c *compiler) compileToDecimal(ct ctype, offset int) ctype {
	if sqltypes.IsDecimal(ct.Type) {
		return ct
	}
	switch ct.Type {
	case sqltypes.Int64:
		c.asm.Convert_id(offset)
	case sqltypes.Uint64:
		c.asm.Convert_ud(offset)
	default:
		c.asm.Convert_xd(offset, 0, 0)
	}
	return ctype{sqltypes.Decimal, ct.Flag, collationNumeric}
}

func (c *compiler) compileNullCheck1(ct ctype) *jump {
	if ct.nullable() {
		j := c.asm.jumpFrom()
		c.asm.NullCheck1(j)
		return j
	}
	return nil
}

func (c *compiler) compileNullCheck1r(ct ctype) *jump {
	if ct.nullable() {
		j := c.asm.jumpFrom()
		c.asm.NullCheck1r(j)
		return j
	}
	return nil
}

func (c *compiler) compileNullCheck2(lt, rt ctype) *jump {
	if lt.nullable() || rt.nullable() {
		j := c.asm.jumpFrom()
		c.asm.NullCheck2(j)
		return j
	}
	return nil
}

func (c *compiler) compileNullCheck3(arg1, arg2, arg3 ctype) *jump {
	if arg1.nullable() || arg2.nullable() || arg3.nullable() {
		j := c.asm.jumpFrom()
		c.asm.NullCheck3(j)
		return j
	}
	return nil
}
