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
	"vitess.io/vitess/go/vt/vterrors"
)

type frame func(vm *VirtualMachine) int

type compiler struct {
	asm              assembler
	fields           []*querypb.Field
	defaultCollation collations.ID
}

type AssemblerLog interface {
	Instruction(ins string, args ...any)
	Stack(old, new int)
}

type compiledCoercion struct {
	col   collations.Collation
	left  collations.Coercion
	right collations.Coercion
}

type CompilerOption func(c *compiler)

func WithAssemblerLog(log AssemblerLog) CompilerOption {
	return func(c *compiler) {
		c.asm.log = log
	}
}

func WithDefaultCollation(collation collations.ID) CompilerOption {
	return func(c *compiler) {
		c.defaultCollation = collation
	}
}

func Compile(expr Expr, fields []*querypb.Field, options ...CompilerOption) (*Program, error) {
	comp := compiler{
		fields:           fields,
		defaultCollation: collations.Default(),
	}
	for _, opt := range options {
		opt(&comp)
	}
	_, err := comp.compileExpr(expr)
	if err != nil {
		return nil, err
	}
	if comp.asm.stack.cur != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "bad compilation: stack pointer at %d after compilation", comp.asm.stack.cur)
	}
	return &Program{code: comp.asm.ins, original: expr, stack: comp.asm.stack.max}, nil
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

func (c *compiler) compileExpr(expr Expr) (ctype, error) {
	switch expr := expr.(type) {
	case *Literal:
		if expr.inner == nil {
			c.asm.PushNull()
		} else if err := c.asm.PushLiteral(expr.inner); err != nil {
			return ctype{}, err
		}

		t, f := expr.typeof(nil)
		return ctype{t, f, evalCollation(expr.inner)}, nil

	case *Column:
		return c.compileColumn(expr.Offset)

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

func (c *compiler) compileColumn(offset int) (ctype, error) {
	if offset >= len(c.fields) {
		return ctype{}, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "Missing field for column %d", offset)
	}

	field := c.fields[offset]
	col := collations.TypedCollation{
		Collation:    collations.ID(field.Charset),
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireASCII,
	}
	if col.Collation != collations.CollationBinaryID {
		col.Repertoire = collations.RepertoireUnicode
	}

	switch tt := field.Type; {
	case sqltypes.IsSigned(tt):
		c.asm.PushColumn_i(offset)
	case sqltypes.IsUnsigned(tt):
		c.asm.PushColumn_u(offset)
	case sqltypes.IsFloat(tt):
		c.asm.PushColumn_f(offset)
	case sqltypes.IsDecimal(tt):
		c.asm.PushColumn_d(offset)
	case sqltypes.IsText(tt):
		if tt == sqltypes.HexNum {
			c.asm.PushColumn_hexnum(offset)
		} else if tt == sqltypes.HexVal {
			c.asm.PushColumn_hexval(offset)
		} else {
			c.asm.PushColumn_text(offset, col)
		}
	case sqltypes.IsBinary(tt):
		c.asm.PushColumn_bin(offset)
	case sqltypes.IsNull(tt):
		c.asm.PushNull()
	case tt == sqltypes.TypeJSON:
		c.asm.PushColumn_json(offset)
	default:
		return ctype{}, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "Type is not supported: %s", tt)
	}

	var flag typeFlag
	if (field.Flags & uint32(querypb.MySqlFlag_NOT_NULL_FLAG)) == 0 {
		flag |= flagNullable
	}

	return ctype{
		Type: field.Type,
		Flag: flag,
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

func (c *compiler) compileToNumeric(ct ctype, offset int) ctype {
	if sqltypes.IsNumber(ct.Type) {
		return ct
	}
	if ct.Type == sqltypes.VarBinary && (ct.Flag&flagHex) != 0 {
		c.asm.Convert_hex(offset)
		return ctype{sqltypes.Uint64, ct.Flag, collationNumeric}
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
	if ct.Flag&flagNullable != 0 {
		j := c.asm.jumpFrom()
		c.asm.NullCheck1(j)
		return j
	}
	return nil
}

func (c *compiler) compileNullCheck2(lt, rt ctype) *jump {
	if lt.Flag&flagNullable != 0 || rt.Flag&flagNullable != 0 {
		j := c.asm.jumpFrom()
		c.asm.NullCheck2(j)
		return j
	}
	return nil
}
