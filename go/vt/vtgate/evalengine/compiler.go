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
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
)

type frame func(env *ExpressionEnv) int

type compiler struct {
	collation    collations.ID
	dynamicTypes []ctype
	asm          assembler
	sqlmode      SQLMode
	env          *vtenv.Environment
}

type CompilerLog interface {
	Instruction(ins string, args ...any)
	Stack(old, new int)
}

type compiledCoercion struct {
	col   colldata.Collation
	left  colldata.Coercion
	right colldata.Coercion
}

type ctype struct {
	Type        sqltypes.Type
	Flag        typeFlag
	Size, Scale int32
	Col         collations.TypedCollation
}

type Type struct {
	typ         sqltypes.Type
	collation   collations.ID
	nullable    bool
	init        bool
	size, scale int32
}

func NewType(t sqltypes.Type, collation collations.ID) Type {
	// New types default to being nullable
	return NewTypeEx(t, collation, true, 0, 0)
}

func NewTypeEx(t sqltypes.Type, collation collations.ID, nullable bool, size, scale int32) Type {
	return Type{
		typ:       t,
		collation: collation,
		nullable:  nullable,
		init:      true,
		size:      size,
		scale:     scale,
	}
}

func NewTypeFromField(f *querypb.Field) Type {
	return Type{
		typ:       f.Type,
		collation: collations.ID(f.Charset),
		nullable:  f.Flags&uint32(querypb.MySqlFlag_NOT_NULL_FLAG) == 0,
		init:      true,
		size:      int32(f.ColumnLength),
		scale:     int32(f.Decimals),
	}
}

func (t *Type) ToField(name string) *querypb.Field {
	// need to get the proper flags for the type; usually leaving flags
	// to 0 is OK, because Vitess' MySQL client will generate the right
	// ones for the column's type, but here we're also setting the NotNull
	// flag, so it needs to be set with the full flags for the column
	_, flags := sqltypes.TypeToMySQL(t.typ)
	if !t.nullable {
		flags |= int64(querypb.MySqlFlag_NOT_NULL_FLAG)
	}

	f := &querypb.Field{
		Name:         name,
		Type:         t.typ,
		Charset:      uint32(t.collation),
		ColumnLength: uint32(t.size),
		Decimals:     uint32(t.scale),
		Flags:        uint32(flags),
	}
	return f
}

func (t *Type) Type() sqltypes.Type {
	if t.init {
		return t.typ
	}
	return sqltypes.Unknown
}

func (t *Type) Collation() collations.ID {
	return t.collation
}

func (t *Type) Size() int32 {
	return t.size
}

func (t *Type) Scale() int32 {
	return t.scale
}

func (t *Type) Nullable() bool {
	if t.init {
		return t.nullable
	}
	return true // nullable by default for unknown types
}

func (t *Type) Valid() bool {
	return t.init
}

func (ct ctype) nullable() bool {
	return ct.Flag&flagNullable != 0
}

func (ct ctype) isTextual() bool {
	return sqltypes.IsTextOrBinary(ct.Type)
}

func (ct ctype) isHexOrBitLiteral() bool {
	return ct.Flag&flagBit != 0 || ct.Flag&flagHex != 0
}

func (c *compiler) unsupported(expr IR) error {
	buf := sqlparser.NewTrackedBuffer(nil)
	expr.format(buf)
	return vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "unsupported compilation for IR '%s'", buf.String())
}

func (c *compiler) compile(expr IR) (*CompiledExpr, error) {
	ct, err := expr.compile(c)
	if err != nil {
		return nil, err
	}
	if c.asm.stack.cur != 1 {
		sql := sqlparser.NewTrackedBuffer(nil)
		expr.format(sql)
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL,
			"bad compilation: stack pointer at %d after compilation (expr: %s)",
			c.asm.stack.cur, sql.String())
	}
	return &CompiledExpr{code: c.asm.ins, ir: expr, stack: c.asm.stack.max, typed: ct}, nil
}

func (c *compiler) compileToNumeric(ct ctype, offset int, fallback sqltypes.Type, preciseDatetime bool) ctype {
	if sqltypes.IsNumber(ct.Type) {
		return ct
	}
	if ct.Type == sqltypes.VarBinary {
		if (ct.Flag & flagHex) != 0 {
			c.asm.Convert_hex(offset)
			return ctype{Type: sqltypes.Uint64, Flag: ct.Flag, Col: collationNumeric}
		}
		if (ct.Flag & flagBit) != 0 {
			c.asm.Convert_bit(offset)
			return ctype{Type: sqltypes.Int64, Flag: ct.Flag, Col: collationNumeric}
		}
	}

	if sqltypes.IsDateOrTime(ct.Type) {
		if preciseDatetime {
			if ct.Size == 0 {
				c.asm.Convert_Ti(offset)
				return ctype{Type: sqltypes.Int64, Flag: ct.Flag, Col: collationNumeric}
			}
			c.asm.Convert_Td(offset)
			return ctype{Type: sqltypes.Decimal, Flag: ct.Flag, Col: collationNumeric, Size: ct.Size}
		}
		c.asm.Convert_Tf(offset)
		return ctype{Type: sqltypes.Float64, Flag: ct.Flag, Col: collationNumeric}
	}

	switch fallback {
	case sqltypes.Int64:
		c.asm.Convert_xi(offset)
		return ctype{Type: sqltypes.Int64, Flag: ct.Flag, Col: collationNumeric}
	case sqltypes.Uint64:
		c.asm.Convert_xu(offset)
		return ctype{Type: sqltypes.Uint64, Flag: ct.Flag, Col: collationNumeric}
	case sqltypes.Decimal:
		c.asm.Convert_xd(offset, 0, 0)
		return ctype{Type: sqltypes.Decimal, Flag: ct.Flag, Col: collationNumeric}
	}
	c.asm.Convert_xf(offset)
	return ctype{Type: sqltypes.Float64, Flag: ct.Flag, Col: collationNumeric}
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
	return ctype{Type: sqltypes.Int64, Flag: ct.Flag, Col: collationNumeric}
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
	return ctype{Type: sqltypes.Uint64, Flag: ct.Flag, Col: collationNumeric}
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
	return ctype{Type: sqltypes.Uint64, Flag: ct.Flag, Col: collationNumeric}
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
	return ctype{Type: sqltypes.Float64, Flag: ct.Flag, Col: collationNumeric}
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
	return ctype{Type: sqltypes.Decimal, Flag: ct.Flag, Col: collationNumeric}
}

func (c *compiler) compileToDate(doct ctype, offset int) ctype {
	switch doct.Type {
	case sqltypes.Date:
		return doct
	default:
		c.asm.Convert_xD(offset, c.sqlmode.AllowZeroDate())
	}
	return ctype{Type: sqltypes.Date, Col: collationBinary, Flag: flagNullable}
}

func (c *compiler) compileToDateTime(doct ctype, offset, prec int) ctype {
	switch doct.Type {
	case sqltypes.Datetime:
		c.asm.Convert_tp(offset, prec)
		return doct
	default:
		c.asm.Convert_xDT(offset, prec, c.sqlmode.AllowZeroDate())
	}
	return ctype{Type: sqltypes.Datetime, Size: int32(prec), Col: collationBinary, Flag: flagNullable}
}

func (c *compiler) compileToTime(doct ctype, offset, prec int) ctype {
	switch doct.Type {
	case sqltypes.Time:
		c.asm.Convert_tp(offset, prec)
		return doct
	default:
		c.asm.Convert_xT(offset, prec)
	}
	return ctype{Type: sqltypes.Time, Size: int32(prec), Col: collationBinary, Flag: flagNullable}
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

func (c *compiler) compileNullCheck4(arg1, arg2, arg3, arg4 ctype) *jump {
	if arg1.nullable() || arg2.nullable() || arg3.nullable() || arg4.nullable() {
		j := c.asm.jumpFrom()
		c.asm.NullCheck4(j)
		return j
	}
	return nil
}

func (c *compiler) compileNullCheckArg(ct ctype, offset int) *jump {
	if ct.nullable() {
		j := c.asm.jumpFrom()
		c.asm.NullCheckArg(j, offset)
		return j
	}
	return nil
}

func (c *compiler) compileNullCheckOffset(ct ctype, offset int) *jump {
	if ct.nullable() {
		j := c.asm.jumpFrom()
		c.asm.NullCheckOffset(j, offset)
		return j
	}
	return nil
}

func (c *compiler) compileNumericPriority(lt, rt ctype) (ctype, ctype, bool) {
	switch lt.Type {
	case sqltypes.Int64:
		if rt.Type == sqltypes.Uint64 || rt.Type == sqltypes.Float64 || rt.Type == sqltypes.Decimal {
			return rt, lt, true
		}
	case sqltypes.Uint64:
		if rt.Type == sqltypes.Float64 || rt.Type == sqltypes.Decimal {
			return rt, lt, true
		}
	case sqltypes.Decimal:
		if rt.Type == sqltypes.Float64 {
			return rt, lt, true
		}
	}
	return lt, rt, false
}

func (c *compiler) compareNumericTypes(lt ctype, rt ctype) (swapped bool) {
	switch lt.Type {
	case sqltypes.Int64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.CmpNum_ii()
		case sqltypes.Uint64:
			c.asm.CmpNum_iu(2, 1)
		case sqltypes.Float64:
			c.asm.CmpNum_if(2, 1)
		case sqltypes.Decimal:
			c.asm.CmpNum_id(2, 1)
		}
	case sqltypes.Uint64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.CmpNum_iu(1, 2)
			swapped = true
		case sqltypes.Uint64:
			c.asm.CmpNum_uu()
		case sqltypes.Float64:
			c.asm.CmpNum_uf(2, 1)
		case sqltypes.Decimal:
			c.asm.CmpNum_ud(2, 1)
		}
	case sqltypes.Float64:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.CmpNum_if(1, 2)
			swapped = true
		case sqltypes.Uint64:
			c.asm.CmpNum_uf(1, 2)
			swapped = true
		case sqltypes.Float64:
			c.asm.CmpNum_ff()
		case sqltypes.Decimal:
			c.asm.CmpNum_fd(2, 1)
		}

	case sqltypes.Decimal:
		switch rt.Type {
		case sqltypes.Int64:
			c.asm.CmpNum_id(1, 2)
			swapped = true
		case sqltypes.Uint64:
			c.asm.CmpNum_ud(1, 2)
			swapped = true
		case sqltypes.Float64:
			c.asm.CmpNum_fd(1, 2)
			swapped = true
		case sqltypes.Decimal:
			c.asm.CmpNum_dd()
		}
	}
	return
}

func (c *compiler) compareAsStrings(lt ctype, rt ctype) error {
	merged, coerceLeft, coerceRight, err := mergeCollations(lt.Col, rt.Col, lt.Type, rt.Type, c.env.CollationEnv())
	if err != nil {
		return err
	}
	if coerceLeft == nil && coerceRight == nil {
		c.asm.CmpString_collate(colldata.Lookup(merged.Collation))
	} else {
		if coerceLeft == nil {
			coerceLeft = func(dst, in []byte) ([]byte, error) { return in, nil }
		}
		if coerceRight == nil {
			coerceRight = func(dst, in []byte) ([]byte, error) { return in, nil }
		}
		c.asm.CmpString_coerce(&compiledCoercion{
			col:   colldata.Lookup(merged.Collation),
			left:  coerceLeft,
			right: coerceRight,
		})
	}
	return nil
}

func isEncodingJSONSafe(col collations.ID) bool {
	switch colldata.Lookup(col).Charset().(type) {
	case charset.Charset_utf8mb4, charset.Charset_utf8mb3, charset.Charset_binary:
		return true
	default:
		return false
	}
}

func (c *compiler) compileParseJSON(fn string, doct ctype, offset int) (ctype, error) {
	switch doct.Type {
	case sqltypes.TypeJSON:
	case sqltypes.VarChar, sqltypes.VarBinary:
		c.asm.Parse_j(offset)
	default:
		return ctype{}, errJSONType(fn)
	}
	return ctype{Type: sqltypes.TypeJSON, Flag: doct.Flag, Col: collationJSON}, nil
}

func (c *compiler) compileToJSON(doct ctype, offset int) (ctype, error) {
	switch doct.Type {
	case sqltypes.TypeJSON:
		return doct, nil
	case sqltypes.Float64:
		c.asm.Convert_fj(offset)
	case sqltypes.Int64:
		c.asm.Convert_ij(offset, doct.Flag&flagIsBoolean != 0)
	case sqltypes.Uint64:
		c.asm.Convert_uj(offset)
	case sqltypes.Decimal:
		c.asm.Convert_dj(offset)
	case sqltypes.VarChar:
		c.asm.Convert_cj(offset)
	case sqltypes.VarBinary:
		c.asm.Convert_bj(offset)
	case sqltypes.Null:
		c.asm.Convert_Nj(offset)
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Timestamp, sqltypes.Time:
		c.asm.Convert_Tj(offset)
	default:
		return ctype{}, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "Unsupported type conversion: %s AS JSON", doct.Type)
	}
	return ctype{Type: sqltypes.TypeJSON, Col: collationJSON}, nil
}

func (c *compiler) compileArgToJSON(doct ctype, offset int) (ctype, error) {
	switch doct.Type {
	case sqltypes.TypeJSON:
		return doct, nil
	case sqltypes.Float64:
		c.asm.Convert_fj(offset)
	case sqltypes.Int64:
		c.asm.Convert_ij(offset, doct.Flag&flagIsBoolean != 0)
	case sqltypes.Uint64:
		c.asm.Convert_uj(offset)
	case sqltypes.Decimal:
		c.asm.Convert_dj(offset)
	case sqltypes.VarChar:
		c.asm.ConvertArg_cj(offset)
	case sqltypes.VarBinary:
		c.asm.Convert_bj(offset)
	case sqltypes.Null:
		c.asm.Convert_Nj(offset)
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Timestamp, sqltypes.Time:
		c.asm.Convert_Tj(offset)
	default:
		return ctype{}, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "Unsupported type conversion: %s AS JSON", doct.Type)
	}
	return ctype{Type: sqltypes.TypeJSON, Col: collationJSON}, nil
}

func (c *compiler) compileToJSONKey(key ctype) error {
	if key.Type == sqltypes.Null {
		return errJSONKeyIsNil
	}
	if key.Type == sqltypes.VarChar && isEncodingJSONSafe(key.Col.Collation) {
		return nil
	}
	if key.Type == sqltypes.VarBinary {
		return nil
	}
	c.asm.Convert_xc(1, sqltypes.VarChar, c.collation, nil)
	return nil
}

func (c *compiler) jsonExtractPath(expr IR) (*json.Path, error) {
	path, ok := expr.(*Literal)
	if !ok {
		return nil, errJSONPath
	}
	pathBytes, ok := path.inner.(*evalBytes)
	if !ok {
		return nil, errJSONPath
	}
	var parser json.PathParser
	return parser.ParseBytes(pathBytes.bytes)
}

func (c *compiler) jsonExtractOneOrAll(fname string, expr IR) (jsonMatch, error) {
	lit, ok := expr.(*Literal)
	if !ok {
		return jsonMatchInvalid, errOneOrAll(fname)
	}
	b, ok := lit.inner.(*evalBytes)
	if !ok {
		return jsonMatchInvalid, errOneOrAll(fname)
	}
	return intoOneOrAll(fname, b.string())
}

func (c *compiler) compareAsJSON(lt ctype, rt ctype) error {
	_, err := c.compileArgToJSON(lt, 2)
	if err != nil {
		return err
	}

	_, err = c.compileArgToJSON(rt, 1)
	if err != nil {
		return err
	}
	c.asm.CmpJSON()

	return nil
}

func (c *compiler) compileCheckTrue(when ctype, offset int) error {
	switch when.Type {
	case sqltypes.Int64:
		c.asm.Convert_iB(offset)
	case sqltypes.Uint64:
		c.asm.Convert_uB(offset)
	case sqltypes.Float64:
		c.asm.Convert_fB(offset)
	case sqltypes.Decimal:
		c.asm.Convert_dB(offset)
	case sqltypes.VarChar, sqltypes.VarBinary:
		c.asm.Convert_bB(offset)
	case sqltypes.Timestamp, sqltypes.Datetime, sqltypes.Time, sqltypes.Date:
		c.asm.Convert_TB(offset)
	case sqltypes.Null:
		c.asm.SetBool(offset, false)
	default:
		return vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "unsupported Truth check: %s", when.Type)
	}
	return nil
}
