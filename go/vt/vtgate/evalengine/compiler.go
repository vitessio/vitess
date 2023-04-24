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
	"vitess.io/vitess/go/mysql/json"
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
	ct, err := expr.compile(c)
	if err != nil {
		return ctype{}, err
	}
	if c.asm.stack.cur != 1 {
		return ctype{}, vterrors.Errorf(vtrpc.Code_INTERNAL, "bad compilation: stack pointer at %d after compilation", c.asm.stack.cur)
	}
	return ct, nil
}

func (c *compiler) compileToNumeric(ct ctype, offset int, fallback sqltypes.Type, preciseDatetime bool) ctype {
	if sqltypes.IsNumber(ct.Type) {
		return ct
	}
	if ct.Type == sqltypes.VarBinary && (ct.Flag&flagHex) != 0 {
		c.asm.Convert_hex(offset)
		return ctype{sqltypes.Uint64, ct.Flag, collationNumeric}
	}

	if sqltypes.IsDateOrTime(ct.Type) {
		if preciseDatetime {
			c.asm.Convert_Ti(offset)
			return ctype{sqltypes.Int64, ct.Flag, collationNumeric}
		}
		c.asm.Convert_Tf(offset)
		return ctype{sqltypes.Float64, ct.Flag, collationNumeric}
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

func (c *compiler) compileToDate(doct ctype, offset int) ctype {
	switch doct.Type {
	case sqltypes.Date:
		return doct
	default:
		c.asm.Convert_xD(offset)
	}
	return ctype{Type: sqltypes.Date, Col: collationBinary, Flag: flagNullable}
}

func (c *compiler) compileToDateTime(doct ctype, offset, prec int) ctype {
	switch doct.Type {
	case sqltypes.Datetime:
		c.asm.Convert_tp(offset, prec)
		return doct
	default:
		c.asm.Convert_xDT(offset, prec)
	}
	return ctype{Type: sqltypes.Datetime, Col: collationBinary, Flag: flagNullable}
}

func (c *compiler) compileToTime(doct ctype, offset, prec int) ctype {
	switch doct.Type {
	case sqltypes.Time:
		c.asm.Convert_tp(offset, prec)
		return doct
	default:
		c.asm.Convert_xT(offset, prec)
	}
	return ctype{Type: sqltypes.Time, Col: collationBinary, Flag: flagNullable}
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
	merged, coerceLeft, coerceRight, err := mergeCollations(lt.Col, rt.Col, lt.Type, rt.Type)
	if err != nil {
		return err
	}
	if coerceLeft == nil && coerceRight == nil {
		c.asm.CmpString_collate(merged.Collation.Get())
	} else {
		if coerceLeft == nil {
			coerceLeft = func(dst, in []byte) ([]byte, error) { return in, nil }
		}
		if coerceRight == nil {
			coerceRight = func(dst, in []byte) ([]byte, error) { return in, nil }
		}
		c.asm.CmpString_coerce(&compiledCoercion{
			col:   merged.Collation.Get(),
			left:  coerceLeft,
			right: coerceRight,
		})
	}
	return nil
}

func isEncodingJSONSafe(col collations.ID) bool {
	switch col.Get().Charset().(type) {
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
	c.asm.Convert_xc(1, sqltypes.VarChar, c.cfg.Collation, 0, false)
	return nil
}

func (c *compiler) jsonExtractPath(expr Expr) (*json.Path, error) {
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

func (c *compiler) jsonExtractOneOrAll(fname string, expr Expr) (jsonMatch, error) {
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
