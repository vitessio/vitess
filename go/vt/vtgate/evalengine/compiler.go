package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/json"
)

type frame func(vm *VirtualMachine) int

type compiler struct {
	ins              []frame
	log              CompilerLog
	fields           []*querypb.Field
	defaultCollation collations.ID

	stack struct {
		cur int
		max int
	}
}

type CompilerLog interface {
	Disasm(ins string, args ...any)
	Stack(old, new int)
}

type compiledCoercion struct {
	col   collations.Collation
	left  collations.Coercion
	right collations.Coercion
}

type CompilerOption func(c *compiler)

func WithCompilerLog(log CompilerLog) CompilerOption {
	return func(c *compiler) {
		c.log = log
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
	if comp.stack.cur != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "bad compilation: stack pointer at %d after compilation", comp.stack.cur)
	}
	return &Program{code: comp.ins, original: expr, stack: comp.stack.max}, nil
}

func (c *compiler) adjustStack(offset int) {
	c.stack.cur += offset
	if c.stack.cur < 0 {
		panic("negative stack position")
	}
	if c.stack.cur > c.stack.max {
		c.stack.max = c.stack.cur
	}
	if c.log != nil {
		c.log.Stack(c.stack.cur-offset, c.stack.cur)
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
		c.emitPushColumn_i(offset)
	case sqltypes.IsUnsigned(tt):
		c.emitPushColumn_u(offset)
	case sqltypes.IsFloat(tt):
		c.emitPushColumn_f(offset)
	case sqltypes.IsDecimal(tt):
		c.emitPushColumn_d(offset)
	case sqltypes.IsText(tt):
		if tt == sqltypes.HexNum {
			c.emitPushColumn_hexnum(offset)
		} else if tt == sqltypes.HexVal {
			c.emitPushColumn_hexval(offset)
		} else {
			c.emitPushColumn_text(offset, col)
		}
	case sqltypes.IsBinary(tt):
		c.emitPushColumn_bin(offset)
	case sqltypes.IsNull(tt):
		c.emitPushNull()
	case tt == sqltypes.TypeJSON:
		c.emitPushColumn_json(offset)
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

type jump struct {
	from, to int
}

func (j *jump) offset() int {
	return j.to - j.from
}

func (c *compiler) jumpFrom() *jump {
	return &jump{from: len(c.ins)}
}

func (c *compiler) jumpDestination(j *jump) {
	j.to = len(c.ins)
}

func (c *compiler) unsupported(expr Expr) error {
	return vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "unsupported compilation for expression '%s'", FormatExpr(expr))
}

func (c *compiler) compileExpr(expr Expr) (ctype, error) {
	switch expr := expr.(type) {
	case *Literal:
		if expr.inner == nil {
			c.emitPushNull()
		} else if err := c.emitPushLiteral(expr.inner); err != nil {
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

	case *builtinBitCount:
		return c.compileBitCount(expr)

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

	case callable:
		return c.compileCallable(expr)

	default:
		return ctype{}, c.unsupported(expr)
	}
}

func (c *compiler) compileCallable(call callable) (ctype, error) {
	switch call := call.(type) {
	case *builtinMultiComparison:
		return c.compileMultiComparison(call)
	case *builtinJSONExtract:
		return c.compileJSONExtract(call)
	case *builtinJSONUnquote:
		return c.compileJSONUnquote(call)
	case *builtinJSONContainsPath:
		return c.compileJSONContainsPath(call)
	case *builtinJSONArray:
		return c.compileJSONArray(call)
	case *builtinJSONObject:
		return c.compileJSONObject(call)
	case *builtinRepeat:
		return c.compileRepeat(call)
	case *builtinToBase64:
		return c.compileToBase64(call)
	case *builtinFromBase64:
		return c.compileFromBase64(call)
	case *builtinChangeCase:
		return c.compileChangeCase(call)
	case *builtinCharLength:
		return c.compileLength(call, charLen)
	case *builtinLength:
		return c.compileLength(call, byteLen)
	case *builtinBitLength:
		return c.compileLength(call, bitLen)
	case *builtinASCII:
		return c.compileASCII(call)
	case *builtinHex:
		return c.compileHex(call)
	default:
		return ctype{}, c.unsupported(call)
	}
}

func (c *compiler) compileComparison(expr *ComparisonExpr) (ctype, error) {
	lt, err := c.compileExpr(expr.Left)
	if err != nil {
		return ctype{}, err
	}

	rt, err := c.compileExpr(expr.Right)
	if err != nil {
		return ctype{}, err
	}

	swapped := false
	skip := c.jumpFrom()

	switch expr.Op.(type) {
	case compareNullSafeEQ:
		c.emitNullCmp(skip)
	default:
		c.emitNullCheck2(skip)
	}

	switch {
	case compareAsStrings(lt.Type, rt.Type):
		var merged collations.TypedCollation
		var coerceLeft collations.Coercion
		var coerceRight collations.Coercion
		var env = collations.Local()

		if lt.Col.Collation != rt.Col.Collation {
			merged, coerceLeft, coerceRight, err = env.MergeCollations(lt.Col, rt.Col, collations.CoercionOptions{
				ConvertToSuperset:   true,
				ConvertWithCoercion: true,
			})
		} else {
			merged = lt.Col
		}
		if err != nil {
			return ctype{}, err
		}

		if coerceLeft == nil && coerceRight == nil {
			c.emitCmpString_collate(merged.Collation.Get())
		} else {
			if coerceLeft == nil {
				coerceLeft = func(dst, in []byte) ([]byte, error) { return in, nil }
			}
			if coerceRight == nil {
				coerceRight = func(dst, in []byte) ([]byte, error) { return in, nil }
			}
			c.emitCmpString_coerce(&compiledCoercion{
				col:   merged.Collation.Get(),
				left:  coerceLeft,
				right: coerceRight,
			})
		}

	case compareAsSameNumericType(lt.Type, rt.Type) || compareAsDecimal(lt.Type, rt.Type):
		switch lt.Type {
		case sqltypes.Int64:
			switch rt.Type {
			case sqltypes.Int64:
				c.emitCmpNum_ii()
			case sqltypes.Uint64:
				c.emitCmpNum_iu(2, 1)
			case sqltypes.Float64:
				c.emitCmpNum_if(2, 1)
			case sqltypes.Decimal:
				c.emitCmpNum_id(2, 1)
			}
		case sqltypes.Uint64:
			switch rt.Type {
			case sqltypes.Int64:
				c.emitCmpNum_iu(1, 2)
				swapped = true
			case sqltypes.Uint64:
				c.emitCmpNum_uu()
			case sqltypes.Float64:
				c.emitCmpNum_uf(2, 1)
			case sqltypes.Decimal:
				c.emitCmpNum_ud(2, 1)
			}
		case sqltypes.Float64:
			switch rt.Type {
			case sqltypes.Int64:
				c.emitCmpNum_if(1, 2)
				swapped = true
			case sqltypes.Uint64:
				c.emitCmpNum_uf(1, 2)
				swapped = true
			case sqltypes.Float64:
				c.emitCmpNum_ff()
			case sqltypes.Decimal:
				c.emitCmpNum_fd(2, 1)
			}

		case sqltypes.Decimal:
			switch rt.Type {
			case sqltypes.Int64:
				c.emitCmpNum_id(1, 2)
				swapped = true
			case sqltypes.Uint64:
				c.emitCmpNum_ud(1, 2)
				swapped = true
			case sqltypes.Float64:
				c.emitCmpNum_fd(1, 2)
				swapped = true
			case sqltypes.Decimal:
				c.emitCmpNum_dd()
			}
		}

	case compareAsDates(lt.Type, rt.Type) || compareAsDateAndString(lt.Type, rt.Type) || compareAsDateAndNumeric(lt.Type, rt.Type):
		return ctype{}, c.unsupported(expr)

	default:
		lt = c.compileToFloat(lt, 2)
		rt = c.compileToFloat(rt, 1)
		c.emitCmpNum_ff()
	}

	cmptype := ctype{Type: sqltypes.Int64, Col: collationNumeric}

	switch expr.Op.(type) {
	case compareEQ:
		c.emitCmp_eq()
	case compareNE:
		c.emitCmp_ne()
	case compareLT:
		if swapped {
			c.emitCmp_gt()
		} else {
			c.emitCmp_lt()
		}
	case compareLE:
		if swapped {
			c.emitCmp_ge()
		} else {
			c.emitCmp_le()
		}
	case compareGT:
		if swapped {
			c.emitCmp_lt()
		} else {
			c.emitCmp_gt()
		}
	case compareGE:
		if swapped {
			c.emitCmp_le()
		} else {
			c.emitCmp_ge()
		}
	case compareNullSafeEQ:
		c.jumpDestination(skip)
		c.emitCmp_eq()
		return cmptype, nil

	default:
		panic("unexpected comparison operator")
	}

	c.jumpDestination(skip)
	return cmptype, nil
}

func (c *compiler) compileArithmetic(expr *ArithmeticExpr) (ctype, error) {
	switch expr.Op.(type) {
	case *opArithAdd:
		return c.compileArithmeticAdd(expr.Left, expr.Right)
	case *opArithSub:
		return c.compileArithmeticSub(expr.Left, expr.Right)
	case *opArithMul:
		return c.compileArithmeticMul(expr.Left, expr.Right)
	case *opArithDiv:
		return c.compileArithmeticDiv(expr.Left, expr.Right)
	default:
		panic("unexpected arithmetic operator")
	}
}

func (c *compiler) compileToNumeric(ct ctype, offset int) ctype {
	if sqltypes.IsNumber(ct.Type) {
		return ct
	}
	if ct.Type == sqltypes.VarBinary && (ct.Flag&flagHex) != 0 {
		c.emitConvert_hex(offset)
		return ctype{sqltypes.Uint64, ct.Flag, collationNumeric}
	}
	c.emitConvert_xf(offset)
	return ctype{sqltypes.Float64, ct.Flag, collationNumeric}
}

func (c *compiler) compileToInt64(ct ctype, offset int) ctype {
	switch ct.Type {
	case sqltypes.Int64:
		return ct
	case sqltypes.Uint64:
		c.emitConvert_ui(offset)
	// TODO: specialization
	default:
		c.emitConvert_xi(offset)
	}
	return ctype{sqltypes.Int64, ct.Flag, collationNumeric}
}

func (c *compiler) compileToUint64(ct ctype, offset int) ctype {
	switch ct.Type {
	case sqltypes.Uint64:
		return ct
	case sqltypes.Int64:
		c.emitConvert_iu(offset)
	// TODO: specialization
	default:
		c.emitConvert_xu(offset)
	}
	return ctype{sqltypes.Uint64, ct.Flag, collationNumeric}
}

func (c *compiler) compileToBitwiseUint64(ct ctype, offset int) ctype {
	switch ct.Type {
	case sqltypes.Uint64:
		return ct
	case sqltypes.Int64:
		c.emitConvert_iu(offset)
	case sqltypes.Decimal:
		c.emitConvert_dbit(offset)
	// TODO: specialization
	default:
		c.emitConvert_xu(offset)
	}
	return ctype{sqltypes.Uint64, ct.Flag, collationNumeric}
}

func (c *compiler) compileToFloat(ct ctype, offset int) ctype {
	if sqltypes.IsFloat(ct.Type) {
		return ct
	}
	switch ct.Type {
	case sqltypes.Int64:
		c.emitConvert_if(offset)
	case sqltypes.Uint64:
		// only emit u->f conversion if this is not a hex value; hex values
		// will already be converted
		c.emitConvert_uf(offset)
	default:
		c.emitConvert_xf(offset)
	}
	return ctype{sqltypes.Float64, ct.Flag, collationNumeric}
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

func (c *compiler) compileArithmeticAdd(left, right Expr) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	swap := false
	skip := c.jumpFrom()
	c.emitNullCheck2(skip)
	lt = c.compileToNumeric(lt, 2)
	rt = c.compileToNumeric(rt, 1)
	lt, rt, swap = c.compileNumericPriority(lt, rt)

	var sumtype sqltypes.Type

	switch lt.Type {
	case sqltypes.Int64:
		c.emitAdd_ii()
		sumtype = sqltypes.Int64
	case sqltypes.Uint64:
		switch rt.Type {
		case sqltypes.Int64:
			c.emitAdd_ui(swap)
		case sqltypes.Uint64:
			c.emitAdd_uu()
		}
		sumtype = sqltypes.Uint64
	case sqltypes.Decimal:
		if swap {
			c.compileToDecimal(rt, 2)
		} else {
			c.compileToDecimal(rt, 1)
		}
		c.emitAdd_dd()
		sumtype = sqltypes.Decimal
	case sqltypes.Float64:
		if swap {
			c.compileToFloat(rt, 2)
		} else {
			c.compileToFloat(rt, 1)
		}
		c.emitAdd_ff()
		sumtype = sqltypes.Float64
	}

	c.jumpDestination(skip)
	return ctype{Type: sumtype, Col: collationNumeric}, nil
}

func (c *compiler) compileArithmeticSub(left, right Expr) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck2(skip)
	lt = c.compileToNumeric(lt, 2)
	rt = c.compileToNumeric(rt, 1)

	var subtype sqltypes.Type

	switch lt.Type {
	case sqltypes.Int64:
		switch rt.Type {
		case sqltypes.Int64:
			c.emitSub_ii()
			subtype = sqltypes.Int64
		case sqltypes.Uint64:
			c.emitSub_iu()
			subtype = sqltypes.Uint64
		case sqltypes.Float64:
			c.compileToFloat(lt, 2)
			c.emitSub_ff()
			subtype = sqltypes.Float64
		case sqltypes.Decimal:
			c.compileToDecimal(lt, 2)
			c.emitSub_dd()
			subtype = sqltypes.Decimal
		}
	case sqltypes.Uint64:
		switch rt.Type {
		case sqltypes.Int64:
			c.emitSub_ui()
			subtype = sqltypes.Uint64
		case sqltypes.Uint64:
			c.emitSub_uu()
			subtype = sqltypes.Uint64
		case sqltypes.Float64:
			c.compileToFloat(lt, 2)
			c.emitSub_ff()
			subtype = sqltypes.Float64
		case sqltypes.Decimal:
			c.compileToDecimal(lt, 2)
			c.emitSub_dd()
			subtype = sqltypes.Decimal
		}
	case sqltypes.Float64:
		c.compileToFloat(rt, 1)
		c.emitSub_ff()
		subtype = sqltypes.Float64
	case sqltypes.Decimal:
		switch rt.Type {
		case sqltypes.Float64:
			c.compileToFloat(lt, 2)
			c.emitSub_ff()
			subtype = sqltypes.Float64
		default:
			c.compileToDecimal(rt, 1)
			c.emitSub_dd()
			subtype = sqltypes.Decimal
		}
	}

	if subtype == 0 {
		panic("did not compile?")
	}

	c.jumpDestination(skip)
	return ctype{Type: subtype, Col: collationNumeric}, nil
}

func (c *compiler) compileToDecimal(ct ctype, offset int) ctype {
	if sqltypes.IsDecimal(ct.Type) {
		return ct
	}
	switch ct.Type {
	case sqltypes.Int64:
		c.emitConvert_id(offset)
	case sqltypes.Uint64:
		c.emitConvert_ud(offset)
	default:
		c.emitConvert_xd(offset, 0, 0)
	}
	return ctype{sqltypes.Decimal, ct.Flag, collationNumeric}
}

func (c *compiler) compileArithmeticMul(left, right Expr) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	swap := false
	skip := c.jumpFrom()
	c.emitNullCheck2(skip)
	lt = c.compileToNumeric(lt, 2)
	rt = c.compileToNumeric(rt, 1)
	lt, rt, swap = c.compileNumericPriority(lt, rt)

	var multype sqltypes.Type

	switch lt.Type {
	case sqltypes.Int64:
		c.emitMul_ii()
		multype = sqltypes.Int64
	case sqltypes.Uint64:
		switch rt.Type {
		case sqltypes.Int64:
			c.emitMul_ui(swap)
		case sqltypes.Uint64:
			c.emitMul_uu()
		}
		multype = sqltypes.Uint64
	case sqltypes.Float64:
		if swap {
			c.compileToFloat(rt, 2)
		} else {
			c.compileToFloat(rt, 1)
		}
		c.emitMul_ff()
		multype = sqltypes.Float64
	case sqltypes.Decimal:
		if swap {
			c.compileToDecimal(rt, 2)
		} else {
			c.compileToDecimal(rt, 1)
		}
		c.emitMul_dd()
		multype = sqltypes.Decimal
	}

	c.jumpDestination(skip)
	return ctype{Type: multype, Col: collationNumeric}, nil
}

func (c *compiler) compileArithmeticDiv(left, right Expr) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck2(skip)
	lt = c.compileToNumeric(lt, 2)
	rt = c.compileToNumeric(rt, 1)

	if lt.Type == sqltypes.Float64 || rt.Type == sqltypes.Float64 {
		c.compileToFloat(lt, 2)
		c.compileToFloat(rt, 1)
		c.emitDiv_ff()
		return ctype{Type: sqltypes.Float64, Col: collationNumeric}, nil
	} else {
		c.compileToDecimal(lt, 2)
		c.compileToDecimal(rt, 1)
		c.emitDiv_dd()
		return ctype{Type: sqltypes.Decimal, Col: collationNumeric}, nil
	}
}

func (c *compiler) compileCollate(expr *CollateExpr) (ctype, error) {
	ct, err := c.compileExpr(expr.Inner)
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck1(skip)

	switch ct.Type {
	case sqltypes.VarChar:
		if err := collations.Local().EnsureCollate(ct.Col.Collation, expr.TypedCollation.Collation); err != nil {
			return ctype{}, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, err.Error())
		}
		fallthrough
	case sqltypes.VarBinary:
		c.emitCollate(expr.TypedCollation.Collation)
	default:
		return ctype{}, c.unsupported(expr)
	}

	c.jumpDestination(skip)

	ct.Col = expr.TypedCollation
	ct.Flag |= flagExplicitCollation
	return ct, nil
}

func (c *compiler) extractJSONPath(expr Expr) (*json.Path, error) {
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

func (c *compiler) extractOneOrAll(fname string, expr Expr) (jsonMatch, error) {
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

func (c *compiler) compileJSONExtract(call *builtinJSONExtract) (ctype, error) {
	doct, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	if slices2.All(call.Arguments[1:], func(expr Expr) bool { return expr.constant() }) {
		paths := make([]*json.Path, 0, len(call.Arguments[1:]))

		for _, arg := range call.Arguments[1:] {
			jp, err := c.extractJSONPath(arg)
			if err != nil {
				return ctype{}, err
			}
			paths = append(paths, jp)
		}

		jt, err := c.compileParseJSON("JSON_EXTRACT", doct, 1)
		if err != nil {
			return ctype{}, err
		}

		c.emitFn_JSON_EXTRACT0(paths)
		return jt, nil
	}

	return ctype{}, c.unsupported(call)
}

func (c *compiler) compileJSONUnquote(call *builtinJSONUnquote) (ctype, error) {
	arg, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck1(skip)

	_, err = c.compileParseJSON("JSON_UNQUOTE", arg, 1)
	if err != nil {
		return ctype{}, err
	}

	c.emitFn_JSON_UNQUOTE()
	c.jumpDestination(skip)
	return ctype{Type: sqltypes.Blob, Col: collationJSON}, nil
}

func (c *compiler) compileJSONContainsPath(call *builtinJSONContainsPath) (ctype, error) {
	doct, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	if !call.Arguments[1].constant() {
		return ctype{}, c.unsupported(call)
	}

	if !slices2.All(call.Arguments[2:], func(expr Expr) bool { return expr.constant() }) {
		return ctype{}, c.unsupported(call)
	}

	match, err := c.extractOneOrAll("JSON_CONTAINS_PATH", call.Arguments[1])
	if err != nil {
		return ctype{}, err
	}

	paths := make([]*json.Path, 0, len(call.Arguments[2:]))

	for _, arg := range call.Arguments[2:] {
		jp, err := c.extractJSONPath(arg)
		if err != nil {
			return ctype{}, err
		}
		paths = append(paths, jp)
	}

	_, err = c.compileParseJSON("JSON_CONTAINS_PATH", doct, 1)
	if err != nil {
		return ctype{}, err
	}

	c.emitFn_JSON_CONTAINS_PATH(match, paths)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
}

func (c *compiler) compileParseJSON(fn string, doct ctype, offset int) (ctype, error) {
	switch doct.Type {
	case sqltypes.TypeJSON:
	case sqltypes.VarChar, sqltypes.VarBinary:
		c.emitParse_j(offset)
	default:
		return ctype{}, errJSONType(fn)
	}
	return ctype{Type: sqltypes.TypeJSON, Col: collationJSON}, nil
}

func (c *compiler) compileToJSON(doct ctype, offset int) (ctype, error) {
	switch doct.Type {
	case sqltypes.TypeJSON:
		return doct, nil
	case sqltypes.Float64:
		c.emitConvert_fj(offset)
	case sqltypes.Int64, sqltypes.Uint64, sqltypes.Decimal:
		c.emitConvert_nj(offset)
	case sqltypes.VarChar:
		c.emitConvert_cj(offset)
	case sqltypes.VarBinary:
		c.emitConvert_bj(offset)
	case sqltypes.Null:
		c.emitConvert_Nj(offset)
	default:
		return ctype{}, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "Unsupported type conversion: %s AS JSON", doct.Type)
	}
	return ctype{Type: sqltypes.TypeJSON, Col: collationJSON}, nil
}

func (c *compiler) compileBitwise(expr *BitwiseExpr) (ctype, error) {
	switch expr.Op.(type) {
	case *opBitAnd:
		return c.compileBitwiseOp(expr.Left, expr.Right, and)
	case *opBitOr:
		return c.compileBitwiseOp(expr.Left, expr.Right, or)
	case *opBitXor:
		return c.compileBitwiseOp(expr.Left, expr.Right, xor)
	case *opBitShl:
		return c.compileBitwiseShift(expr.Left, expr.Right, -1)
	case *opBitShr:
		return c.compileBitwiseShift(expr.Left, expr.Right, 1)
	default:
		panic("unexpected arithmetic operator")
	}
}

type bitwiseOp int

const (
	and bitwiseOp = iota
	or
	xor
)

func (c *compiler) compileBitwiseOp(left Expr, right Expr, op bitwiseOp) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck2(skip)

	if lt.Type == sqltypes.VarBinary && rt.Type == sqltypes.VarBinary {
		if !lt.isHexOrBitLiteral() || !rt.isHexOrBitLiteral() {
			c.emitBitOp_bb(op)
			c.jumpDestination(skip)
			return ctype{Type: sqltypes.VarBinary, Col: collationBinary}, nil
		}
	}

	lt = c.compileToBitwiseUint64(lt, 2)
	rt = c.compileToBitwiseUint64(rt, 1)

	c.emitBitOp_uu(op)
	c.jumpDestination(skip)
	return ctype{Type: sqltypes.Uint64, Col: collationNumeric}, nil
}

func (c *compiler) compileBitwiseShift(left Expr, right Expr, i int) (ctype, error) {
	lt, err := c.compileExpr(left)
	if err != nil {
		return ctype{}, err
	}

	rt, err := c.compileExpr(right)
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck2(skip)

	if lt.Type == sqltypes.VarBinary && !lt.isHexOrBitLiteral() {
		_ = c.compileToUint64(rt, 1)
		if i < 0 {
			c.emitBitShiftLeft_bu()
		} else {
			c.emitBitShiftRight_bu()
		}
		c.jumpDestination(skip)
		return ctype{Type: sqltypes.VarBinary, Col: collationBinary}, nil
	}

	_ = c.compileToBitwiseUint64(lt, 2)
	_ = c.compileToUint64(rt, 1)

	if i < 0 {
		c.emitBitShiftLeft_uu()
	} else {
		c.emitBitShiftRight_uu()
	}

	c.jumpDestination(skip)
	return ctype{Type: sqltypes.Uint64, Col: collationNumeric}, nil
}

func (c *compiler) compileBitCount(expr *builtinBitCount) (ctype, error) {
	ct, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck1(skip)

	if ct.Type == sqltypes.VarBinary && !ct.isHexOrBitLiteral() {
		c.emitBitCount_b()
		c.jumpDestination(skip)
		return ctype{Type: sqltypes.Int64, Col: collationBinary}, nil
	}

	_ = c.compileToBitwiseUint64(ct, 1)
	c.emitBitCount_u()
	c.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationBinary}, nil
}

func (c *compiler) compileBitwiseNot(expr *BitwiseNotExpr) (ctype, error) {
	ct, err := c.compileExpr(expr.Inner)
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck1(skip)

	if ct.Type == sqltypes.VarBinary && !ct.isHexOrBitLiteral() {
		c.emitBitwiseNot_b()
		c.jumpDestination(skip)
		return ct, nil
	}

	ct = c.compileToBitwiseUint64(ct, 1)
	c.emitBitwiseNot_u()
	c.jumpDestination(skip)
	return ct, nil
}

func (c *compiler) compileConvert(conv *ConvertExpr) (ctype, error) {
	arg, err := c.compileExpr(conv.Inner)
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck1(skip)

	var convt ctype

	switch conv.Type {
	case "BINARY":
		convt = ctype{Type: conv.convertToBinaryType(arg.Type), Col: collationBinary}
		c.emitConvert_xb(1, convt.Type, conv.Length, conv.HasLength)

	case "CHAR", "NCHAR":
		convt = ctype{
			Type: conv.convertToCharType(arg.Type),
			Col:  collations.TypedCollation{Collation: conv.Collation},
		}
		c.emitConvert_xc(1, convt.Type, convt.Col.Collation, conv.Length, conv.HasLength)

	case "DECIMAL":
		convt = ctype{Type: sqltypes.Decimal, Col: collationNumeric}
		m, d := conv.decimalPrecision()
		c.emitConvert_xd(1, m, d)

	case "DOUBLE", "REAL":
		convt = c.compileToFloat(arg, 1)

	case "SIGNED", "SIGNED INTEGER":
		convt = c.compileToInt64(arg, 1)

	case "UNSIGNED", "UNSIGNED INTEGER":
		convt = c.compileToUint64(arg, 1)

	case "JSON":
		// TODO: what does NULL map to?
		convt, err = c.compileToJSON(arg, 1)
		if err != nil {
			return ctype{}, err
		}

	default:
		return ctype{}, c.unsupported(conv)
	}

	c.jumpDestination(skip)
	return convt, nil

}

func (c *compiler) compileConvertUsing(conv *ConvertUsingExpr) (ctype, error) {
	_, err := c.compileExpr(conv.Inner)
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck1(skip)
	c.emitConvert_xc(1, sqltypes.VarChar, conv.Collation, 0, false)
	c.jumpDestination(skip)

	col := collations.TypedCollation{
		Collation:    conv.Collation,
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireASCII,
	}
	return ctype{Type: sqltypes.VarChar, Col: col}, nil
}

func (c *compiler) compileCase(cs *CaseExpr) (ctype, error) {
	var ca collationAggregation
	var ta typeAggregation
	var local = collations.Local()

	for _, wt := range cs.cases {
		when, err := c.compileExpr(wt.when)
		if err != nil {
			return ctype{}, err
		}

		if err := c.compileCheckTrue(when, 1); err != nil {
			return ctype{}, err
		}

		then, err := c.compileExpr(wt.then)
		if err != nil {
			return ctype{}, err
		}

		ta.add(then.Type, then.Flag)
		if err := ca.add(local, then.Col); err != nil {
			return ctype{}, err
		}
	}

	if cs.Else != nil {
		els, err := c.compileExpr(cs.Else)
		if err != nil {
			return ctype{}, err
		}

		ta.add(els.Type, els.Flag)
		if err := ca.add(local, els.Col); err != nil {
			return ctype{}, err
		}
	}

	ct := ctype{Type: ta.result(), Col: ca.result()}
	c.emitCmpCase(len(cs.cases), cs.Else != nil, ct.Type, ct.Col)
	return ct, nil
}

func (c *compiler) compileCheckTrue(when ctype, offset int) error {
	switch when.Type {
	case sqltypes.Int64:
		c.emitConvert_iB(offset)
	case sqltypes.Uint64:
		c.emitConvert_uB(offset)
	case sqltypes.Float64:
		c.emitConvert_fB(offset)
	case sqltypes.Decimal:
		c.emitConvert_dB(offset)
	case sqltypes.VarChar, sqltypes.VarBinary:
		c.emitConvert_bB(offset)
	case sqltypes.Null:
		c.emitSetBool(offset, false)
	default:
		return vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "unsupported Truth check: %s", when.Type)
	}
	return nil
}

func (c *compiler) compileNegate(expr *NegateExpr) (ctype, error) {
	arg, err := c.compileExpr(expr.Inner)
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck1(skip)

	arg = c.compileToNumeric(arg, 1)
	var neg sqltypes.Type

	switch arg.Type {
	case sqltypes.Int64:
		neg = sqltypes.Int64
		c.emitNeg_i()
	case sqltypes.Uint64:
		if arg.Flag&flagHex != 0 {
			neg = sqltypes.Float64
			c.emitNeg_hex()
		} else {
			neg = sqltypes.Int64
			c.emitNeg_u()
		}
	case sqltypes.Float64:
		neg = sqltypes.Float64
		c.emitNeg_f()
	case sqltypes.Decimal:
		neg = sqltypes.Decimal
		c.emitNeg_d()
	default:
		panic("unexpected Numeric type")
	}

	c.jumpDestination(skip)
	return ctype{Type: neg, Col: collationNumeric}, nil
}

func (c *compiler) compileMultiComparison(call *builtinMultiComparison) (ctype, error) {
	var (
		integersI int
		integersU int
		floats    int
		decimals  int
		text      int
		binary    int
		args      []ctype
	)

	/*
		If any argument is NULL, the result is NULL. No comparison is needed.
		If all arguments are integer-valued, they are compared as integers.
		If at least one argument is double precision, they are compared as double-precision values. Otherwise, if at least one argument is a DECIMAL value, they are compared as DECIMAL values.
		If the arguments comprise a mix of numbers and strings, they are compared as strings.
		If any argument is a nonbinary (character) string, the arguments are compared as nonbinary strings.
		In all other cases, the arguments are compared as binary strings.
	*/

	for _, expr := range call.Arguments {
		tt, err := c.compileExpr(expr)
		if err != nil {
			return ctype{}, err
		}

		args = append(args, tt)

		switch tt.Type {
		case sqltypes.Int64:
			integersI++
		case sqltypes.Uint64:
			integersU++
		case sqltypes.Float64:
			floats++
		case sqltypes.Decimal:
			decimals++
		case sqltypes.Text, sqltypes.VarChar:
			text++
		case sqltypes.Blob, sqltypes.Binary, sqltypes.VarBinary:
			binary++
		default:
			return ctype{}, c.unsupported(call)
		}
	}

	if integersI+integersU == len(args) {
		if integersI == len(args) {
			c.emitFn_MULTICMP_i(len(args), call.cmp < 0)
			return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
		}
		if integersU == len(args) {
			c.emitFn_MULTICMP_u(len(args), call.cmp < 0)
			return ctype{Type: sqltypes.Uint64, Col: collationNumeric}, nil
		}
		return c.compileMultiComparison_d(args, call.cmp < 0)
	}
	if binary > 0 || text > 0 {
		if text > 0 {
			return c.compileMultiComparison_c(args, call.cmp < 0)
		}
		c.emitFn_MULTICMP_b(len(args), call.cmp < 0)
		return ctype{Type: sqltypes.VarBinary, Col: collationBinary}, nil
	} else {
		if floats > 0 {
			for i, tt := range args {
				c.compileToFloat(tt, len(args)-i)
			}
			c.emitFn_MULTICMP_f(len(args), call.cmp < 0)
			return ctype{Type: sqltypes.Float64, Col: collationNumeric}, nil
		}
		if decimals > 0 {
			return c.compileMultiComparison_d(args, call.cmp < 0)
		}
	}
	return ctype{}, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected argument for GREATEST/LEAST")
}

func (c *compiler) compileMultiComparison_c(args []ctype, lessThan bool) (ctype, error) {
	env := collations.Local()

	var ca collationAggregation
	for _, arg := range args {
		if err := ca.add(env, arg.Col); err != nil {
			return ctype{}, err
		}
	}

	tc := ca.result()
	c.emitFn_MULTICMP_c(len(args), lessThan, tc)
	return ctype{Type: sqltypes.VarChar, Col: tc}, nil
}

func (c *compiler) compileMultiComparison_d(args []ctype, lessThan bool) (ctype, error) {
	for i, tt := range args {
		c.compileToDecimal(tt, len(args)-i)
	}
	c.emitFn_MULTICMP_d(len(args), lessThan)
	return ctype{Type: sqltypes.Decimal, Col: collationNumeric}, nil
}

func (c *compiler) compileRepeat(expr *builtinRepeat) (ctype, error) {
	str, err := c.compileExpr(expr.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	repeat, err := c.compileExpr(expr.Arguments[1])
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck2(skip)

	switch {
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
	default:
		c.emitConvert_xc(2, sqltypes.VarChar, c.defaultCollation, 0, false)
	}
	_ = c.compileToInt64(repeat, 1)

	c.emitFn_REPEAT(1)
	c.jumpDestination(skip)
	return ctype{Type: sqltypes.VarChar, Col: str.Col}, nil
}

func (c *compiler) compileToBase64(call *builtinToBase64) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck1(skip)

	t := sqltypes.VarChar
	if str.Type == sqltypes.Blob || str.Type == sqltypes.TypeJSON {
		t = sqltypes.Text
	}

	switch {
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
	default:
		c.emitConvert_xc(1, t, c.defaultCollation, 0, false)
	}

	col := collations.TypedCollation{
		Collation:    c.defaultCollation,
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireASCII,
	}

	c.emitFn_TO_BASE64(t, col)
	c.jumpDestination(skip)

	return ctype{Type: t, Col: col}, nil
}

func (c *compiler) compileFromBase64(call *builtinFromBase64) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck1(skip)

	switch {
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
	default:
		c.emitConvert_xc(1, sqltypes.VarBinary, c.defaultCollation, 0, false)
	}

	c.emitFromBase64()
	c.jumpDestination(skip)

	return ctype{Type: sqltypes.VarBinary, Col: collationBinary}, nil
}

func (c *compiler) compileChangeCase(call *builtinChangeCase) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck1(skip)

	switch {
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
	default:
		c.emitConvert_xc(1, sqltypes.VarChar, c.defaultCollation, 0, false)
	}

	c.emitFn_LUCASE(call.upcase)
	c.jumpDestination(skip)

	return ctype{Type: sqltypes.VarChar, Col: str.Col}, nil
}

type lengthOp int

const (
	charLen lengthOp = iota
	byteLen
	bitLen
)

func (c *compiler) compileLength(call callable, op lengthOp) (ctype, error) {
	str, err := c.compileExpr(call.callable()[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck1(skip)

	switch {
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
	default:
		c.emitConvert_xc(1, sqltypes.VarChar, c.defaultCollation, 0, false)
	}

	c.emitFn_LENGTH(op)
	c.jumpDestination(skip)

	return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
}

func (c *compiler) compileASCII(call *builtinASCII) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck1(skip)

	switch {
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
	default:
		c.emitConvert_xc(1, sqltypes.VarChar, c.defaultCollation, 0, false)
	}

	c.emitFn_ASCII()
	c.jumpDestination(skip)

	return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
}

func (c *compiler) compileHex(call *builtinHex) (ctype, error) {
	str, err := c.compileExpr(call.Arguments[0])
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck1(skip)

	col := collations.TypedCollation{
		Collation:    c.defaultCollation,
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireASCII,
	}

	t := sqltypes.VarChar
	if str.Type == sqltypes.Blob || str.Type == sqltypes.TypeJSON {
		t = sqltypes.Text
	}

	switch {
	case sqltypes.IsNumber(str.Type), sqltypes.IsDecimal(str.Type):
		c.emitFn_HEXd(col)
	case sqltypes.IsText(str.Type) || sqltypes.IsBinary(str.Type):
		c.emitFn_HEXc(t, col)
	default:
		c.emitConvert_xc(1, t, c.defaultCollation, 0, false)
		c.emitFn_HEXc(t, col)
	}

	c.jumpDestination(skip)

	return ctype{Type: t, Col: col}, nil
}

func (c *compiler) compileLike(expr *LikeExpr) (ctype, error) {
	lt, err := c.compileExpr(expr.Left)
	if err != nil {
		return ctype{}, err
	}

	rt, err := c.compileExpr(expr.Right)
	if err != nil {
		return ctype{}, err
	}

	skip := c.jumpFrom()
	c.emitNullCheck2(skip)

	if !sqltypes.IsText(lt.Type) && !sqltypes.IsBinary(lt.Type) {
		c.emitConvert_xc(2, sqltypes.VarChar, c.defaultCollation, 0, false)
		lt.Col = collations.TypedCollation{
			Collation:    c.defaultCollation,
			Coercibility: collations.CoerceCoercible,
			Repertoire:   collations.RepertoireASCII,
		}
	}

	if !sqltypes.IsText(rt.Type) && !sqltypes.IsBinary(rt.Type) {
		c.emitConvert_xc(1, sqltypes.VarChar, c.defaultCollation, 0, false)
		rt.Col = collations.TypedCollation{
			Collation:    c.defaultCollation,
			Coercibility: collations.CoerceCoercible,
			Repertoire:   collations.RepertoireASCII,
		}
	}

	var merged collations.TypedCollation
	var coerceLeft collations.Coercion
	var coerceRight collations.Coercion
	var env = collations.Local()

	if lt.Col.Collation != rt.Col.Collation {
		merged, coerceLeft, coerceRight, err = env.MergeCollations(lt.Col, rt.Col, collations.CoercionOptions{
			ConvertToSuperset:   true,
			ConvertWithCoercion: true,
		})
	} else {
		merged = lt.Col
	}
	if err != nil {
		return ctype{}, err
	}

	if coerceLeft == nil && coerceRight == nil {
		c.emitLike_collate(expr, merged.Collation.Get())
	} else {
		if coerceLeft == nil {
			coerceLeft = func(dst, in []byte) ([]byte, error) { return in, nil }
		}
		if coerceRight == nil {
			coerceRight = func(dst, in []byte) ([]byte, error) { return in, nil }
		}
		c.emitLike_coerce(expr, &compiledCoercion{
			col:   merged.Collation.Get(),
			left:  coerceLeft,
			right: coerceRight,
		})
	}

	c.jumpDestination(skip)

	return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
}

func (c *compiler) compileJSONArray(call *builtinJSONArray) (ctype, error) {
	for _, arg := range call.Arguments {
		tt, err := c.compileExpr(arg)
		if err != nil {
			return ctype{}, err
		}

		_, err = c.compileToJSON(tt, 1)
		if err != nil {
			return ctype{}, err
		}
	}
	c.emitFn_JSON_ARRAY(len(call.Arguments))
	return ctype{Type: sqltypes.TypeJSON, Col: collationJSON}, nil
}

func (c *compiler) compileJSONObject(call *builtinJSONObject) (ctype, error) {
	for i := 0; i < len(call.Arguments); i += 2 {
		key, err := c.compileExpr(call.Arguments[i])
		if err != nil {
			return ctype{}, err
		}
		c.compileToJSONKey(key)
		val, err := c.compileExpr(call.Arguments[i+1])
		if err != nil {
			return ctype{}, err
		}
		val, err = c.compileToJSON(val, 1)
		if err != nil {
			return ctype{}, err
		}
	}
	c.emitFn_JSON_OBJECT(len(call.Arguments))
	return ctype{Type: sqltypes.TypeJSON, Col: collationJSON}, nil
}

func isEncodingJSONSafe(col collations.ID) bool {
	switch col.Get().Charset().(type) {
	case charset.Charset_utf8mb4, charset.Charset_utf8mb3, charset.Charset_binary:
		return true
	default:
		return false
	}
}

func (c *compiler) compileToJSONKey(key ctype) {
	if key.Type == sqltypes.VarChar && isEncodingJSONSafe(key.Col.Collation) {
		return
	}
	if key.Type == sqltypes.VarBinary {
		return
	}
	c.emitConvert_xc(1, sqltypes.VarChar, collations.CollationUtf8mb4ID, 0, false)
}
