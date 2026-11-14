/*
Copyright 2026 The Vitess Authors.

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
	"math"
	"reflect"
	"strconv"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/mysql/fastparse"
	"vitess.io/vitess/go/sqltypes"
)

// strLen is the maximum length of a string expression in characters, the
// way MySQL declares it: CONCAT(s, 'x') on a VARCHAR(10) column s is 11.
// A nil *strLen means that the length is unknown.
//
// A length that depends on a bind variable is resolved at execution, because
// MySQL sees the literal the bind variable stands for: CONCAT(s, :v) is 10 +
// the length of the value of v. Such a length is a function of env.
type strLen struct {
	// n is the length, when it is a constant.
	n int64
	// resolve returns the length against the bind variables of env, and
	// false when it is unknown. It is nil for a constant.
	resolve func(env *ExpressionEnv) (int64, bool)
	// bv is set for the length of a bind variable by itself.
	bv *BindVariable
}

const (
	// maxStrLen is the length of the longest string MySQL declares, a
	// LONGTEXT or a LONGBLOB of 4,294,967,295 bytes. Lengths are capped at it.
	maxStrLen = math.MaxUint32

	// maxVarcharBytes is the longest result MySQL declares as a VARCHAR or a
	// VARBINARY, in bytes. A longer result is a TEXT or a BLOB.
	maxVarcharBytes = 65535

	// maxTextLen is the length of a TEXT or a BLOB, 65,535 characters, and
	// the length given to a TEXT or BLOB value whose length is not known.
	// A utf8mb4 TEXT is above the VARCHAR limit by itself; a binary BLOB is
	// not: UPPER(bl) on a BLOB column is a VARBINARY.
	maxTextLen = 65535

	// maxInt32 is the largest count MySQL takes from a constant argument such
	// as the 3 in REPEAT(s, 3); larger counts are capped at it.
	maxInt32 = math.MaxInt32
)

// strLenLong is the length of a TEXT, BLOB or JSON value whose length is not
// known, and of a function result that is as long as a string can be, such
// as REPEAT(s, i) for a column i.
var strLenLong = newStrLenConst(maxStrLen)

// strLenText is the length of a TEXT or BLOB value whose length is not known.
var strLenText = newStrLenConst(maxTextLen)

func newStrLenConst(n int64) *strLen {
	return &strLen{n: max(0, min(n, maxStrLen))}
}

// constant returns the value of a constant length.
func (l *strLen) constant() (int64, bool) {
	if l == nil || l.resolve != nil {
		return 0, false
	}
	return l.n, true
}

// symbolic returns whether the length depends on bind variables.
func (l *strLen) symbolic() bool {
	return l != nil && l.resolve != nil
}

func (l *strLen) isLong() bool {
	n, ok := l.constant()
	return ok && n == maxStrLen
}

func (l *strLen) isZero() bool {
	n, ok := l.constant()
	return ok && n == 0
}

// get returns the length, resolved against the bind variables of env when
// it depends on them.
func (l *strLen) get(env *ExpressionEnv) optLen {
	switch {
	case l == nil:
		return optLen{}
	case l.resolve == nil:
		return optLen{n: l.n, ok: true}
	}
	n, ok := l.resolve(env)
	return optLen{n: n, ok: ok}
}

// equal compares two lengths: constants by value, the lengths of two bind
// variables by key, so that one compiled program serves every value, and
// any other lengths by identity.
func (l *strLen) equal(other *strLen) bool {
	switch {
	case l == nil || other == nil:
		return l == other
	case l.resolve == nil && other.resolve == nil:
		return l.n == other.n
	case l.bv != nil && other.bv != nil:
		return l.bv.Key == other.bv.Key
	}
	return l == other
}

// optLen is a length that is known or not, for the rules below that
// compute lengths, at compile time and at execution alike.
type optLen struct {
	n  int64
	ok bool
}

func someLen(n int64) optLen {
	return optLen{n: max(0, min(n, maxStrLen)), ok: true}
}

func (x optLen) long() bool {
	return x.ok && x.n == maxStrLen
}

// strLen returns the length as a constant, or nil when it is unknown.
func (x optLen) strLen() *strLen {
	if !x.ok {
		return nil
	}
	return newStrLenConst(x.n)
}

// sumLen returns x + y: CONCAT(s, 'x') is 10 + 1. A sum with a long operand
// is long, even when the other operand is unknown.
func sumLen(x, y optLen) optLen {
	switch {
	case x.long() || y.long():
		return someLen(maxStrLen)
	case !x.ok || !y.ok:
		return optLen{}
	}
	return someLen(x.n + y.n)
}

// productLen returns x × y: REPEAT(s, 3) is 10 × 3. A product with a zero
// operand is zero, even when the other operand is unknown.
func productLen(x, y optLen) optLen {
	switch {
	case (x.ok && x.n == 0) || (y.ok && y.n == 0):
		return someLen(0)
	case !x.ok || !y.ok:
		return optLen{}
	case x.n > maxStrLen/y.n:
		return someLen(maxStrLen)
	}
	return someLen(x.n * y.n)
}

// leastLen returns the smaller of x and y: LEFT(s, 3) is min(10, 3). When
// one operand is unknown, the other one is still an upper bound.
func leastLen(x, y optLen) optLen {
	switch {
	case !x.ok:
		return y
	case !y.ok:
		return x
	}
	return someLen(min(x.n, y.n))
}

// diffLen returns x - y, and 0 when y is larger: SUBSTRING(s, 3) is 10 - 2.
// When y is unknown, x is still an upper bound, and a long x stays long.
func diffLen(x, y optLen) optLen {
	if !x.ok || !y.ok || x.n == maxStrLen {
		return x
	}
	return someLen(x.n - y.n)
}

// strLenSum returns a + b, see sumLen.
func strLenSum(a, b *strLen) *strLen {
	if a.isLong() || b.isLong() {
		return strLenLong
	}
	if a == nil || b == nil {
		return nil
	}
	return applyLen(sumLen, a, b)
}

// strLenProduct returns a × b, see productLen.
func strLenProduct(a, b *strLen) *strLen {
	if a.isZero() || b.isZero() {
		return newStrLenConst(0)
	}
	if a == nil || b == nil {
		return nil
	}
	return applyLen(productLen, a, b)
}

// strLenLeast returns the smaller of a and b, see leastLen.
func strLenLeast(a, b *strLen) *strLen {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return applyLen(leastLen, a, b)
}

// strLenDiff returns a - b, see diffLen.
func strLenDiff(a, b *strLen) *strLen {
	if a == nil || b == nil {
		return a
	}
	return applyLen(diffLen, a, b)
}

// applyLen applies a rule to two lengths: now when both are constants, and
// at execution when one depends on bind variables.
func applyLen(rule func(x, y optLen) optLen, a, b *strLen) *strLen {
	if !a.symbolic() && !b.symbolic() {
		return rule(a.get(nil), b.get(nil)).strLen()
	}
	return &strLen{resolve: func(env *ExpressionEnv) (int64, bool) {
		r := rule(a.get(env), b.get(env))
		return r.n, r.ok
	}}
}

// countLen returns the length of a function that takes integer arguments,
// such as REPEAT(s, 3), from rule, the length of its string argument and its
// integer arguments a and b: now when they are all known, and at execution
// when one depends on bind variables, such as REPEAT(s, :n).
func countLen(rule func(str optLen, a, b constInt, version string) optLen, str *strLen, a, b constInt, version string) *strLen {
	if !str.symbolic() && a.expr == nil && b.expr == nil {
		return rule(str.get(nil), a, b, version).strLen()
	}
	return &strLen{resolve: func(env *ExpressionEnv) (int64, bool) {
		ra, aok := a.resolve(env)
		rb, bok := b.resolve(env)
		if !aok || !bok {
			return 0, false
		}
		r := rule(str.get(env), ra, rb, env.currentVersion())
		return r.n, r.ok
	}}
}

// resolveStrLen returns the length of l against the bind variables of env,
// resolving it once for the bind variables of env.
func (env *ExpressionEnv) resolveStrLen(l *strLen) (int64, bool) {
	if !l.symbolic() {
		r := l.get(env)
		return r.n, r.ok
	}
	if env.strLens == nil || reflect.ValueOf(env.strLensVars).UnsafePointer() != reflect.ValueOf(env.BindVars).UnsafePointer() {
		env.strLens = make(map[*strLen]optLen)
		env.strLensVars = env.BindVars
	}
	r, ok := env.strLens[l]
	if !ok {
		r = l.get(env)
		env.strLens[l] = r
	}
	return r.n, r.ok
}

// bindVarStrLen returns the length of a bind variable by itself: the length
// of its value, as the length of the literal it stands for.
func bindVarStrLen(bv *BindVariable) *strLen {
	return &strLen{bv: bv, resolve: func(env *ExpressionEnv) (int64, bool) {
		return bindVarLength(env, bv)
	}}
}

// bindVarLength returns the length of the value of a bind variable, with
// the rules of evalStrLen: the characters of a string, the bytes of a binary
// string, the digits and a sign of an integer. A bind variable missing from
// env.BindVars is NULL, of length 0. The length is unknown when env has no
// bind variables at all, as when the planner asks for a type.
func bindVarLength(env *ExpressionEnv, bv *BindVariable) (int64, bool) {
	if env == nil || env.BindVars == nil {
		return 0, false
	}
	bvar, ok := env.BindVars[bv.Key]
	if !ok {
		return 0, true
	}
	switch typ := bvar.Type; {
	case typ == sqltypes.Null:
		return 0, true
	case typ == sqltypes.HexNum || typ == sqltypes.HexVal || typ == sqltypes.BitNum:
	case sqltypes.IsText(typ):
		if n, ok := valueCharLength(bvar.Value, collations.CollationForType(typ, bv.Collation)); ok {
			return n, true
		}
	case sqltypes.IsBinary(typ):
		return int64(len(bvar.Value)), true
	case sqltypes.IsSigned(typ):
		if i, err := fastparse.ParseInt64(hack.String(bvar.Value), 10); err == nil {
			return intLength(i), true
		}
	case sqltypes.IsUnsigned(typ):
		if u, err := fastparse.ParseUint64(hack.String(bvar.Value), 10); err == nil {
			return digits(u), true
		}
	}
	value, ok := exprValue(env, bv)
	if !ok {
		return 0, false
	}
	r := evalStrLen(value).get(nil)
	return r.n, r.ok
}

// digits returns the number of decimal digits of u.
func digits(u uint64) int64 {
	n := int64(1)
	for ; u >= 10; u /= 10 {
		n++
	}
	return n
}

// intLength returns the length of an integer literal: its digits and one
// for a sign, whether it has one or not, as MySQL declares 1 and -1 as 2.
func intLength(i int64) int64 {
	u := uint64(i)
	if i < 0 {
		u = -u
	}
	return digits(u) + 1
}

// exprValue returns the value of an expression that depends only on bind
// variables, such as :n or -:n, evaluated with the interpreter. A bind
// variable missing from env.BindVars makes it NULL, as an error does. ok is
// false when env has no bind variables at all.
func exprValue(env *ExpressionEnv, expr IR) (eval, bool) {
	if env == nil || env.BindVars == nil {
		return nil, false
	}
	value, err := expr.eval(env)
	if err != nil {
		return nil, true
	}
	return value, true
}

// constantWithBindVars returns whether an expression is a constant once its
// bind variables are known: it has no column and no function that the
// evalengine does not take as a constant, such as NOW(). MySQL takes such an
// expression as a constant, since the normalizer turned its literals into
// bind variables: REPEAT(s, :n * :m) is REPEAT(s, 2*800) for MySQL. The walk
// follows constant(), which takes every bind variable as not constant; the
// functions whose constant() is false whatever their arguments are listed.
func constantWithBindVars(e IR) bool {
	if e.constant() {
		return true
	}
	switch e := e.(type) {
	case *BindVariable:
		return e.Type != sqltypes.Tuple
	case *builtinNow, *builtinSysdate, *builtinCurdate, *builtinUtcDate, *builtinUUID,
		*builtinUser, *builtinDatabase, *builtinLastInsertID, *builtinRandomBytes:
		return false
	case *builtinUnixTimestamp:
		return len(e.Arguments) > 0 && constantWithBindVars(e.Arguments[0])
	case *ConvertExpr:
		return constantWithBindVars(e.Inner)
	case *ConvertUsingExpr:
		return constantWithBindVars(e.Inner)
	case *NegateExpr:
		return constantWithBindVars(e.Inner)
	case *CollateExpr:
		return constantWithBindVars(e.Inner)
	case *IntroducerExpr:
		return constantWithBindVars(e.Inner)
	case *IsExpr:
		return constantWithBindVars(e.Inner)
	case *BitwiseNotExpr:
		return constantWithBindVars(e.Inner)
	case *NotExpr:
		return constantWithBindVars(e.Inner)
	case *ArithmeticExpr:
		return constantWithBindVars(e.Left) && constantWithBindVars(e.Right)
	case *LogicalExpr:
		return constantWithBindVars(e.Left) && constantWithBindVars(e.Right)
	case *BitwiseExpr:
		return constantWithBindVars(e.Left) && constantWithBindVars(e.Right)
	case *LikeExpr:
		return constantWithBindVars(e.Left) && constantWithBindVars(e.Right)
	case *ComparisonExpr:
		return constantWithBindVars(e.Left) && constantWithBindVars(e.Right)
	case *CaseExpr:
		if e.Else != nil && !constantWithBindVars(e.Else) {
			return false
		}
		for _, c := range e.cases {
			if !constantWithBindVars(c.when) || !constantWithBindVars(c.then) {
				return false
			}
		}
		return true
	case callable:
		for _, arg := range e.callable() {
			if !constantWithBindVars(arg) {
				return false
			}
		}
		return true
	}
	return false
}

// charsetWidth returns the maximum number of bytes of a character in the
// character set of a collation: 4 for utf8mb4, 1 for latin1 and binary, and
// 0 for an unknown collation.
func charsetWidth(col collations.ID) int64 {
	coll := colldata.Lookup(col)
	if coll == nil {
		return 0
	}
	return int64(coll.Charset().MaxWidth())
}

// strLenIn returns the length of a string of collation from once converted
// to the character set of the collation to. Only a conversion to binary
// changes it, to the length in bytes: CONCAT(0x41, 'abc') counts 'abc' as
// 3 × 4 = 12 bytes, as MySQL does, and declares 13.
func strLenIn(l *strLen, from, to collations.ID) *strLen {
	if to != collations.CollationBinaryID || from == collations.CollationBinaryID {
		return l
	}
	width := charsetWidth(from)
	if width == 0 {
		return nil
	}
	return strLenProduct(l, newStrLenConst(width))
}

// stringType returns the type of a string function result from its maximum
// length in characters and its collation: base when the length is unknown,
// otherwise a TEXT (or a BLOB, when base or the collation is binary) when
// the length in bytes is above 65,535, and a VARCHAR (or a VARBINARY, when
// base is binary) when it is not. REPEAT(s, 1638) on a utf8mb4 VARCHAR(10)
// is 16,380 characters, at most 65,520 bytes, so a VARCHAR; REPEAT(s, 1639)
// is a TEXT. The same 16,390 characters in latin1 are a VARCHAR.
func stringType(base sqltypes.Type, length *strLen, col collations.ID) sqltypes.Type {
	n, ok := length.constant()
	if !ok {
		return base
	}
	return stringTypeOf(base, n, col)
}

// stringTypeOf is stringType for a length that is known.
func stringTypeOf(base sqltypes.Type, n int64, col collations.ID) sqltypes.Type {
	width := charsetWidth(col)
	if width == 0 {
		return base
	}
	long := n > maxVarcharBytes/width
	switch {
	case (sqltypes.IsBinary(base) || col == collations.CollationBinaryID) && long:
		return sqltypes.Blob
	case sqltypes.IsBinary(base):
		return sqltypes.VarBinary
	case long:
		return sqltypes.Text
	default:
		return sqltypes.VarChar
	}
}

// stringLen returns the length of an argument as a string: its own length
// when it is known, 65,535 for a TEXT or BLOB argument when it is not, and
// the longest a string can be for a JSON argument.
func (ct *ctype) stringLen() *strLen {
	if ct.StrLen != nil {
		return ct.StrLen
	}
	return typeStrLen(ct.Type)
}

// typeStrLen returns the length of a value of a type whose length is not
// known: 65,535 for a TEXT or BLOB, the longest a string can be for JSON,
// and unknown for any other type.
func typeStrLen(tt sqltypes.Type) *strLen {
	switch tt {
	case sqltypes.Text, sqltypes.Blob:
		return strLenText
	case sqltypes.TypeJSON:
		return strLenLong
	}
	return nil
}

// columnStrLen returns the length of a column as a string: its ColumnLength,
// which MySQL reports in bytes, divided by the width of its character set.
// A utf8mb4 VARCHAR(10) has a ColumnLength of 40 and a length of 10; an INT
// has a ColumnLength of 11 and a length of 11. A column without a
// ColumnLength has the length of its type, see typeStrLen.
func columnStrLen(tt sqltypes.Type, size uint32, col collations.ID) *strLen {
	n, ok := charLength(size, col)
	if !ok {
		return typeStrLen(tt)
	}
	return newStrLenConst(n)
}

// charLength returns a length in bytes as a length in characters of the
// character set of col.
func charLength(size uint32, col collations.ID) (int64, bool) {
	width := charsetWidth(col)
	if size == 0 || width == 0 {
		return 0, false
	}
	return int64(size) / width, true
}

// evalStrLen returns the length of a value as a string, the way MySQL
// declares the length of a literal: 'abc' is 3, 0x4142 is 2, 1 is 2 (one
// digit and a sign), 1.5 is 4, 1e0 is 22, and DATE'2020-01-01' is 10. The
// interpreter uses it for every argument value. A TEXT or BLOB value is the
// result of a function that declared it longer than a VARCHAR can be, so it
// is at least that long: 65,536 bytes for a BLOB, 16,384 characters for a
// utf8mb4 TEXT. NULL is 0: CONCAT(s, NULL) declares 10.
func evalStrLen(e eval) *strLen {
	switch e := e.(type) {
	case nil:
		return newStrLenConst(0)
	case *evalBytes:
		n, ok := valueCharLength(e.bytes, e.col.Collation)
		if !ok {
			return nil
		}
		switch e.SQLType() {
		case sqltypes.Text, sqltypes.Blob:
			// Only a function result is a TEXT or a BLOB here, and only
			// because its declared length is above the VARCHAR limit.
			n = max(n, maxVarcharBytes/max(1, charsetWidth(e.col.Collation))+1)
		}
		return newStrLenConst(n)
	case *evalJSON:
		return strLenLong
	case *evalInt64:
		if e == evalBoolTrue || e == evalBoolFalse {
			return newStrLenConst(1)
		}
		n := len(strconv.FormatInt(e.i, 10))
		if e.i >= 0 {
			n++
		}
		return newStrLenConst(int64(n))
	case *evalUint64:
		return newStrLenConst(int64(len(strconv.FormatUint(e.u, 10))))
	case *evalFloat:
		return newStrLenConst(22)
	case *evalDecimal:
		n := len(e.ToRawBytes())
		if e.dec.Sign() >= 0 {
			n++
		}
		return newStrLenConst(int64(n))
	case *evalTemporal:
		var frac int64
		if e.prec > 0 {
			frac = 1 + int64(e.prec)
		}
		switch e.t {
		case sqltypes.Date:
			return newStrLenConst(10)
		case sqltypes.Datetime, sqltypes.Timestamp:
			return newStrLenConst(19 + frac)
		case sqltypes.Time:
			return newStrLenConst(10 + frac)
		}
	}
	return nil
}

// valueCharLength returns the length of a string in characters, and in
// bytes for a binary string.
func valueCharLength(b []byte, col collations.ID) (int64, bool) {
	if col == collations.CollationBinaryID {
		return int64(len(b)), true
	}
	coll := colldata.Lookup(col)
	if coll == nil {
		return 0, false
	}
	return int64(charset.Length(coll.Charset(), b)), true
}

// argStrLen returns the length of the value of a function argument for the
// interpreter. A value read from a column is a VARCHAR or a VARBINARY here,
// even for a TEXT or BLOB column, so a column argument takes its length from
// its declared type or its field when there is one: 255 for a TINYTEXT.
// Without either, a TEXT or BLOB column is 65,535 characters.
func argStrLen(env *ExpressionEnv, arg IR, e eval) *strLen {
	if col, ok := arg.(*Column); ok {
		if col.typed() && col.strLen != nil {
			return col.strLen
		}
		if col.Offset < len(env.Fields) {
			field := env.Fields[col.Offset]
			if l := columnStrLen(field.Type, field.ColumnLength, collations.ID(field.Charset)); l != nil {
				return l
			}
		}
		if col.Offset < len(env.Row) {
			if l := typeStrLen(env.Row[col.Offset].Type()); l != nil && e != nil {
				return l
			}
		}
	}
	return evalStrLen(e)
}

// strResult is how a string function types its result. When the length of
// the result is known, the type is decided once, at compile time. When it
// depends on bind variables, it is decided for each execution from their
// values. When it is not known, it is decided for each value from its own
// length, the way the interpreter does: REPEAT(dt, 1639) on a DATE column
// whose length is not known is a TEXT, because each value is 16,390
// characters.
type strResult struct {
	base   sqltypes.Type
	tt     sqltypes.Type // the type, when fixed
	length *strLen
	col    collations.ID
	fixed  bool
}

func newStrResult(base sqltypes.Type, length *strLen, col collations.ID) strResult {
	_, fixed := length.constant()
	return strResult{base: base, tt: stringType(base, length, col), length: length, col: col, fixed: fixed || charsetWidth(col) == 0}
}

// typeOf returns the type of a result value.
func (r *strResult) typeOf(env *ExpressionEnv, value []byte) sqltypes.Type {
	if r.fixed {
		return r.tt
	}
	if n, ok := env.resolveStrLen(r.length); ok {
		return stringTypeOf(r.base, n, r.col)
	}
	n, ok := valueCharLength(value, r.col)
	if !ok {
		return r.base
	}
	return stringTypeOf(r.base, n, r.col)
}

// eval returns a result value of the interpreter, typed from its length.
func (r *strResult) eval(env *ExpressionEnv, b []byte, col collations.TypedCollation) *evalBytes {
	return newEvalRaw(r.typeOf(env, b), b, col)
}

// constInt is the value of an integer argument that is a constant, such as
// the 3 in REPEAT(s, 3) or the -1 in LEFT(s, -1). ok is false when the
// argument is not a constant, and null is true when it is NULL. An argument
// that depends only on bind variables, such as :n or -:n, is a constant
// whose value is known at execution: expr is set and n and null are not.
type constInt struct {
	n        int64
	null, ok bool
	expr     IR
}

// newConstInt returns the value of arg, which evaluates to value, when arg
// is a constant once its bind variables are known.
func newConstInt(arg IR, value eval) constInt {
	if !constantWithBindVars(arg) {
		return constInt{}
	}
	return constIntOf(value)
}

// resolve returns the value of the argument against the bind variables of
// env, and false when env has no bind variables at all. A bind variable of
// an integer type is read directly from its value; any other expression is
// evaluated.
func (a constInt) resolve(env *ExpressionEnv) (constInt, bool) {
	if a.expr == nil {
		return a, true
	}
	if env == nil || env.BindVars == nil {
		return constInt{}, false
	}
	if bv, ok := a.expr.(*BindVariable); ok {
		bvar, found := env.BindVars[bv.Key]
		switch {
		case !found || bvar.Type == sqltypes.Null:
			return constInt{null: true, ok: true}, true
		case sqltypes.IsSigned(bvar.Type):
			if i, err := fastparse.ParseInt64(hack.String(bvar.Value), 10); err == nil {
				return constInt{n: i, ok: true}, true
			}
		case sqltypes.IsUnsigned(bvar.Type):
			if u, err := fastparse.ParseUint64(hack.String(bvar.Value), 10); err == nil {
				return constInt{n: int64(min(u, math.MaxInt64)), ok: true}, true
			}
		}
	}
	value, _ := exprValue(env, a.expr)
	return constIntOf(value), true
}

// constIntOf returns a constant integer argument from its value, the way
// MySQL reads it: '3' is 3 and 'abc' is 0. An unsigned value above the
// largest int64 becomes the largest int64, so it stays positive.
func constIntOf(value eval) constInt {
	switch v := value.(type) {
	case nil:
		return constInt{null: true, ok: true}
	case *evalUint64:
		if v.u > math.MaxInt64 {
			return constInt{n: math.MaxInt64, ok: true}
		}
	}
	return constInt{n: evalToInt64(value).i, ok: true}
}

// constInt returns the value of arg when it is a constant, evaluating it at
// compile time. Without constant folding, the -1 in LEFT(s, -1) is a
// negation of the literal 1, so it is evaluated here.
func (c *compiler) constInt(arg IR) constInt {
	if !arg.constant() {
		if constantWithBindVars(arg) {
			return constInt{ok: true, expr: arg}
		}
		return constInt{}
	}
	value, err := arg.eval(EmptyExpressionEnv(c.env))
	if err != nil {
		return constInt{}
	}
	return constIntOf(value)
}

// strLen returns the length of a value of the type as a string: its
// character length when it is known, and the length of its type otherwise.
func (t *Type) strLen() *strLen {
	if t.hasChars {
		return newStrLenConst(int64(t.chars))
	}
	return typeStrLen(t.typ)
}
