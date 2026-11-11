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
	"bytes"
	"math"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type EvalResult struct {
	v            eval
	collationEnv *collations.Environment

	// typ is the declared (static) type of the expression that produced v.
	// When typed is set, Value() produces the value AS this type, so that
	// field metadata obtained through TypeOf and the value always agree.
	typ   ctype
	typed bool
	env   *ExpressionEnv
}

func (env *ExpressionEnv) typedResult(e eval, ct ctype) EvalResult {
	return EvalResult{v: e, collationEnv: env.collationEnv, typ: ct, typed: true, env: env}
}

// Value allows for retrieval of the value we expose for public consumption.
// It will be converted to the passed in collation which is the connection
// collation and what the client expects the result to be in.
func (er EvalResult) Value(id collations.ID) sqltypes.Value {
	e := er.v
	if er.typed && e != nil {
		var v sqltypes.Value
		var done bool
		if e, v, done = er.declaredValue(); done {
			return v
		}
	}
	str, ok := e.(*evalBytes)
	if !ok || str.isBinary() || str.col.Collation == collations.Unknown || str.col.Collation == id {
		return evalToSQLValue(e)
	}

	lookup := colldata.Lookup(id)
	var err error
	var dst []byte
	if lookup != nil {
		dst, err = charset.Convert(nil, lookup.Charset(), str.bytes, colldata.Lookup(str.col.Collation).Charset())
	}
	if lookup == nil || err != nil {
		// If we can't convert, we just return what we have, but it's going
		// to be invalidly encoded. Should normally never happen as only utf8mb4
		// is really supported for the connection character set anyway and all
		// other charsets can be converted to utf8mb4.
		return sqltypes.MakeTrusted(str.SQLType(), str.bytes)
	}
	return sqltypes.MakeTrusted(str.SQLType(), dst)
}

// declaredValue converts er.v (non-nil) to the declared type of the result.
// For integer and float declared types it returns the finished value (done);
// for every other type it returns the converted eval. Declared types the
// engine cannot convert to (ENUM, SET, BIT, JSON, GEOMETRY, BLOB/TEXT, NULL,
// ...) leave the value as it is.
func (er EvalResult) declaredValue() (eval, sqltypes.Value, bool) {
	e := er.v
	t := er.typ.sqlType()
	switch {
	case sqltypes.IsSigned(t):
		var i int64
		if ei, ok := e.(*evalInt64); ok {
			i = ei.i
		} else {
			i = evalToInt64(e).i
		}
		return nil, sqltypes.MakeTrusted(t, strconv.AppendInt(nil, i, 10)), true
	case sqltypes.IsUnsigned(t): // includes YEAR
		var u uint64
		if eu, ok := e.(*evalUint64); ok {
			u = eu.u
		} else {
			u = evalToInt64(e).toUint64().u
		}
		return nil, sqltypes.MakeTrusted(t, strconv.AppendUint(nil, u, 10)), true
	case t == sqltypes.Float32:
		f, _ := evalToFloat(e)
		return nil, sqltypes.MakeTrusted(t, formatFloat32(f.f)), true
	}
	if e.SQLType() == t {
		return e, sqltypes.Value{}, false
	}
	switch t {
	case sqltypes.Float64, sqltypes.Decimal,
		sqltypes.Date, sqltypes.Datetime, sqltypes.Timestamp, sqltypes.Time,
		sqltypes.VarChar, sqltypes.Char, sqltypes.VarBinary, sqltypes.Binary:
		col := er.typ.Col.Collation
		if col == collations.Unknown {
			return e, sqltypes.Value{}, false
		}
		var now time.Time
		var allowZero bool
		if er.env != nil {
			now = er.env.now
			allowZero = er.env.sqlmode.AllowZeroDate()
		}
		var c eval
		switch t {
		case sqltypes.Date:
			if d := evalToDate(e, now, allowZero); d != nil {
				c = d
			}
		case sqltypes.Datetime, sqltypes.Timestamp:
			if d := evalToDateTime(e, int(er.typ.Size), now, allowZero); d != nil {
				c = d
			}
		case sqltypes.Time:
			if d := evalToTime(e, int(er.typ.Size)); d != nil {
				c = d
			}
		default:
			var err error
			if c, err = evalCoerce(e, t, er.typ.Size, er.typ.Scale, col, now, allowZero); err != nil {
				return e, sqltypes.Value{}, false
			}
		}
		if c == nil {
			return nil, sqltypes.NULL, true
		}
		return c, sqltypes.Value{}, false
	}
	return e, sqltypes.Value{}, false
}

// formatFloat32 formats a FLOAT (single precision) value like format.FormatFloat
// does for DOUBLE.
func formatFloat32(f float64) []byte {
	fmtc := byte('f')
	if a := math.Abs(f); a >= 1e15 || (a < 1e-15 && a != 0) {
		fmtc = 'g'
	}
	fstr := strconv.AppendFloat(nil, f, fmtc, -1, 32)
	if idx := bytes.IndexByte(fstr, 'e'); idx >= 0 && fstr[idx+1] == '+' {
		fstr = append(fstr[:idx+1], fstr[idx+2:]...)
	}
	return fstr
}

func (er EvalResult) Collation() collations.ID {
	e := er.v
	if er.typed && e != nil {
		var done bool
		if e, _, done = er.declaredValue(); done {
			return collations.CollationBinaryID
		}
	}
	return evalCollation(e).Collation
}

func (er EvalResult) String() string {
	return er.Value(er.collationEnv.DefaultConnectionCharset()).String()
}

// TupleValues allows for retrieval of the value we expose for public consumption
func (er EvalResult) TupleValues() []sqltypes.Value {
	// TODO: Make this collation-aware
	switch v := er.v.(type) {
	case *evalTuple:
		result := make([]sqltypes.Value, 0, len(v.t))
		for _, val := range v.t {
			result = append(result, evalToSQLValue(val))
		}
		return result
	default:
		return nil
	}
}

func (er EvalResult) MustBoolean() bool {
	b, err := er.ToBooleanStrict()
	if err != nil {
		panic(err)
	}
	return b
}

func (er EvalResult) ToBoolean() bool {
	return evalIsTruthy(er.v) == boolTrue
}

// ToBooleanStrict is used when the casting to a boolean has to be minimally forgiving,
// such as when assigning to a system variable that is expected to be a boolean
func (er EvalResult) ToBooleanStrict() (bool, error) {
	switch v := er.v.(type) {
	case *evalInt64:
		switch v.i {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%d is not a boolean", v.i)
		}
	case *evalUint64:
		switch v.u {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%d is not a boolean", v.u)
		}
	case *evalBytes:
		switch strings.ToLower(v.string()) {
		case "on":
			return true, nil
		case "off":
			return false, nil
		default:
			return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "'%s' is not a boolean", v.string())
		}
	}
	return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "'%s' is not a boolean", er.v.ToRawBytes())
}

// sqlType returns the declared SQL type of this ctype (step 1: same as Type).
func (ct *ctype) sqlType() sqltypes.Type { return ct.Type }
