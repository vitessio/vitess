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
	"math"
	"time"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/sqltypes"
)

var SystemTime = time.Now

const maxTimePrec = 6

type (
	builtinNow struct {
		CallExpr
		utc      bool
		onlyTime bool
		prec     uint8
	}

	builtinSysdate struct {
		CallExpr
		prec uint8
	}

	builtinCurdate struct {
		CallExpr
	}

	builtinUtcDate struct {
		CallExpr
	}

	builtinDateFormat struct {
		CallExpr
		collate collations.ID
	}

	builtinDate struct {
		CallExpr
	}

	builtinDayOfMonth struct {
		CallExpr
	}

	builtinDayOfWeek struct {
		CallExpr
	}

	builtinDayOfYear struct {
		CallExpr
	}

	builtinFromUnixtime struct {
		CallExpr
		collate collations.ID
	}

	builtinHour struct {
		CallExpr
	}

	builtinMakedate struct {
		CallExpr
	}

	builtinMaketime struct {
		CallExpr
	}

	builtinMicrosecond struct {
		CallExpr
	}

	builtinMinute struct {
		CallExpr
	}

	builtinMonth struct {
		CallExpr
	}

	builtinMonthName struct {
		CallExpr
		collate collations.ID
	}

	builtinLastDay struct {
		CallExpr
	}

	builtinToDays struct {
		CallExpr
	}

	builtinFromDays struct {
		CallExpr
	}

	builtinTimeToSec struct {
		CallExpr
	}

	builtinQuarter struct {
		CallExpr
	}

	builtinSecond struct {
		CallExpr
	}

	builtinTime struct {
		CallExpr
	}

	builtinUnixTimestamp struct {
		CallExpr
	}

	builtinWeek struct {
		CallExpr
	}

	builtinWeekDay struct {
		CallExpr
	}

	builtinWeekOfYear struct {
		CallExpr
	}

	builtinYear struct {
		CallExpr
	}

	builtinYearWeek struct {
		CallExpr
	}

	builtinDateMath struct {
		CallExpr
		sub     bool
		unit    datetime.IntervalType
		collate collations.ID
	}
)

var _ IR = (*builtinNow)(nil)
var _ IR = (*builtinSysdate)(nil)
var _ IR = (*builtinCurdate)(nil)
var _ IR = (*builtinUtcDate)(nil)
var _ IR = (*builtinDateFormat)(nil)
var _ IR = (*builtinDate)(nil)
var _ IR = (*builtinDayOfMonth)(nil)
var _ IR = (*builtinDayOfWeek)(nil)
var _ IR = (*builtinDayOfYear)(nil)
var _ IR = (*builtinHour)(nil)
var _ IR = (*builtinFromUnixtime)(nil)
var _ IR = (*builtinMakedate)(nil)
var _ IR = (*builtinMaketime)(nil)
var _ IR = (*builtinMicrosecond)(nil)
var _ IR = (*builtinMinute)(nil)
var _ IR = (*builtinMonth)(nil)
var _ IR = (*builtinMonthName)(nil)
var _ IR = (*builtinLastDay)(nil)
var _ IR = (*builtinToDays)(nil)
var _ IR = (*builtinFromDays)(nil)
var _ IR = (*builtinTimeToSec)(nil)
var _ IR = (*builtinQuarter)(nil)
var _ IR = (*builtinSecond)(nil)
var _ IR = (*builtinTime)(nil)
var _ IR = (*builtinUnixTimestamp)(nil)
var _ IR = (*builtinWeek)(nil)
var _ IR = (*builtinWeekDay)(nil)
var _ IR = (*builtinWeekOfYear)(nil)
var _ IR = (*builtinYear)(nil)
var _ IR = (*builtinYearWeek)(nil)

func (call *builtinNow) eval(env *ExpressionEnv) (eval, error) {
	now := env.time(call.utc)
	if call.onlyTime {
		return newEvalTime(now.Time, int(call.prec)), nil
	} else {
		return newEvalDateTime(now, int(call.prec), false), nil
	}
}

func (call *builtinNow) compile(c *compiler) (ctype, error) {
	var t sqltypes.Type

	if call.onlyTime {
		t = sqltypes.Time
		c.asm.Fn_NowTime(call.prec, call.utc)
	} else {
		t = sqltypes.Datetime
		c.asm.Fn_Now(call.prec, call.utc)
	}
	return ctype{Type: t, Col: collationBinary}, nil
}

func (call *builtinNow) constant() bool {
	return false
}

func (call *builtinSysdate) eval(env *ExpressionEnv) (eval, error) {
	now := SystemTime()
	if tz := env.currentTimezone(); tz != nil {
		now = now.In(tz)
	}
	return newEvalDateTime(datetime.NewDateTimeFromStd(now), int(call.prec), false), nil
}

func (call *builtinSysdate) compile(c *compiler) (ctype, error) {
	c.asm.Fn_Sysdate(call.prec)
	return ctype{Type: sqltypes.Datetime, Col: collationBinary}, nil
}

func (call *builtinSysdate) constant() bool {
	return false
}

func (call *builtinCurdate) eval(env *ExpressionEnv) (eval, error) {
	now := env.time(false)
	return newEvalDate(now.Date, false), nil
}

func (*builtinCurdate) compile(c *compiler) (ctype, error) {
	c.asm.Fn_Curdate()
	return ctype{Type: sqltypes.Date, Col: collationBinary}, nil
}

func (call *builtinCurdate) constant() bool {
	return false
}

func (call *builtinUtcDate) eval(env *ExpressionEnv) (eval, error) {
	now := env.time(true)
	return newEvalDate(now.Date, false), nil
}

func (*builtinUtcDate) compile(c *compiler) (ctype, error) {
	c.asm.Fn_UtcDate()
	return ctype{Type: sqltypes.Date, Col: collationBinary}, nil
}

func (call *builtinUtcDate) constant() bool {
	return false
}

func (b *builtinDateFormat) eval(env *ExpressionEnv) (eval, error) {
	date, format, err := b.arg2(env)
	if err != nil {
		return nil, err
	}
	if date == nil || format == nil {
		return nil, nil
	}
	var t *evalTemporal
	switch e := date.(type) {
	case *evalTemporal:
		t = e.toDateTime(datetime.DefaultPrecision, env.now)
	default:
		t = evalToDateTime(date, datetime.DefaultPrecision, env.now, false)
		if t == nil {
			return nil, nil
		}
	}

	f := evalToBinary(format)
	d, err := datetime.Format(f.string(), t.dt, t.prec)
	if err != nil {
		return nil, err
	}
	return newEvalText(d, typedCoercionCollation(sqltypes.VarChar, b.collate)), nil
}

func (call *builtinDateFormat) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Datetime, sqltypes.Date:
	default:
		c.asm.Convert_xDT(1, datetime.DefaultPrecision, false)
	}

	format, err := call.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(format)

	switch format.Type {
	case sqltypes.VarChar, sqltypes.VarBinary:
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, nil)
	}

	col := typedCoercionCollation(sqltypes.VarChar, c.collation)
	c.asm.Fn_DATE_FORMAT(col)
	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: sqltypes.VarChar, Col: col, Flag: arg.Flag | flagNullable}, nil
}

type builtinConvertTz struct {
	CallExpr
}

var _ IR = (*builtinConvertTz)(nil)

func convertTz(dt datetime.DateTime, from, to *time.Location) (datetime.DateTime, bool) {
	buf := datetime.DateTime_YYYY_MM_DD_hh_mm_ss.Format(dt, datetime.DefaultPrecision)
	ts, err := time.ParseInLocation(time.DateTime, hack.String(buf), from)
	if err != nil {
		return datetime.DateTime{}, false
	}

	if ts.Unix() < 0 || ts.Unix() >= maxUnixtime {
		return dt, true
	}
	return datetime.NewDateTimeFromStd(ts.In(to)), true
}

func (call *builtinConvertTz) eval(env *ExpressionEnv) (eval, error) {
	n, err := call.Arguments[0].eval(env)
	if err != nil {
		return nil, err
	}
	from, err := call.Arguments[1].eval(env)
	if err != nil {
		return nil, err
	}
	to, err := call.Arguments[2].eval(env)
	if err != nil {
		return nil, err
	}

	if n == nil || from == nil || to == nil {
		return nil, nil
	}

	f := evalToBinary(from)
	t := evalToBinary(to)

	fromTz, err := datetime.ParseTimeZone(f.string())
	if err != nil {
		return nil, nil
	}

	toTz, err := datetime.ParseTimeZone(t.string())
	if err != nil {
		return nil, nil
	}

	dt := evalToDateTime(n, -1, env.now, false)
	if dt == nil {
		return nil, nil
	}

	out, ok := convertTz(dt.dt, fromTz, toTz)
	if !ok {
		return nil, nil
	}
	return newEvalDateTime(out, int(dt.prec), false), nil
}

func (call *builtinConvertTz) compile(c *compiler) (ctype, error) {
	n, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	from, err := call.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}
	to, err := call.Arguments[2].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck3(n, from, to)

	switch {
	case from.isTextual():
	default:
		c.asm.Convert_xb(2, sqltypes.VarBinary, nil)
	}

	switch {
	case to.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, nil)
	}

	switch n.Type {
	case sqltypes.Datetime, sqltypes.Date:
	default:
		c.asm.Convert_xDT(3, -1, false)
	}

	c.asm.Fn_CONVERT_TZ()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Datetime, Col: collationBinary, Flag: n.Flag | flagNullable}, nil
}

func (b *builtinDate) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToDate(date, env.now, env.sqlmode.AllowZeroDate())
	if d == nil {
		return nil, nil
	}
	return d, nil
}

func (call *builtinDate) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date:
	default:
		c.asm.Convert_xD(1, c.sqlmode.AllowZeroDate())
	}

	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Date, Col: collationBinary, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinDayOfMonth) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToDate(date, env.now, true)
	if d == nil {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Date.Day())), nil
}

func (call *builtinDayOfMonth) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime:
	default:
		c.asm.Convert_xD(1, true)
	}
	c.asm.Fn_DAYOFMONTH()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinDayOfWeek) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToDate(date, env.now, false)
	if d == nil {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Date.Weekday() + 1)), nil
}

func (call *builtinDayOfWeek) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime:
	default:
		c.asm.Convert_xD(1, false)
	}
	c.asm.Fn_DAYOFWEEK()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinDayOfYear) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToDate(date, env.now, false)
	if d == nil {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Date.ToStdTime(env.currentTimezone()).YearDay())), nil
}

func (call *builtinDayOfYear) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime:
	default:
		c.asm.Convert_xD(1, false)
	}
	c.asm.Fn_DAYOFYEAR()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

const maxUnixtime = 32536771200

func (b *builtinFromUnixtime) eval(env *ExpressionEnv) (eval, error) {
	ts, err := b.arg1(env)
	if err != nil {
		return nil, err
	}

	if ts == nil {
		return nil, nil
	}

	prec := 0
	var sec, frac int64

	switch ts := ts.(type) {
	case *evalInt64:
		if ts.i < 0 || ts.i >= maxUnixtime {
			return nil, nil
		}
		sec = ts.i
	case *evalUint64:
		if ts.u >= maxUnixtime {
			return nil, nil
		}
		sec = int64(ts.u)
	case *evalFloat:
		if ts.f < 0 || ts.f >= maxUnixtime {
			return nil, nil
		}
		sf, ff := math.Modf(ts.f)
		sec = int64(sf)
		frac = int64(ff * 1e9)
		prec = maxTimePrec
	case *evalDecimal:
		if ts.dec.Sign() < 0 {
			return nil, nil
		}
		sd, fd := ts.dec.QuoRem(decimal.New(1, 0), 0)
		sec, _ = sd.Int64()
		if sec >= maxUnixtime {
			return nil, nil
		}
		frac, _ = fd.Mul(decimal.New(1, 9)).Int64()
		prec = int(ts.length)
	case *evalTemporal:
		if ts.prec == 0 {
			sec = ts.toInt64()
			if sec < 0 || sec >= maxUnixtime {
				return nil, nil
			}
		} else {
			dec := ts.toDecimal()
			if dec.Sign() < 0 {
				return nil, nil
			}
			sd, fd := dec.QuoRem(decimal.New(1, 0), 0)
			sec, _ = sd.Int64()
			if sec >= maxUnixtime {
				return nil, nil
			}
			frac, _ = fd.Mul(decimal.New(1, 9)).Int64()
			prec = int(ts.prec)
		}
	case *evalBytes:
		if ts.isHexOrBitLiteral() {
			u, _ := ts.toNumericHex()
			if u.u >= maxUnixtime {
				return nil, nil
			}
			sec = int64(u.u)
		} else {
			f, _ := evalToFloat(ts)
			if f.f < 0 || f.f >= maxUnixtime {
				return nil, nil
			}
			sf, ff := math.Modf(f.f)
			sec = int64(sf)
			frac = int64(ff * 1e9)
			prec = maxTimePrec
		}
	default:
		f, _ := evalToFloat(ts)
		if f.f < 0 || f.f >= maxUnixtime {
			return nil, nil
		}
		sf, ff := math.Modf(f.f)
		sec = int64(sf)
		frac = int64(ff * 1e9)
		prec = maxTimePrec
	}

	t := time.Unix(sec, frac)
	if tz := env.currentTimezone(); tz != nil {
		t = t.In(tz)
	}

	dt := newEvalDateTime(datetime.NewDateTimeFromStd(t), prec, env.sqlmode.AllowZeroDate())

	if len(b.Arguments) == 1 {
		return dt, nil
	}

	format, err := b.Arguments[1].eval(env)
	if err != nil {
		return nil, err
	}
	f := evalToBinary(format)
	d, err := datetime.Format(f.string(), dt.dt, dt.prec)
	if err != nil {
		return nil, err
	}
	return newEvalText(d, typedCoercionCollation(sqltypes.VarChar, b.collate)), nil
}

func (call *builtinFromUnixtime) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Int64:
		c.asm.Fn_FROM_UNIXTIME_i()
	case sqltypes.Uint64:
		c.asm.Fn_FROM_UNIXTIME_u()
	case sqltypes.Float64:
		c.asm.Fn_FROM_UNIXTIME_f()
	case sqltypes.Decimal:
		c.asm.Fn_FROM_UNIXTIME_d()
	case sqltypes.Datetime, sqltypes.Date, sqltypes.Time:
		if arg.Size == 0 {
			c.asm.Convert_Ti(1)
			c.asm.Fn_FROM_UNIXTIME_i()
		} else {
			c.asm.Convert_Td(1)
			c.asm.Fn_FROM_UNIXTIME_d()
		}
	case sqltypes.VarChar, sqltypes.VarBinary:
		if arg.isHexOrBitLiteral() {
			c.asm.Convert_xu(1)
			c.asm.Fn_FROM_UNIXTIME_u()
		} else {
			c.asm.Convert_xf(1)
			c.asm.Fn_FROM_UNIXTIME_f()
		}
	default:
		c.asm.Convert_xf(1)
		c.asm.Fn_FROM_UNIXTIME_f()
	}

	if len(call.Arguments) == 1 {
		c.asm.jumpDestination(skip1)
		return ctype{Type: sqltypes.Datetime, Col: collationBinary, Flag: arg.Flag | flagNullable}, nil
	}

	format, err := call.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(format)

	switch format.Type {
	case sqltypes.VarChar, sqltypes.VarBinary:
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, nil)
	}

	col := typedCoercionCollation(sqltypes.VarChar, c.collation)
	c.asm.Fn_DATE_FORMAT(col)
	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: sqltypes.VarChar, Col: col, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinHour) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToTime(date, -1)
	if d == nil {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Time.Hour())), nil
}

func (call *builtinHour) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Time:
	default:
		c.asm.Convert_xT(1, -1)
	}
	c.asm.Fn_HOUR()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func yearDayToTime(loc *time.Location, y, yd int64) time.Time {
	if y >= 0 && y < 100 {
		if y < 70 {
			y += 2000
		} else {
			y += 1900
		}
	}

	if y < 0 || y > 9999 || yd < 1 || yd > math.MaxInt32 {
		return time.Time{}
	}
	t := time.Date(int(y), time.January, 1, 0, 0, 0, 0, loc).AddDate(0, 0, int(yd-1))
	if t.Year() > 9999 {
		return time.Time{}
	}
	return t
}

func (b *builtinMakedate) eval(env *ExpressionEnv) (eval, error) {
	// For some reason, MySQL first evaluates the year day argument.
	yearDay, err := b.Arguments[1].eval(env)
	if err != nil {
		return nil, err
	}
	if yearDay == nil {
		return nil, nil
	}

	year, err := b.Arguments[0].eval(env)
	if err != nil {
		return nil, err
	}
	if year == nil {
		return nil, nil
	}

	y := evalToInt64(year).i
	yd := evalToInt64(yearDay).i

	t := yearDayToTime(env.currentTimezone(), y, yd)
	if t.IsZero() {
		return nil, nil
	}
	return newEvalDate(datetime.NewDateTimeFromStd(t).Date, false), nil
}

func (call *builtinMakedate) compile(c *compiler) (ctype, error) {
	// Similar here, we have to evaluate these in reverse order as well.
	yearDay, err := call.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(yearDay)

	year, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(year)

	switch yearDay.Type {
	case sqltypes.Int64:
	default:
		c.asm.Convert_xi(2)
	}

	switch year.Type {
	case sqltypes.Int64:
	default:
		c.asm.Convert_xi(1)
	}

	c.asm.Fn_MAKEDATE()
	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: sqltypes.Date, Col: collationBinary, Flag: year.Flag | yearDay.Flag | flagNullable}, nil
}

func clampHourMinute(h, m int64) (int64, int64, bool, bool) {
	var clamped bool
	if h > 838 || h < -838 {
		clamped = true
		if h > 0 {
			h = 838
		} else {
			h = -838
		}
		m = 59
	}
	neg := h < 0
	if neg {
		h = -h
	}
	return h, m, neg, clamped
}

func makeTime_i(h, m, s int64) (int64, bool) {
	if m < 0 || m > 59 || s < 0 || s > 59 {
		return 0, false
	}

	h, m, neg, clamped := clampHourMinute(h, m)
	if clamped {
		s = 59
	}

	v := h*10000 + m*100 + s
	if neg {
		v = -v
	}
	return v, true
}

func makeTime_d(h, m int64, s decimal.Decimal) (decimal.Decimal, bool) {
	if m < 0 || m > 59 || s.Sign() < 0 || s.Cmp(decimal.NewFromInt(60)) >= 0 {
		return decimal.Zero, false
	}

	h, m, neg, clamped := clampHourMinute(h, m)
	if clamped {
		s = decimal.NewFromInt(59)
	}

	dec := decimal.NewFromInt(h*10000 + m*100).Add(s)
	if neg {
		dec = dec.Neg()
	}
	return dec, true
}

func makeTime_f(h, m int64, s float64) (float64, bool) {
	if m < 0 || m > 59 || s < 0.0 || s >= 60.0 {
		return 0, false
	}

	h, m, neg, clamped := clampHourMinute(h, m)
	if clamped {
		s = 59.0
	}

	v := float64(h*10000+m*100) + s
	if neg {
		v = -v
	}
	return v, true
}

func (b *builtinMaketime) eval(env *ExpressionEnv) (eval, error) {
	hour, err := b.Arguments[0].eval(env)
	if err != nil {
		return nil, err
	}
	if hour == nil {
		return nil, nil
	}
	min, err := b.Arguments[1].eval(env)
	if err != nil {
		return nil, err
	}
	if min == nil {
		return nil, nil
	}
	sec, err := b.Arguments[2].eval(env)
	if err != nil {
		return nil, err
	}
	if sec == nil {
		return nil, nil
	}

	var h int64
	switch hour := hour.(type) {
	case *evalInt64:
		h = hour.i
	case *evalUint64:
		if hour.u > math.MaxInt64 {
			h = math.MaxInt64
		} else {
			h = int64(hour.u)
		}
	case *evalBytes:
		if hour.isHexOrBitLiteral() {
			hex, ok := hour.toNumericHex()
			if ok {
				if hex.u > math.MaxInt64 {
					h = math.MaxInt64
				} else {
					h = int64(hex.u)
				}
			}
		} else {
			h = evalToInt64(hour).i
		}
	default:
		h = evalToInt64(hour).i
	}

	m := evalToInt64(min).i
	s := evalToNumeric(sec, false)

	var ok bool
	var t datetime.Time
	var l int
	switch s := s.(type) {
	case *evalInt64:
		var v int64
		v, ok = makeTime_i(h, m, s.i)
		if !ok {
			return nil, nil
		}
		t, ok = datetime.ParseTimeInt64(v)
	case *evalUint64:
		var v int64
		v, ok = makeTime_i(h, m, int64(s.u))
		if !ok {
			return nil, nil
		}
		t, ok = datetime.ParseTimeInt64(v)
	case *evalDecimal:
		var v decimal.Decimal
		v, ok = makeTime_d(h, m, s.dec)
		if !ok {
			return nil, nil
		}
		t, l, ok = datetime.ParseTimeDecimal(v, s.length, -1)
	case *evalFloat:
		var v float64
		v, ok = makeTime_f(h, m, s.f)
		if !ok {
			return nil, nil
		}
		t, l, ok = datetime.ParseTimeFloat(v, -1)
	}
	if !ok {
		return nil, nil
	}

	return newEvalTime(t, l), nil
}

func (call *builtinMaketime) compile(c *compiler) (ctype, error) {
	hour, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip1 := c.compileNullCheck1(hour)

	min, err := call.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip2 := c.compileNullCheck1r(min)

	sec, err := call.Arguments[2].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip3 := c.compileNullCheck3(hour, min, sec)

	switch hour.Type {
	case sqltypes.Int64:
	case sqltypes.Uint64:
		c.asm.Clamp_u(3, math.MaxInt64)
		c.asm.Convert_xi(3)
	case sqltypes.VarChar, sqltypes.VarBinary:
		if hour.isHexOrBitLiteral() {
			c.asm.Convert_xu(3)
			c.asm.Clamp_u(3, math.MaxInt64)
			c.asm.Convert_xi(3)
		} else {
			c.asm.Convert_xi(3)
		}
	default:
		c.asm.Convert_xi(3)
	}

	switch min.Type {
	case sqltypes.Int64:
	default:
		c.asm.Convert_xi(2)
	}

	switch sec.Type {
	case sqltypes.Int64:
		c.asm.Fn_MAKETIME_i()
	case sqltypes.Uint64:
		c.asm.Convert_ui(1)
		c.asm.Fn_MAKETIME_i()
	case sqltypes.Decimal:
		c.asm.Fn_MAKETIME_d()
	case sqltypes.Float64:
		c.asm.Fn_MAKETIME_f()
	case sqltypes.VarChar, sqltypes.VarBinary:
		if sec.isHexOrBitLiteral() {
			c.asm.Convert_xi(1)
			c.asm.Fn_MAKETIME_i()
		} else {
			c.asm.Convert_xf(1)
			c.asm.Fn_MAKETIME_f()
		}
	default:
		c.asm.Convert_xf(1)
		c.asm.Fn_MAKETIME_f()
	}

	c.asm.jumpDestination(skip1, skip2, skip3)
	return ctype{Type: sqltypes.Time, Col: collationBinary, Flag: hour.Flag | min.Flag | sec.Flag | flagNullable}, nil
}

func (b *builtinMicrosecond) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToTime(date, -1)
	if d == nil {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Time.Nanosecond() / 1000)), nil
}

func (call *builtinMicrosecond) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Time:
	default:
		c.asm.Convert_xT(1, -1)
	}
	c.asm.Fn_MICROSECOND()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinMinute) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToTime(date, -1)
	if d == nil {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Time.Minute())), nil
}

func (call *builtinMinute) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Time:
	default:
		c.asm.Convert_xT(1, -1)
	}
	c.asm.Fn_MINUTE()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinMonth) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToDate(date, env.now, true)
	if d == nil {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Date.Month())), nil
}

func (call *builtinMonth) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime:
	default:
		c.asm.Convert_xD(1, true)
	}
	c.asm.Fn_MONTH()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinMonthName) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToDate(date, env.now, false)
	if d == nil {
		return nil, nil
	}
	m := d.dt.Date.Month()
	if m < 1 || m > 12 {
		return nil, nil
	}

	return newEvalText(hack.StringBytes(time.Month(d.dt.Date.Month()).String()), typedCoercionCollation(sqltypes.VarChar, b.collate)), nil
}

func (call *builtinMonthName) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime:
	default:
		c.asm.Convert_xD(1, false)
	}
	col := typedCoercionCollation(sqltypes.VarChar, call.collate)
	c.asm.Fn_MONTHNAME(col)
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarChar, Col: col, Flag: arg.Flag | flagNullable}, nil
}

func lastDay(loc *time.Location, dt datetime.DateTime) datetime.Date {
	ts := dt.Date.ToStdTime(loc)
	firstDayOfMonth := time.Date(ts.Year(), ts.Month(), 1, 0, 0, 0, 0, loc)
	lastDayOfMonth := firstDayOfMonth.AddDate(0, 1, -1)

	date := datetime.NewDateFromStd(lastDayOfMonth)
	return date
}

func (b *builtinLastDay) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	dt := evalToDateTime(date, -1, env.now, env.sqlmode.AllowZeroDate())
	if dt == nil || dt.isZero() {
		return nil, nil
	}

	d := lastDay(env.currentTimezone(), dt.dt)
	return newEvalDate(d, env.sqlmode.AllowZeroDate()), nil
}

func (call *builtinLastDay) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime:
	default:
		c.asm.Convert_xD(1, c.sqlmode.AllowZeroDate())
	}
	c.asm.Fn_LAST_DAY()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Date, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinToDays) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	dt := evalToDate(date, env.now, false)
	if dt == nil {
		return nil, nil
	}

	numDays := datetime.MysqlDayNumber(dt.dt.Date.Year(), dt.dt.Date.Month(), dt.dt.Date.Day())
	return newEvalInt64(int64(numDays)), nil
}

func (call *builtinToDays) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime:
	default:
		c.asm.Convert_xD(1, false)
	}
	c.asm.Fn_TO_DAYS()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinFromDays) eval(env *ExpressionEnv) (eval, error) {
	arg, err := b.arg1(env)
	if arg == nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	d := datetime.DateFromDayNumber(int(evalToInt64(arg).i))

	// mysql returns NULL if year is greater than 9999
	if d.Year() > 9999 {
		return nil, nil
	}
	return newEvalDate(d, true), nil
}

func (call *builtinFromDays) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Int64:
	default:
		c.asm.Convert_xi(1)
	}

	c.asm.Fn_FROM_DAYS()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Date, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinTimeToSec) eval(env *ExpressionEnv) (eval, error) {
	arg, err := b.arg1(env)
	if arg == nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	d := evalToTime(arg, -1)
	if d == nil {
		return nil, nil
	}

	sec := d.dt.Time.ToSeconds()
	return newEvalInt64(sec), nil
}

func (call *builtinTimeToSec) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Time:
	default:
		c.asm.Convert_xT(1, -1)
	}

	c.asm.Fn_TIME_TO_SEC()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinQuarter) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToDate(date, env.now, true)
	if d == nil {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Date.Quarter())), nil
}

func (call *builtinQuarter) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime:
	default:
		c.asm.Convert_xD(1, true)
	}
	c.asm.Fn_QUARTER()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinSecond) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToTime(date, -1)
	if d == nil {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Time.Second())), nil
}

func (call *builtinSecond) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Time:
	default:
		c.asm.Convert_xT(1, -1)
	}
	c.asm.Fn_SECOND()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinTime) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToTime(date, -1)
	if d == nil {
		return nil, nil
	}
	return d, nil
}

func (call *builtinTime) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Time:
	default:
		c.asm.Convert_xT(1, -1)
	}
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Time, Col: collationBinary, Flag: arg.Flag | flagNullable}, nil
}

func dateTimeUnixTimestamp(env *ExpressionEnv, date eval) evalNumeric {
	var dt *evalTemporal
	switch e := date.(type) {
	case *evalTemporal:
		dt = e.toDateTime(int(e.prec), env.now)
	default:
		dt = evalToDateTime(date, -1, env.now, false)
		if dt == nil {
			var prec int32
			switch d := date.(type) {
			case *evalInt64, *evalUint64:
				return newEvalInt64(0)
			case *evalDecimal:
				prec = d.length
			case *evalBytes:
				if d.isHexOrBitLiteral() {
					return newEvalInt64(0)
				}
				prec = maxTimePrec
			default:
				prec = maxTimePrec
			}
			return newEvalDecimalWithPrec(decimal.Zero, prec)
		}
	}

	ts := dt.dt.ToStdTime(env.now)
	if ts.Unix() < 0 || ts.Unix() >= maxUnixtime {
		return newEvalInt64(0)
	}

	if dt.prec == 0 {
		return newEvalInt64(ts.Unix())
	}

	dec := decimal.New(ts.Unix(), 0)
	dec = dec.Add(decimal.New(int64(dt.dt.Time.Nanosecond()), -9))
	return newEvalDecimalWithPrec(dec, int32(dt.prec))
}

func (b *builtinUnixTimestamp) eval(env *ExpressionEnv) (eval, error) {
	if len(b.Arguments) == 0 {
		return newEvalInt64(env.now.Unix()), nil
	}

	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}

	if date == nil {
		return nil, nil
	}

	return dateTimeUnixTimestamp(env, date), nil
}

func (call *builtinUnixTimestamp) constant() bool {
	if len(call.Arguments) == 0 {
		return false
	}
	return call.Arguments[0].constant()
}

func (call *builtinUnixTimestamp) compile(c *compiler) (ctype, error) {
	if len(call.Arguments) == 0 {
		c.asm.Fn_UNIX_TIMESTAMP0()
		return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
	}

	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip := c.compileNullCheck1(arg)

	c.asm.Fn_UNIX_TIMESTAMP1()
	c.asm.jumpDestination(skip)
	switch arg.Type {
	case sqltypes.Datetime, sqltypes.Time, sqltypes.Decimal:
		if arg.Size == 0 {
			return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag}, nil
		}
		return ctype{Type: sqltypes.Decimal, Size: arg.Size, Col: collationNumeric, Flag: arg.Flag}, nil
	case sqltypes.Date, sqltypes.Int64, sqltypes.Uint64:
		return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag}, nil
	case sqltypes.VarChar, sqltypes.VarBinary:
		if arg.isHexOrBitLiteral() {
			return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag}, nil
		}
		if lit, ok := call.Arguments[0].(*Literal); ok {
			if dt := evalToDateTime(lit.inner, -1, time.Now(), c.sqlmode.AllowZeroDate()); dt != nil {
				if dt.prec == 0 {
					return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag}, nil
				}
				return ctype{Type: sqltypes.Decimal, Size: int32(dt.prec), Col: collationNumeric, Flag: arg.Flag}, nil
			}
		}
		fallthrough
	default:
		return ctype{Type: sqltypes.Decimal, Size: maxTimePrec, Col: collationNumeric, Flag: arg.Flag}, nil
	}
}

func (b *builtinWeek) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}

	d := evalToDate(date, env.now, false)
	if d == nil {
		return nil, nil
	}

	mode := int64(0)
	if len(b.Arguments) == 2 {
		m, err := b.Arguments[1].eval(env)
		if err != nil {
			return nil, err
		}
		if m == nil {
			return nil, nil
		}
		mode = evalToInt64(m).i
	}

	week := d.dt.Date.Week(int(mode))
	return newEvalInt64(int64(week)), nil
}

func (call *builtinWeek) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(arg)
	var skip2 *jump

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime:
	default:
		c.asm.Convert_xD(1, false)
	}

	if len(call.Arguments) == 1 {
		c.asm.Fn_WEEK0()
	} else {
		mode, err := call.Arguments[1].compile(c)
		if err != nil {
			return ctype{}, err
		}
		skip2 = c.compileNullCheck1r(mode)
		c.asm.Convert_xi(1)
		c.asm.Fn_WEEK()
	}

	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinWeekDay) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToDate(date, env.now, false)
	if d == nil {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Date.Weekday()+6) % 7), nil
}

func (call *builtinWeekDay) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime:
	default:
		c.asm.Convert_xD(1, false)
	}

	c.asm.Fn_WEEKDAY()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinWeekOfYear) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToDate(date, env.now, false)
	if d == nil {
		return nil, nil
	}

	_, week := d.dt.Date.ISOWeek()
	return newEvalInt64(int64(week)), nil
}

func (call *builtinWeekOfYear) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime:
	default:
		c.asm.Convert_xD(1, false)
	}

	c.asm.Fn_WEEKOFYEAR()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinYear) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToDate(date, env.now, true)
	if d == nil {
		return nil, nil
	}

	return newEvalInt64(int64(d.dt.Date.Year())), nil
}

func (call *builtinYear) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime:
	default:
		c.asm.Convert_xD(1, true)
	}

	c.asm.Fn_YEAR()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinYearWeek) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}

	d := evalToDate(date, env.now, false)
	if d == nil {
		return nil, nil
	}

	mode := int64(0)
	if len(b.Arguments) == 2 {
		m, err := b.Arguments[1].eval(env)
		if err != nil {
			return nil, err
		}
		if m == nil {
			return nil, nil
		}
		mode = evalToInt64(m).i
	}

	week := d.dt.Date.YearWeek(int(mode))
	return newEvalInt64(int64(week)), nil
}

func (call *builtinYearWeek) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(arg)
	var skip2 *jump

	switch arg.Type {
	case sqltypes.Date, sqltypes.Datetime:
	default:
		c.asm.Convert_xD(1, false)
	}

	if len(call.Arguments) == 1 {
		c.asm.Fn_YEARWEEK0()
	} else {
		mode, err := call.Arguments[1].compile(c)
		if err != nil {
			return ctype{}, err
		}
		skip2 = c.compileNullCheck1r(mode)
		c.asm.Convert_xi(1)
		c.asm.Fn_YEARWEEK()
	}

	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
}

func evalToInterval(itv eval, unit datetime.IntervalType, negate bool) *datetime.Interval {
	switch itv := itv.(type) {
	case *evalBytes:
		return datetime.ParseInterval(itv.string(), unit, negate)
	case *evalFloat:
		return datetime.ParseIntervalFloat(itv.f, unit, negate)
	case *evalDecimal:
		return datetime.ParseIntervalDecimal(itv.dec, itv.length, unit, negate)
	default:
		return datetime.ParseIntervalInt64(evalToNumeric(itv, false).toInt64().i, unit, negate)
	}
}

func (call *builtinDateMath) eval(env *ExpressionEnv) (eval, error) {
	date, err := call.Arguments[0].eval(env)
	if err != nil || date == nil {
		return date, err
	}

	itv, err := call.Arguments[1].eval(env)
	if err != nil || itv == nil {
		return itv, err
	}

	interval := evalToInterval(itv, call.unit, call.sub)
	if interval == nil {
		return nil, nil
	}

	if tmp, ok := date.(*evalTemporal); ok {
		return tmp.addInterval(interval, collations.Unknown, env.now), nil
	}

	if tmp := evalToTemporal(date, env.sqlmode.AllowZeroDate()); tmp != nil {
		return tmp.addInterval(interval, call.collate, env.now), nil
	}

	return nil, nil
}

func (call *builtinDateMath) compile(c *compiler) (ctype, error) {
	date, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	// TODO: constant propagation
	_, err = call.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	var ret ctype
	ret.Flag = date.Flag | flagNullable
	ret.Col = collationBinary

	switch {
	case date.Type == sqltypes.Date && !call.unit.HasTimeParts():
		ret.Type = sqltypes.Date
		c.asm.Fn_DATEADD_D(call.unit, call.sub)
	case date.Type == sqltypes.Time && !call.unit.HasDateParts():
		ret.Type = sqltypes.Time
		c.asm.Fn_DATEADD_D(call.unit, call.sub)
	case date.Type == sqltypes.Datetime || date.Type == sqltypes.Timestamp || (date.Type == sqltypes.Date && call.unit.HasTimeParts()) || (date.Type == sqltypes.Time && call.unit.HasDateParts()):
		ret.Type = sqltypes.Datetime
		c.asm.Fn_DATEADD_D(call.unit, call.sub)
	default:
		ret.Type = sqltypes.Char
		ret.Col = typedCoercionCollation(sqltypes.Char, c.collation)
		c.asm.Fn_DATEADD_s(call.unit, call.sub, ret.Col.Collation)
	}
	return ret, nil
}
