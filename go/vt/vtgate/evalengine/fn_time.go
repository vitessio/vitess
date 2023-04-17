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
	"time"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var SystemTime = time.Now

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

	builtinHour struct {
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

	builtinQuarter struct {
		CallExpr
	}

	builtinSecond struct {
		CallExpr
	}

	builtinTime struct {
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
)

var _ Expr = (*builtinNow)(nil)
var _ Expr = (*builtinSysdate)(nil)
var _ Expr = (*builtinCurdate)(nil)
var _ Expr = (*builtinUtcDate)(nil)
var _ Expr = (*builtinDateFormat)(nil)
var _ Expr = (*builtinDate)(nil)
var _ Expr = (*builtinDayOfMonth)(nil)
var _ Expr = (*builtinDayOfWeek)(nil)
var _ Expr = (*builtinDayOfYear)(nil)
var _ Expr = (*builtinHour)(nil)
var _ Expr = (*builtinMicrosecond)(nil)
var _ Expr = (*builtinMinute)(nil)
var _ Expr = (*builtinMonth)(nil)
var _ Expr = (*builtinMonthName)(nil)
var _ Expr = (*builtinQuarter)(nil)
var _ Expr = (*builtinSecond)(nil)
var _ Expr = (*builtinTime)(nil)
var _ Expr = (*builtinWeek)(nil)
var _ Expr = (*builtinWeekDay)(nil)
var _ Expr = (*builtinWeekOfYear)(nil)
var _ Expr = (*builtinYear)(nil)
var _ Expr = (*builtinYearWeek)(nil)

func (call *builtinNow) eval(env *ExpressionEnv) (eval, error) {
	now := env.time(call.utc)
	if call.onlyTime {
		buf := datetime.Time_hh_mm_ss.Format(now, call.prec)
		return newEvalRaw(sqltypes.Time, buf, collationBinary), nil
	} else {
		buf := datetime.DateTime_YYYY_MM_DD_hh_mm_ss.Format(now, call.prec)
		return newEvalRaw(sqltypes.Datetime, buf, collationBinary), nil
	}
}

func (call *builtinNow) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	if call.onlyTime {
		return sqltypes.Time, 0
	}
	return sqltypes.Datetime, 0
}

func (call *builtinNow) compile(c *compiler) (ctype, error) {
	var format *datetime.Strftime
	var t sqltypes.Type

	if call.onlyTime {
		format = datetime.Time_hh_mm_ss
		t = sqltypes.Time
	} else {
		format = datetime.DateTime_YYYY_MM_DD_hh_mm_ss
		t = sqltypes.Datetime
	}
	c.asm.Fn_Now(t, format, call.prec, call.utc)
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
	return newEvalRaw(sqltypes.Datetime, datetime.FromStdTime(now).Format(call.prec), collationBinary), nil
}

func (call *builtinSysdate) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Datetime, 0
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
	return newEvalRaw(sqltypes.Date, datetime.Date_YYYY_MM_DD.Format(now, 0), collationBinary), nil
}

func (call *builtinCurdate) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Date, 0
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
	return newEvalRaw(sqltypes.Date, datetime.Date_YYYY_MM_DD.Format(now, 0), collationBinary), nil
}

func (call *builtinUtcDate) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Date, 0
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
		t = e.toDateTime(datetime.DefaultPrecision)
	default:
		t = evalToDateTime(date, datetime.DefaultPrecision)
		if t == nil || t.isZero() {
			return nil, nil
		}
	}

	f := evalToBinary(format)
	d, err := datetime.Format(f.string(), t.dt, t.prec)
	if err != nil {
		return nil, err
	}
	return newEvalText(d, defaultCoercionCollation(b.collate)), nil
}

func (b *builtinDateFormat) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.VarChar, flagNullable
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
		c.asm.Convert_xDT_nz(1, datetime.DefaultPrecision)
	}

	format, err := call.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(format)

	switch format.Type {
	case sqltypes.VarChar, sqltypes.VarBinary:
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, 0, false)
	}

	col := defaultCoercionCollation(c.cfg.Collation)
	c.asm.Fn_DATE_FORMAT(col)
	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: sqltypes.VarChar, Col: col, Flag: arg.Flag | flagNullable}, nil
}

type builtinConvertTz struct {
	CallExpr
}

var _ Expr = (*builtinConvertTz)(nil)

func convertTz(dt datetime.DateTime, from, to *time.Location) (datetime.DateTime, bool) {
	buf := datetime.DateTime_YYYY_MM_DD_hh_mm_ss.Format(dt, datetime.DefaultPrecision)
	ts, err := time.ParseInLocation(time.DateTime, hack.String(buf), from)
	if err != nil {
		return datetime.DateTime{}, false
	}
	return datetime.FromStdTime(ts.In(to)), true
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

	dt := evalToDateTime(n, -1)
	if dt == nil || dt.isZero() {
		return nil, nil
	}

	out, ok := convertTz(dt.dt, fromTz, toTz)
	if !ok {
		return nil, nil
	}
	return newEvalDateTime(out, int(dt.prec)), nil
}

func (call *builtinConvertTz) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Datetime, f | flagNullable
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
		c.asm.Convert_xb(2, sqltypes.VarBinary, 0, false)
	}

	switch {
	case to.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, 0, false)
	}

	switch n.Type {
	case sqltypes.Datetime, sqltypes.Date:
	default:
		c.asm.Convert_xDT_nz(3, -1)
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
	d := evalToDate(date)
	if d == nil {
		return nil, nil
	}
	return d, nil
}

func (b *builtinDate) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Date, flagNullable
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
		c.asm.Convert_xD(1)
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
	d := evalToDate(date)
	if d == nil {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Date.Day())), nil
}

func (b *builtinDayOfMonth) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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
		c.asm.Convert_xD(1)
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
	d := evalToDate(date)
	if d == nil || d.isZero() {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Date.ToStdTime(time.Local).Weekday() + 1)), nil
}

func (b *builtinDayOfWeek) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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
		c.asm.Convert_xD_nz(1)
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
	d := evalToDate(date)
	if d == nil || d.isZero() {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Date.ToStdTime(time.Local).YearDay())), nil
}

func (b *builtinDayOfYear) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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
		c.asm.Convert_xD_nz(1)
	}
	c.asm.Fn_DAYOFYEAR()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: arg.Flag | flagNullable}, nil
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

func (b *builtinHour) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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

func (b *builtinMicrosecond) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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

func (b *builtinMinute) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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
	d := evalToDate(date)
	if d == nil {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Date.Month())), nil
}

func (b *builtinMonth) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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
		c.asm.Convert_xD(1)
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
	d := evalToDate(date)
	if d == nil {
		return nil, nil
	}
	m := d.dt.Date.Month()
	if m < 1 || m > 12 {
		return nil, nil
	}

	return newEvalText(hack.StringBytes(time.Month(d.dt.Date.Month()).String()), defaultCoercionCollation(b.collate)), nil
}

func (b *builtinMonthName) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.VarChar, flagNullable
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
		c.asm.Convert_xD(1)
	}
	col := defaultCoercionCollation(call.collate)
	c.asm.Fn_MONTHNAME(col)
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarChar, Col: col, Flag: arg.Flag | flagNullable}, nil
}

func (b *builtinQuarter) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}
	d := evalToDate(date)
	if d == nil {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Date.Quarter())), nil
}

func (b *builtinQuarter) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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
		c.asm.Convert_xD(1)
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

func (b *builtinSecond) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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

func (b *builtinTime) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Time, flagNullable
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

func (b *builtinWeek) eval(env *ExpressionEnv) (eval, error) {
	date, err := b.arg1(env)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}

	d := evalToDate(date)
	if d == nil || d.isZero() {
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

func (b *builtinWeek) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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
		c.asm.Convert_xD_nz(1)
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
	d := evalToDate(date)
	if d == nil || d.isZero() {
		return nil, nil
	}
	return newEvalInt64(int64(d.dt.Date.Weekday()+6) % 7), nil
}

func (b *builtinWeekDay) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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
		c.asm.Convert_xD_nz(1)
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
	d := evalToDate(date)
	if d == nil || d.isZero() {
		return nil, nil
	}

	_, week := d.dt.Date.ISOWeek()
	return newEvalInt64(int64(week)), nil
}

func (b *builtinWeekOfYear) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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
		c.asm.Convert_xD_nz(1)
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
	d := evalToDate(date)
	if d == nil {
		return nil, nil
	}

	return newEvalInt64(int64(d.dt.Date.Year())), nil
}

func (b *builtinYear) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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
		c.asm.Convert_xD(1)
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

	d := evalToDate(date)
	if d == nil || d.isZero() {
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

func (b *builtinYearWeek) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagNullable
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
		c.asm.Convert_xD_nz(1)
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
