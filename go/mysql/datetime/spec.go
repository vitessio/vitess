/*
Copyright 2016 lestrrat
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

package datetime

import (
	"time"
)

type Spec interface {
	parser
	formatter
}

type formatter interface {
	format(*timeparts, time.Time, []byte) []byte
}

type parser interface {
	parse(*timeparts, string) (string, bool)
}

type numeric interface {
	numeric(*timeparts, time.Time) (int, int)
}

var shortDayNames = []string{
	"Sun",
	"Mon",
	"Tue",
	"Wed",
	"Thu",
	"Fri",
	"Sat",
}

var shortMonthNames = []string{
	"Jan",
	"Feb",
	"Mar",
	"Apr",
	"May",
	"Jun",
	"Jul",
	"Aug",
	"Sep",
	"Oct",
	"Nov",
	"Dec",
}

type fmtWeekdayNameShort struct{}

func (fmtWeekdayNameShort) format(_ *timeparts, t time.Time, b []byte) []byte {
	return append(b, t.Weekday().String()[3:]...)
}

func (fmtWeekdayNameShort) parse(_ *timeparts, b string) (out string, ok bool) {
	_, out, ok = lookup(shortDayNames, b)
	return
}

type fmtMonthNameShort struct{}

func (fmtMonthNameShort) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initYear(t)
	return append(b, time.Month(tp.month).String()[3:]...)
}

func (fmtMonthNameShort) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.month, out, ok = lookup(shortMonthNames, b)
	return
}

type fmtMonth struct {
	zero bool
}

func (s fmtMonth) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initYear(t)
	if s.zero {
		return appendInt(b, tp.month, 2)
	}
	return appendInt(b, tp.month, 0)
}

func (s fmtMonth) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.month, out, ok = getnum(b, s.zero)
	if ok && (tp.month < 1 || tp.month > 12) {
		ok = false
	}
	return
}

func (s fmtMonth) numeric(tp *timeparts, t time.Time) (int, int) {
	tp.initYear(t)
	return tp.month, 100
}

type fmtMonthDaySuffix struct{}

func (fmtMonthDaySuffix) format(_ *timeparts, _ time.Time, _ []byte) []byte {
	panic("TODO")
}

func (d fmtMonthDaySuffix) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtDay struct {
	zero bool
}

func (s fmtDay) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initYear(t)
	if s.zero {
		return appendInt(b, tp.day, 2)
	}
	return appendInt(b, tp.day, 0)
}

func (s fmtDay) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.day, out, ok = getnum(b, s.zero)
	return
}

func (fmtDay) numeric(tp *timeparts, t time.Time) (int, int) {
	tp.initYear(t)
	return tp.day, 100
}

type fmtNanoseconds struct{}

func appendNsec(b []byte, nsec int, prec int) []byte {
	f := nsec / 1000
	l := len(b) + prec
	b = appendInt(b, f, 6)
	for len(b) < l {
		b = append(b, '0')
	}
	return b[:l]
}

func (fmtNanoseconds) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initHour(t)
	return appendNsec(b, tp.nsec, int(tp.prec))
}

func (f fmtNanoseconds) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtHour24 struct {
	zero bool
}

func (s fmtHour24) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initHour(t)
	if s.zero {
		return appendInt(b, tp.hour, 2)
	}
	return appendInt(b, tp.hour, 0)
}

func (s fmtHour24) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.hour, out, ok = getnum(b, s.zero)
	if tp.hour < 0 || 24 <= tp.hour {
		ok = false
	}
	return
}

func (fmtHour24) numeric(tp *timeparts, t time.Time) (int, int) {
	tp.initHour(t)
	return tp.hour, 100
}

type fmtHour12 struct {
	zero bool
}

func (f fmtHour12) format(tp *timeparts, t time.Time, b []byte) []byte {
	hr, _ := f.numeric(tp, t)
	if f.zero {
		return appendInt(b, hr, 2)
	}
	return appendInt(b, hr, 0)
}

func (f fmtHour12) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.hour, out, ok = getnum(b, f.zero)
	if tp.hour < 0 || 12 < tp.hour {
		ok = false
	}
	return
}

func (f fmtHour12) numeric(tp *timeparts, t time.Time) (int, int) {
	tp.initHour(t)
	hr := tp.hour % 12
	if hr == 0 {
		hr = 12
	}
	return hr, 100
}

type fmtMin struct {
	zero bool
}

func (s fmtMin) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initHour(t)
	if s.zero {
		return appendInt(b, tp.min, 2)
	}
	return appendInt(b, tp.min, 0)
}

func (s fmtMin) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.min, out, ok = getnum(b, s.zero)
	if tp.min < 0 || 60 <= tp.min {
		ok = false
	}
	return
}

func (s fmtMin) numeric(tp *timeparts, t time.Time) (int, int) {
	tp.initHour(t)
	return tp.min, 100
}

type fmtZeroYearDay struct{}

func (fmtZeroYearDay) format(_ *timeparts, t time.Time, b []byte) []byte {
	return appendInt(b, t.YearDay(), 3)
}
func (j fmtZeroYearDay) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtMonthName struct{}

func (fmtMonthName) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initYear(t)
	return append(b, time.Month(tp.month).String()...)
}

func (m fmtMonthName) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtAMorPM struct{}

func (fmtAMorPM) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initHour(t)
	if tp.hour < 12 {
		return append(b, "AM"...)
	}
	return append(b, "PM"...)
}

func (p fmtAMorPM) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtFullTime12 struct{}

func (fmtFullTime12) format(tp *timeparts, t time.Time, b []byte) []byte {
	b = (fmtHour12{true}).format(tp, t, b)
	b = append(b, ':')
	b = (fmtMin{true}).format(tp, t, b)
	b = append(b, ':')
	b = (fmtSecond{true, false}).format(tp, t, b)
	b = (fmtAMorPM{}).format(tp, t, b)
	return b
}

func (r fmtFullTime12) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtSecond struct {
	zero bool
	nsec bool
}

func (s fmtSecond) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initHour(t)
	if s.zero {
		b = appendInt(b, tp.sec, 2)
	} else {
		b = appendInt(b, tp.sec, 0)
	}
	if s.nsec && tp.prec > 0 && tp.nsec != 0 {
		b = append(b, '.')
		b = appendNsec(b, tp.nsec, int(tp.prec))
	}
	return b
}

func (s fmtSecond) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.sec, out, ok = getnum(b, s.zero)
	if tp.sec < 0 || 60 <= tp.sec {
		return "", false
	}
	if s.nsec && len(out) >= 2 && out[0] == '.' && isDigit(out, 1) {
		n := 2
		for ; n < len(out) && isDigit(out, n); n++ {
		}
		tp.nsec, ok = parseNanoseconds(out, n)
		out = out[n:]
	}
	return
}

func (s fmtSecond) numeric(tp *timeparts, t time.Time) (int, int) {
	tp.initHour(t)
	return tp.sec, 100
}

type fmtFullTime24 struct{}

func (fmtFullTime24) format(tp *timeparts, t time.Time, b []byte) []byte {
	b = (fmtHour24{true}).format(tp, t, b)
	b = append(b, ':')
	b = (fmtMin{true}).format(tp, t, b)
	b = append(b, ':')
	b = (fmtSecond{true, false}).format(tp, t, b)
	return b
}

func (t2 fmtFullTime24) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtWeek0 struct{}

func (fmtWeek0) format(ctx *timeparts, t time.Time, b []byte) []byte {
	panic("TODO")
}

func (u fmtWeek0) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtWeek1 struct{}

func (fmtWeek1) format(ctx *timeparts, t time.Time, b []byte) []byte {
	panic("TODO")
}

func (u fmtWeek1) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtWeek2 struct{}

func (fmtWeek2) format(ctx *timeparts, t time.Time, b []byte) []byte {
	panic("TODO")
}

func (v fmtWeek2) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtWeek3 struct{}

func (fmtWeek3) format(_ *timeparts, t time.Time, b []byte) []byte {
	_, week := t.ISOWeek()
	return appendInt(b, week, 2)
}
func (v fmtWeek3) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtWeekdayName struct{}

func (fmtWeekdayName) format(_ *timeparts, t time.Time, b []byte) []byte {
	return append(b, t.Weekday().String()...)
}
func (w fmtWeekdayName) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtWeekday struct{}

func (fmtWeekday) format(_ *timeparts, t time.Time, b []byte) []byte {
	return appendInt(b, int(t.Weekday()), 0)
}
func (w fmtWeekday) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtYearForWeek2 struct{}

func (fmtYearForWeek2) format(ctx *timeparts, t time.Time, b []byte) []byte {
	panic("TODO")
}
func (x fmtYearForWeek2) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtYearForWeek3 struct{}

func (fmtYearForWeek3) format(_ *timeparts, t time.Time, b []byte) []byte {
	year, _ := t.ISOWeek()
	return appendInt(b, year, 4)
}
func (x fmtYearForWeek3) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtYearLong struct{}

func (fmtYearLong) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initYear(t)
	return appendInt(b, tp.year, 4)
}

func (y fmtYearLong) parse(tp *timeparts, b string) (out string, ok bool) {
	if len(b) >= 4 {
		b, out = b[0:4], b[4:]
		tp.year, ok = atoi(b)
	}
	return
}

func (y fmtYearLong) numeric(tp *timeparts, t time.Time) (int, int) {
	tp.initYear(t)
	return tp.year, 10000
}

type fmtYearShort struct{}

func (f fmtYearShort) format(tp *timeparts, t time.Time, b []byte) []byte {
	y, _ := f.numeric(tp, t)
	return appendInt(b, y, 2)
}

func (fmtYearShort) parse(tp *timeparts, b string) (out string, ok bool) {
	if len(b) >= 2 {
		b, out = b[0:2], b[2:]
		if tp.year, ok = atoi(b); ok {
			if tp.year >= 70 {
				tp.year += 1900
			} else {
				tp.year += 2000
			}
		}
	}
	return
}

func (fmtYearShort) numeric(tp *timeparts, t time.Time) (int, int) {
	tp.initYear(t)
	y := tp.year
	if y < 0 {
		y = -y
	}
	return y % 100, 2
}

type fmtVerbatim struct {
	s string
}

func (v *fmtVerbatim) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

func (v *fmtVerbatim) format(_ *timeparts, _ time.Time, b []byte) []byte {
	return append(b, v.s...)
}

type fmtSeparator byte

func (s fmtSeparator) format(_ *timeparts, _ time.Time, b []byte) []byte {
	return append(b, byte(s))
}

func (s fmtSeparator) parse(_ *timeparts, b string) (string, bool) {
	if len(b) > 0 {
		return b[1:], isSeparator(b[0])
	}
	return "", false
}

func isSeparator(b byte) bool {
	switch b {
	case '!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<', '=', '>', '?', '@', '[', '\\', ']', '^', '_', '`', '{', '|', '}', '~':
		return true
	default:
		return false
	}
}

type fmtTimeSeparator struct{}

func (s fmtTimeSeparator) format(_ *timeparts, _ time.Time, b []byte) []byte {
	return append(b, byte(' '))
}

func (s fmtTimeSeparator) parse(_ *timeparts, b string) (string, bool) {
	if len(b) > 0 {
		if b[0] == 'T' {
			return b[1:], true
		}
		if isSpace(b[0]) {
			for len(b) > 0 && isSpace(b[0]) {
				b = b[1:]
			}
			return b, true
		}
	}
	return "", false
}

func isSpace(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\v', '\r':
		return true
	default:
		return false
	}
}
