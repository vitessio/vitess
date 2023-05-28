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
	format(dst []byte, t DateTime, prec uint8) []byte
}

type parser interface {
	parse(*timeparts, string) (string, bool)
}

type numeric interface {
	numeric(t DateTime) (int, int)
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

func (fmtWeekdayNameShort) format(dst []byte, t DateTime, prec uint8) []byte {
	return append(dst, t.Date.Weekday().String()[:3]...)
}

func (fmtWeekdayNameShort) parse(_ *timeparts, b string) (out string, ok bool) {
	_, out, ok = lookup(shortDayNames, b)
	return
}

type fmtMonthNameShort struct{}

func (fmtMonthNameShort) format(dst []byte, t DateTime, prec uint8) []byte {
	return append(dst, time.Month(t.Date.Month()).String()[:3]...)
}

func (fmtMonthNameShort) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.month, out, ok = lookup(shortMonthNames, b)
	return
}

type fmtMonth struct {
	zero bool
}

func (s fmtMonth) format(dst []byte, t DateTime, prec uint8) []byte {
	if s.zero {
		return appendInt(dst, t.Date.Month(), 2)
	}
	return appendInt(dst, t.Date.Month(), 0)
}

func (s fmtMonth) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.month, out, ok = getnum(b, s.zero)
	if ok && (tp.month < 0 || tp.month > 12) {
		ok = false
	}
	return
}

func (s fmtMonth) numeric(t DateTime) (int, int) {
	return t.Date.Month(), 100
}

type fmtMonthDaySuffix struct{}

func (fmtMonthDaySuffix) format(dst []byte, t DateTime, prec uint8) []byte {
	d := t.Date.Day()
	dst = appendInt(dst, d, 0)

	switch {
	case d >= 11 && d < 20:
		return append(dst, "th"...)
	case d%10 == 1:
		return append(dst, "st"...)
	case d%10 == 2:
		return append(dst, "nd"...)
	case d%10 == 3:
		return append(dst, "rd"...)
	default:
		return append(dst, "th"...)
	}
}

func (d fmtMonthDaySuffix) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtDay struct {
	zero bool
}

func (s fmtDay) format(dst []byte, t DateTime, prec uint8) []byte {
	if s.zero {
		return appendInt(dst, t.Date.Day(), 2)
	}
	return appendInt(dst, t.Date.Day(), 0)
}

func (s fmtDay) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.day, out, ok = getnum(b, s.zero)
	return
}

func (fmtDay) numeric(t DateTime) (int, int) {
	return t.Date.Day(), 100
}

type fmtMicroseconds struct{}

func appendNsec(b []byte, nsec int, prec int) []byte {
	f := nsec / 1000
	l := len(b) + prec
	b = appendInt(b, f, 6)
	for len(b) < l {
		b = append(b, '0')
	}
	return b[:l]
}

func (fmtMicroseconds) format(dst []byte, t DateTime, prec uint8) []byte {
	return appendNsec(dst, t.Time.Nanosecond(), 6)
}

func (f fmtMicroseconds) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtHour24 struct {
	zero bool
}

func (s fmtHour24) format(dst []byte, t DateTime, prec uint8) []byte {
	if s.zero {
		return appendInt(dst, t.Time.Hour(), 2)
	}
	return appendInt(dst, t.Time.Hour(), 0)
}

func (s fmtHour24) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.hour, out, ok = getnum(b, s.zero)
	if tp.hour < 0 || 24 <= tp.hour {
		ok = false
	}
	return
}

func (fmtHour24) numeric(t DateTime) (int, int) {
	return t.Time.Hour(), 100
}

type fmtHour12 struct {
	zero bool
}

func (f fmtHour12) format(dst []byte, t DateTime, prec uint8) []byte {
	hr, _ := f.numeric(t)
	if f.zero {
		return appendInt(dst, hr, 2)
	}
	return appendInt(dst, hr, 0)
}

func (f fmtHour12) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.hour, out, ok = getnum(b, f.zero)
	if tp.hour < 0 || 12 < tp.hour {
		ok = false
	}
	return
}

func (f fmtHour12) numeric(t DateTime) (int, int) {
	hr := t.Time.Hour() % 12
	if hr == 0 {
		hr = 12
	}
	return hr, 100
}

type fmtMin struct {
	zero bool
}

func (s fmtMin) format(dst []byte, t DateTime, prec uint8) []byte {
	if s.zero {
		return appendInt(dst, t.Time.Minute(), 2)
	}
	return appendInt(dst, t.Time.Minute(), 0)
}

func (s fmtMin) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.min, out, ok = getnum(b, s.zero)
	if tp.min < 0 || 60 <= tp.min {
		ok = false
	}
	return
}

func (s fmtMin) numeric(t DateTime) (int, int) {
	return t.Time.Minute(), 100
}

type fmtZeroYearDay struct{}

func (fmtZeroYearDay) format(dst []byte, t DateTime, prec uint8) []byte {
	return appendInt(dst, t.Date.Yearday(), 3)
}
func (j fmtZeroYearDay) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtMonthName struct{}

func (fmtMonthName) format(dst []byte, t DateTime, prec uint8) []byte {
	return append(dst, time.Month(t.Date.Month()).String()...)
}

func (m fmtMonthName) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtAMorPM struct{}

func (fmtAMorPM) format(dst []byte, t DateTime, prec uint8) []byte {
	if t.Time.Hour() < 12 {
		return append(dst, "AM"...)
	}
	return append(dst, "PM"...)
}

func (p fmtAMorPM) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtFullTime12 struct{}

func (fmtFullTime12) format(dst []byte, t DateTime, prec uint8) []byte {
	dst = (fmtHour12{true}).format(dst, t, prec)
	dst = append(dst, ':')
	dst = (fmtMin{true}).format(dst, t, prec)
	dst = append(dst, ':')
	dst = (fmtSecond{true, false}).format(dst, t, prec)
	dst = append(dst, ' ')
	dst = (fmtAMorPM{}).format(dst, t, prec)
	return dst
}

func (r fmtFullTime12) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtSecond struct {
	zero bool
	nsec bool
}

func (s fmtSecond) format(dst []byte, t DateTime, prec uint8) []byte {
	if s.zero {
		dst = appendInt(dst, t.Time.Second(), 2)
	} else {
		dst = appendInt(dst, t.Time.Second(), 0)
	}
	if s.nsec && prec > 0 {
		dst = append(dst, '.')
		dst = appendNsec(dst, t.Time.Nanosecond(), int(prec))
	}
	return dst
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
		var l int
		tp.nsec, l, ok = parseNanoseconds(out, n)
		tp.prec = uint8(l)
		out = out[n:]
	}
	return
}

func (s fmtSecond) numeric(t DateTime) (int, int) {
	return t.Time.Second(), 100
}

type fmtFullTime24 struct{}

func (fmtFullTime24) format(dst []byte, t DateTime, prec uint8) []byte {
	dst = (fmtHour24{true}).format(dst, t, prec)
	dst = append(dst, ':')
	dst = (fmtMin{true}).format(dst, t, prec)
	dst = append(dst, ':')
	dst = (fmtSecond{true, false}).format(dst, t, prec)
	return dst
}

func (t2 fmtFullTime24) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtWeek0 struct{}

func (fmtWeek0) format(dst []byte, t DateTime, prec uint8) []byte {
	year, week := t.Date.SundayWeek()
	if year < t.Date.Year() {
		week = 0
	}
	return appendInt(dst, week, 2)
}

func (u fmtWeek0) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtWeek1 struct{}

func (fmtWeek1) format(dst []byte, t DateTime, prec uint8) []byte {
	year, week := t.Date.ISOWeek()
	if year < t.Date.Year() {
		week = 0
	}
	return appendInt(dst, week, 2)
}

func (u fmtWeek1) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtWeek2 struct{}

func (fmtWeek2) format(dst []byte, t DateTime, prec uint8) []byte {
	_, week := t.Date.SundayWeek()
	return appendInt(dst, week, 2)
}

func (v fmtWeek2) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtWeek3 struct{}

func (fmtWeek3) format(dst []byte, t DateTime, prec uint8) []byte {
	_, week := t.Date.ISOWeek()
	return appendInt(dst, week, 2)
}

func (v fmtWeek3) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtWeekdayName struct{}

func (fmtWeekdayName) format(dst []byte, t DateTime, prec uint8) []byte {
	return append(dst, t.Date.Weekday().String()...)
}
func (w fmtWeekdayName) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtWeekday struct{}

func (fmtWeekday) format(dst []byte, t DateTime, prec uint8) []byte {
	return appendInt(dst, int(t.Date.Weekday()), 0)
}
func (w fmtWeekday) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtYearForWeek2 struct{}

func (fmtYearForWeek2) format(dst []byte, t DateTime, prec uint8) []byte {
	year, _ := t.Date.SundayWeek()
	return appendInt(dst, year, 4)
}
func (x fmtYearForWeek2) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtYearForWeek3 struct{}

func (fmtYearForWeek3) format(dst []byte, t DateTime, prec uint8) []byte {
	year, _ := t.Date.ISOWeek()
	return appendInt(dst, year, 4)
}
func (x fmtYearForWeek3) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type fmtYearLong struct{}

func (fmtYearLong) format(dst []byte, t DateTime, prec uint8) []byte {
	return appendInt(dst, t.Date.Year(), 4)
}

func (y fmtYearLong) parse(tp *timeparts, b string) (out string, ok bool) {
	if len(b) >= 4 {
		b, out = b[0:4], b[4:]
		tp.year, ok = atoi(b)
	}
	return
}

func (y fmtYearLong) numeric(t DateTime) (int, int) {
	return t.Date.Year(), 10000
}

type fmtYearShort struct{}

func (f fmtYearShort) format(dst []byte, t DateTime, prec uint8) []byte {
	y, _ := f.numeric(t)
	return appendInt(dst, y, 2)
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

func (fmtYearShort) numeric(t DateTime) (int, int) {
	y := t.Date.Year()
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

func (v *fmtVerbatim) format(dst []byte, t DateTime, prec uint8) []byte {
	return append(dst, v.s...)
}

type fmtSeparator byte

func (s fmtSeparator) format(dst []byte, t DateTime, prec uint8) []byte {
	return append(dst, byte(s))
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

func (s fmtTimeSeparator) format(dst []byte, t DateTime, prec uint8) []byte {
	return append(dst, byte(' '))
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
