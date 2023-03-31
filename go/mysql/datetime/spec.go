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

var longDayNames = []string{
	"Sunday",
	"Monday",
	"Tuesday",
	"Wednesday",
	"Thursday",
	"Friday",
	"Saturday",
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

var longMonthNames = []string{
	"January",
	"February",
	"March",
	"April",
	"May",
	"June",
	"July",
	"August",
	"September",
	"October",
	"November",
	"December",
}

var DefaultMySQLStrftime = map[byte]Spec{
	'a': spec_a{},
	'b': spec_b{},
	'c': spec_cm{},
	'D': spec_D{},
	'd': spec_d{},
	'e': spec_e{},
	'f': spec_f{},
	'H': spec_Hk{},
	'h': spec_hIl{},
	'I': spec_hIl{},
	'i': spec_i{},
	'j': spec_j{},
	'k': spec_Hk{},
	'l': spec_hIl{},
	'M': spec_M{},
	'm': spec_cm{},
	'p': spec_p{},
	'r': spec_r{},
	'S': spec_Ss{},
	's': spec_Ss{},
	'T': spec_T{},
	'U': spec_U{},
	'u': spec_u{},
	'V': spec_V{},
	'v': spec_v{},
	'W': spec_W{},
	'w': spec_w{},
	'X': spec_X{},
	'x': spec_x{},
	'Y': spec_Y{},
	'y': spec_y{},
}

type spec_a struct{}

func (spec_a) format(_ *timeparts, t time.Time, b []byte) []byte {
	return append(b, t.Weekday().String()[3:]...)
}

func (spec_a) parse(_ *timeparts, b string) (out string, ok bool) {
	_, out, ok = lookup(shortDayNames, b)
	return
}

type spec_b struct{}

func (spec_b) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initYear(t)
	return append(b, time.Month(tp.month).String()[3:]...)
}

func (spec_b) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.month, out, ok = lookup(shortMonthNames, b)
	return
}

type spec_cm struct {
	relaxed bool
}

func (spec_cm) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initYear(t)
	return appendInt(b, tp.month, 2)
}

func (s spec_cm) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.month, out, ok = getnum(b, !s.relaxed)
	if ok && (tp.month < 1 || tp.month > 12) {
		ok = false
	}
	return
}

type spec_D struct{}

func (spec_D) format(_ *timeparts, _ time.Time, _ []byte) []byte {
	panic("TODO")
}

func (d spec_D) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_d struct{}

func (spec_d) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initYear(t)
	return appendInt(b, tp.day, 2)
}

func (spec_d) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.day, out, ok = getnum(b, true)
	return
}

type spec_e struct{}

func (spec_e) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initYear(t)
	return appendInt(b, tp.day, 0)
}

func (spec_e) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.day, out, ok = getnum(b, false)
	return
}

type spec_f struct{}

func (spec_f) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initHour(t)
	f := tp.nsec / 1000
	l := len(b) + int(tp.prec)
	b = appendInt(b, f, 6)
	for len(b) < l {
		b = append(b, '0')
	}
	return b[:l]
}

func (f spec_f) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_Hk struct {
	relaxed bool
}

func (spec_Hk) relax() Spec {
	return spec_Hk{relaxed: true}
}

func (spec_Hk) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initHour(t)
	return appendInt(b, tp.hour, 2)
}

func (s spec_Hk) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.hour, out, ok = getnum(b, !s.relaxed)
	if tp.hour < 0 || 24 <= tp.hour {
		ok = false
	}
	return
}

type spec_hIl struct{}

func (spec_hIl) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initHour(t)
	hr := tp.hour % 12
	if hr == 0 {
		hr = 12
	}
	return appendInt(b, hr, 2)
}

func (spec_hIl) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.hour, out, ok = getnum(b, true)
	if tp.hour < 0 || 12 < tp.hour {
		ok = false
	}
	return
}

type spec_i struct {
	relaxed bool
}

func (spec_i) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initHour(t)
	return appendInt(b, tp.min, 2)
}

func (s spec_i) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.min, out, ok = getnum(b, !s.relaxed)
	if tp.min < 0 || 60 <= tp.min {
		ok = false
	}
	return
}

type spec_j struct{}

func (spec_j) format(_ *timeparts, t time.Time, b []byte) []byte {
	return appendInt(b, t.YearDay(), 3)
}
func (j spec_j) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_M struct{}

func (spec_M) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initYear(t)
	return append(b, time.Month(tp.month).String()...)
}

func (m spec_M) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_p struct{}

func (spec_p) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initHour(t)
	if tp.hour < 12 {
		return append(b, "AM"...)
	}
	return append(b, "PM"...)
}

func (p spec_p) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_r struct{}

func (spec_r) format(tp *timeparts, t time.Time, b []byte) []byte {
	b = (spec_hIl{}).format(tp, t, b)
	b = append(b, ':')
	b = (spec_i{}).format(tp, t, b)
	b = append(b, ':')
	b = (spec_Ss{}).format(tp, t, b)
	b = (spec_p{}).format(tp, t, b)
	return b
}

func (r spec_r) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_Ss struct {
	relaxed bool
	nsec    bool
}

func (s spec_Ss) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initHour(t)
	b = appendInt(b, tp.sec, 2)
	if s.nsec && tp.nsec != 0 {
		b = append(b, '.')
		b = spec_f{}.format(tp, t, b)
	}
	return b
}

func (s spec_Ss) parse(tp *timeparts, b string) (out string, ok bool) {
	tp.sec, out, ok = getnum(b, !s.relaxed)
	if tp.sec < 0 || 60 <= tp.sec {
		return "", false
	}
	if len(out) >= 2 && out[0] == '.' && isDigit(out, 1) {
		n := 2
		for ; n < len(out) && isDigit(out, n); n++ {
		}
		tp.nsec, ok = parseNanoseconds(out, n)
		out = out[n:]
	}
	return
}

type spec_T struct{}

func (spec_T) format(tp *timeparts, t time.Time, b []byte) []byte {
	b = (spec_Hk{}).format(tp, t, b)
	b = append(b, ':')
	b = (spec_i{}).format(tp, t, b)
	b = append(b, ':')
	b = (spec_Ss{}).format(tp, t, b)
	return b
}

func (t2 spec_T) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_U struct{}

func (spec_U) format(ctx *timeparts, t time.Time, b []byte) []byte {
	panic("TODO")
}

func (u spec_U) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_u struct{}

func (spec_u) format(ctx *timeparts, t time.Time, b []byte) []byte {
	panic("TODO")
}

func (u spec_u) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_V struct{}

func (spec_V) format(ctx *timeparts, t time.Time, b []byte) []byte {
	panic("TODO")
}

func (v spec_V) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_v struct{}

func (spec_v) format(_ *timeparts, t time.Time, b []byte) []byte {
	_, week := t.ISOWeek()
	return appendInt(b, week, 2)
}
func (v spec_v) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_W struct{}

func (spec_W) format(_ *timeparts, t time.Time, b []byte) []byte {
	return append(b, t.Weekday().String()...)
}
func (w spec_W) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_w struct{}

func (spec_w) format(_ *timeparts, t time.Time, b []byte) []byte {
	return appendInt(b, int(t.Weekday()), 0)
}
func (w spec_w) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_X struct{}

func (spec_X) format(ctx *timeparts, t time.Time, b []byte) []byte {
	panic("TODO")
}
func (x spec_X) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_x struct{}

func (spec_x) format(_ *timeparts, t time.Time, b []byte) []byte {
	year, _ := t.ISOWeek()
	return appendInt(b, year, 4)
}
func (x spec_x) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

type spec_Y struct{}

func (spec_Y) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initYear(t)
	return appendInt(b, tp.year, 4)
}

func (y spec_Y) parse(tp *timeparts, b string) (out string, ok bool) {
	if len(b) >= 4 {
		b, out = b[0:4], b[4:]
		tp.year, ok = atoi(b)
	}
	return
}

type spec_y struct{}

func (spec_y) format(tp *timeparts, t time.Time, b []byte) []byte {
	tp.initYear(t)
	y := tp.year
	if y < 0 {
		y = -y
	}
	return appendInt(b, y%100, 2)
}

func (spec_y) parse(tp *timeparts, b string) (out string, ok bool) {
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

type timeparts struct {
	year  int
	month int
	day   int
	yday  int
	hour  int
	min   int
	sec   int
	nsec  int

	pmset bool
	amset bool
	prec  uint8
}

func (tp *timeparts) toTime(loc *time.Location) (time.Time, bool) {
	if tp.pmset && tp.hour < 12 {
		tp.hour += 12
	} else if tp.amset && tp.hour == 12 {
		tp.hour = 0
	}
	if tp.yday > 0 {
		return time.Time{}, false
	} else {
		if tp.month < 0 {
			tp.month = int(time.January)
		}
		if tp.day < 0 {
			tp.day = 1
		}
	}
	if tp.day < 1 || tp.day > daysIn(time.Month(tp.month), tp.year) {
		return time.Time{}, false
	}
	return time.Date(tp.year, time.Month(tp.month), tp.day, tp.hour, tp.min, tp.sec, tp.nsec, loc), true
}

func (tp *timeparts) initYear(t time.Time) {
	if tp.year == -1 {
		var month time.Month
		tp.year, month, tp.day = t.Date()
		tp.month = int(month)
	}
}

func (tp *timeparts) initHour(t time.Time) {
	if tp.hour == -1 {
		tp.hour, tp.min, tp.sec = t.Clock()
		tp.nsec = t.Nanosecond()
	}
}

func newtimeparts(prec uint8) timeparts {
	return timeparts{
		year: -1,
		hour: -1,
		prec: prec,
	}
}

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

type verbatim struct {
	s string
}

func (v *verbatim) parse(t *timeparts, bytes string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

func (v *verbatim) format(_ *timeparts, _ time.Time, b []byte) []byte {
	return append(b, v.s...)
}

type spec_sep byte

func (s spec_sep) format(_ *timeparts, _ time.Time, b []byte) []byte {
	return append(b, byte(s))
}

func (s spec_sep) parse(_ *timeparts, b string) (string, bool) {
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

type spec_tsep struct{}

func (s spec_tsep) format(_ *timeparts, _ time.Time, b []byte) []byte {
	return append(b, byte(' '))
}

func (s spec_tsep) parse(_ *timeparts, b string) (string, bool) {
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
