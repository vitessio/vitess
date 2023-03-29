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
type spec_b struct{}
type spec_cm struct{}
type spec_D struct{}
type spec_d struct{}
type spec_e struct{}
type spec_f struct{}
type spec_Hk struct{}
type spec_hIl struct{}
type spec_i struct{}
type spec_j struct{}
type spec_M struct{}
type spec_p struct{}
type spec_r struct{}
type spec_Ss struct{}
type spec_T struct{}
type spec_U struct{}
type spec_u struct{}
type spec_V struct{}
type spec_v struct{}
type spec_W struct{}
type spec_w struct{}
type spec_X struct{}
type spec_x struct{}
type spec_Y struct{}
type spec_y struct{}

func (spec_a) format(ctx *formatctx, b []byte) []byte {
	return append(b, ctx.t.Weekday().String()[3:]...)
}

func (spec_b) format(ctx *formatctx, b []byte) []byte {
	ctx.initYear()
	return append(b, ctx.month.String()[3:]...)
}

func (spec_cm) format(ctx *formatctx, b []byte) []byte {
	ctx.initYear()
	return appendInt(b, int(ctx.month), 2)
}

func (spec_d) format(ctx *formatctx, b []byte) []byte {
	ctx.initYear()
	return appendInt(b, ctx.day, 2)
}

func (spec_D) format(ctx *formatctx, b []byte) []byte {
	panic("TODO")
}

func (spec_e) format(ctx *formatctx, b []byte) []byte {
	ctx.initYear()
	return appendInt(b, ctx.day, 0)
}

func (spec_f) format(ctx *formatctx, b []byte) []byte {
	f := ctx.t.Nanosecond() / 1000
	l := len(b)
	b = appendInt(b, f, 6)
	return b[:l+int(ctx.prec)]
}

func (spec_Hk) format(ctx *formatctx, b []byte) []byte {
	ctx.initHour()
	return appendInt(b, ctx.hour, 2)
}

func (spec_hIl) format(ctx *formatctx, b []byte) []byte {
	ctx.initHour()
	hr := ctx.hour % 12
	if hr == 0 {
		hr = 12
	}
	return appendInt(b, hr, 2)
}

func (spec_i) format(ctx *formatctx, b []byte) []byte {
	ctx.initHour()
	return appendInt(b, ctx.min, 2)
}

func (spec_j) format(ctx *formatctx, b []byte) []byte {
	return appendInt(b, ctx.t.YearDay(), 3)
}

func (spec_M) format(ctx *formatctx, b []byte) []byte {
	ctx.initYear()
	return append(b, ctx.month.String()...)
}

func (spec_p) format(ctx *formatctx, b []byte) []byte {
	ctx.initHour()
	if ctx.hour < 12 {
		return append(b, "AM"...)
	}
	return append(b, "PM"...)
}

func (spec_r) format(ctx *formatctx, b []byte) []byte {
	b = (spec_hIl{}).format(ctx, b)
	b = append(b, ':')
	b = (spec_i{}).format(ctx, b)
	b = append(b, ':')
	b = (spec_Ss{}).format(ctx, b)
	b = (spec_p{}).format(ctx, b)
	return b
}

func (spec_Ss) format(ctx *formatctx, b []byte) []byte {
	ctx.initHour()
	return appendInt(b, ctx.sec, 2)
}

func (spec_T) format(ctx *formatctx, b []byte) []byte {
	b = (spec_Hk{}).format(ctx, b)
	b = append(b, ':')
	b = (spec_i{}).format(ctx, b)
	b = append(b, ':')
	b = (spec_Ss{}).format(ctx, b)
	return b
}

func (spec_U) format(ctx *formatctx, b []byte) []byte {
	panic("TODO")
}

func (spec_u) format(ctx *formatctx, b []byte) []byte {
	panic("TODO")
}

func (spec_V) format(ctx *formatctx, b []byte) []byte {
	panic("TODO")
}

func (spec_v) format(ctx *formatctx, b []byte) []byte {
	_, week := ctx.t.ISOWeek()
	return appendInt(b, week, 2)
}

func (spec_X) format(ctx *formatctx, b []byte) []byte {
	panic("TODO")
}

func (spec_x) format(ctx *formatctx, b []byte) []byte {
	year, _ := ctx.t.ISOWeek()
	return appendInt(b, year, 4)
}

func (spec_W) format(ctx *formatctx, b []byte) []byte {
	return append(b, ctx.t.Weekday().String()...)
}

func (spec_w) format(ctx *formatctx, b []byte) []byte {
	return appendInt(b, int(ctx.t.Weekday()), 0)
}

func (spec_Y) format(ctx *formatctx, b []byte) []byte {
	ctx.initYear()
	return appendInt(b, ctx.year, 4)
}

func (spec_y) format(ctx *formatctx, b []byte) []byte {
	ctx.initYear()
	y := ctx.year
	if y < 0 {
		y = -y
	}
	return appendInt(b, y%100, 2)
}

type formatctx struct {
	t     time.Time
	year  int
	month time.Month
	day   int
	hour  int
	min   int
	sec   int
	prec  uint8
}

func (f *formatctx) initYear() {
	if f.year == -1 {
		f.year, f.month, f.day = f.t.Date()
	}
}

func (f *formatctx) initHour() {
	if f.hour == -1 {
		f.hour, f.min, f.sec = f.t.Clock()
	}
}

func newformatctx(t time.Time, prec uint8) formatctx {
	return formatctx{
		t:    t,
		year: -1,
		hour: -1,
		prec: prec,
	}
}

// Spec is the interface that must be fulfilled by components that
// implement the translation of specifications to actual time value.
//
// The Append method takes the accumulated byte buffer, and the time to
// use to generate the textual representation. The resulting byte
// sequence must be returned by this method, normally by using the
// append() builtin function.
type Spec interface {
	format(*formatctx, []byte) []byte
}

type verbatimw struct {
	s string
}

// Verbatim returns an Spec suitable for generating static text.
// For static text, this method is slightly favorable than creating
// your own appender, as adjacent verbatim blocks will be combined
// at compile time to produce more efficient Appenders
func Verbatim(s string) Spec {
	return &verbatimw{s: s}
}

func (v *verbatimw) format(_ *formatctx, b []byte) []byte {
	return append(b, v.s...)
}
