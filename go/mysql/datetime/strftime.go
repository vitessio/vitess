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
	"fmt"
	"strings"
	"time"
)

func compile(ds map[byte]Spec, p string, exec func(Spec)) error {
	for l := len(p); l > 0; l = len(p) {
		i := strings.IndexByte(p, '%')
		if i < 0 {
			exec(&verbatim{s: p})
			break
		}
		if i == l-1 {
			return fmt.Errorf(`stray %% at the end of pattern`)
		}

		// we found a '%'. we need the next byte to decide what to do next
		// we already know that i < l - 1
		// everything up to the i is verbatim
		if i > 0 {
			exec(&verbatim{s: p[:i]})
			p = p[i:]
		}

		if spec, ok := ds[p[1]]; ok {
			exec(spec)
		} else {
			exec(&verbatim{s: p[1:2]})
		}
		p = p[2:]
	}
	return nil
}

// Format takes the format `p` and the time `t` to produce the
// format date/time. Note that this function re-compiles the
// pattern every time it is called.
//
// If you know beforehand that you will be reusing the pattern
// within your application, consider creating a `Strftime` object
// and reusing it.
func Format(p string, t time.Time, prec uint8) ([]byte, error) {
	var dst []byte
	var tp = newtimeparts(prec)
	err := compile(DefaultMySQLStrftime, p, func(a Spec) {
		dst = a.format(&tp, t, dst)
	})
	return dst, err
}

// Strftime is the object that represents a compiled strftime pattern
type Strftime struct {
	pattern  string
	compiled []Spec
	prec     uint8
}

// New creates a new Strftime object. If the compilation fails, then
// an error is returned in the second argument.
func New(p string, prec uint8) (*Strftime, error) {
	var list []Spec
	err := compile(DefaultMySQLStrftime, p, func(a Spec) {
		list = append(list, a)
	})
	if err != nil {
		return nil, err
	}
	return &Strftime{pattern: p, compiled: list, prec: prec}, nil
}

// Pattern returns the original pattern string
func (f *Strftime) Pattern() string {
	return f.pattern
}

func (f *Strftime) Format(t time.Time) []byte {
	var tp timeparts
	tp.year = -1
	tp.hour = -1
	tp.prec = f.prec
	return f.format(&tp, make([]byte, 0, len(f.pattern)+10), t)
}

func (f *Strftime) AppendFormat(dst []byte, t time.Time) []byte {
	var tp timeparts
	tp.year = -1
	tp.hour = -1
	tp.prec = f.prec
	return f.format(&tp, dst, t)
}

func (f *Strftime) format(tp *timeparts, b []byte, t time.Time) []byte {
	for _, w := range f.compiled {
		b = w.format(tp, t, b)
	}
	return b
}

func (f *Strftime) FormatString(tm time.Time) string {
	return string(f.Format(tm))
}

func (f *Strftime) parse(s string) (time.Time, string, bool) {
	var tp timeparts
	tp.month = -1
	tp.day = -1
	tp.yday = -1

	var ok bool
	for _, w := range f.compiled {
		s, ok = w.parse(&tp, s)
		if !ok {
			return time.Time{}, "", false
		}
	}
	t, ok := tp.toTime(time.Local)
	return t, s, ok
}

func (f *Strftime) Parse(s string) (time.Time, bool) {
	t, s, ok := f.parse(s)
	return t, ok && len(s) == 0
}

func (f *Strftime) ParseBestEffort(s string) (time.Time, bool) {
	t, _, ok := f.parse(s)
	return t, ok
}

var Date_YYYY_MM_DD = &Strftime{
	pattern: "YYYY-MM-DD",
	compiled: []Spec{
		spec_Y{},
		spec_sep('-'),
		spec_cm{true},
		spec_sep('-'),
		spec_e{},
	},
}

var Date_YY_MM_DD = &Strftime{
	pattern: "YY-MM-DD",
	compiled: []Spec{
		spec_y{},
		spec_sep('-'),
		spec_cm{true},
		spec_sep('-'),
		spec_e{},
	},
}

var Date_YYYYMMDD = &Strftime{
	pattern: "YYYYMMDD",
	compiled: []Spec{
		spec_Y{},
		spec_cm{false},
		spec_e{},
	},
}

var Date_YYMMDD = &Strftime{
	pattern: "YYMMDD",
	compiled: []Spec{
		spec_y{},
		spec_cm{false},
		spec_e{},
	},
}

var DateTime_YYYY_MM_DD_hh_mm_ss = &Strftime{
	pattern: "YYYY-MM-DD hh:mm:ss",
	compiled: []Spec{
		spec_Y{},
		spec_sep('-'),
		spec_cm{true},
		spec_sep('-'),
		spec_e{},
		spec_tsep{},
		spec_Hk{true},
		spec_sep(':'),
		spec_i{true},
		spec_sep(':'),
		spec_Ss{true, false},
	},
}

var DateTime_YY_MM_DD_hh_mm_ss = &Strftime{
	pattern: "YY-MM-DD hh:mm:ss",
	compiled: []Spec{
		spec_y{},
		spec_sep('-'),
		spec_cm{true},
		spec_sep('-'),
		spec_e{},
		spec_tsep{},
		spec_Hk{true},
		spec_sep(':'),
		spec_i{true},
		spec_sep(':'),
		spec_Ss{true, false},
	},
}

var DateTime_YYYYMMDDhhmmss = &Strftime{
	pattern: "YYYYMMDDhhmmss",
	compiled: []Spec{
		spec_Y{},
		spec_cm{false},
		spec_e{},
		spec_Hk{false},
		spec_i{false},
		spec_Ss{false, false},
	},
}

var DateTime_YYMMDDhhmmss = &Strftime{
	pattern: "YYMMDDhhmmss",
	compiled: []Spec{
		spec_y{},
		spec_cm{false},
		spec_e{},
		spec_Hk{false},
		spec_i{false},
		spec_Ss{false, false},
	},
}

var Time_hh_mm_ss = &Strftime{
	pattern: "hh:mm:ss",
	compiled: []Spec{
		spec_Hk{true},
		spec_sep(':'),
		spec_i{true},
		spec_sep(':'),
		spec_Ss{true, true},
	},
}

var Time_hhmmss = &Strftime{
	pattern: "hhmmss",
	compiled: []Spec{
		spec_Hk{false},
		spec_i{false},
		spec_Ss{false, true},
	},
}

func ParseDate(s string) (time.Time, bool) {
	if len(s) >= 8 {
		if t, ok := Date_YYYY_MM_DD.Parse(s); ok {
			return t, true
		}
	}
	if len(s) >= 6 {
		if t, ok := Date_YY_MM_DD.Parse(s); ok {
			return t, true
		}
		if t, ok := Date_YYYYMMDD.Parse(s); ok {
			return t, true
		}
		return Date_YYMMDD.Parse(s)
	}
	return time.Time{}, false
}

func ParseDateTime(s string) (time.Time, bool) {
	if t, ok := DateTime_YYYY_MM_DD_hh_mm_ss.Parse(s); ok {
		return t, true
	}
	if t, ok := DateTime_YY_MM_DD_hh_mm_ss.Parse(s); ok {
		return t, true
	}
	if t, ok := DateTime_YYYYMMDDhhmmss.Parse(s); ok {
		return t, true
	}
	if t, ok := DateTime_YYMMDDhhmmss.Parse(s); ok {
		return t, true
	}
	return time.Time{}, false
}
