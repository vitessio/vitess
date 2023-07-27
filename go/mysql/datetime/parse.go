/*
Copyright 2019 The Vitess Authors.

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
	"math"
	"strings"

	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/fastparse"
)

func parsetimeHours(tp *timeparts, in string) (out string, ok bool) {
	if tp.hour, in, ok = getnumn(in); ok {
		tp.day = tp.day + tp.hour/24
		tp.hour = tp.hour % 24

		switch {
		case len(in) == 0:
			return "", true
		case in[0] == ':':
			return parsetimeMinutes(tp, in[1:])
		}
	}
	return "", false
}

func parsetimeMinutes(tp *timeparts, in string) (out string, ok bool) {
	if tp.min, in, ok = getnum(in, false); ok {
		switch {
		case tp.min > 59:
			return "", false
		case len(in) == 0:
			return "", true
		case in[0] == ':':
			return parsetimeSeconds(tp, in[1:])
		}
	}
	return "", false
}

func parsetimeSeconds(tp *timeparts, in string) (out string, ok bool) {
	if tp.sec, in, ok = getnum(in, false); ok {
		switch {
		case tp.sec > 59:
			return "", false
		case len(in) == 0:
			return "", true
		case len(in) > 1 && in[0] == '.':
			n := 1
			for ; n < len(in) && isDigit(in, n); n++ {
			}
			var l int
			tp.nsec, l, ok = parseNanoseconds(in, n)
			tp.prec = uint8(l)
			return "", ok && len(in) == n
		}
	}
	return "", false
}

func parsetimeAny(tp *timeparts, in string) (out string, ok bool) {
	orig := in
	for i := 0; i < len(in); i++ {
		switch r := in[i]; {
		case isSpace(r):
			tp.day, in, ok = getnum(in, false)
			if !ok || !isSpace(in[0]) {
				tp.day = 0
				return parsetimeNoDelimiters(tp, orig)
			}
			for len(in) > 0 && isSpace(in[0]) {
				in = in[1:]
			}
			if !isDigit(in, 0) {
				tp.day = 0
				return parsetimeNoDelimiters(tp, orig)
			}
			if tp.day > 34 {
				return "", clampTimeparts(tp)
			}
			return parsetimeHours(tp, in)
		case r == ':':
			return parsetimeHours(tp, in)
		}
	}
	return parsetimeNoDelimiters(tp, in)
}

func parsetimeNoDelimiters(tp *timeparts, in string) (out string, ok bool) {
	var integral int
	for ; integral < len(in); integral++ {
		if in[integral] == '.' || !isDigit(in, integral) {
			break
		}
	}

	switch integral {
	default:
		// MySQL limits this to a numeric value that fits in a 32-bit unsigned integer.
		i, _ := fastparse.ParseInt64(in[:integral], 10)
		if i > math.MaxUint32 {
			return "", false
		}
		if i < -math.MaxUint32 {
			return "", false
		}

		tp.hour, in, ok = getnuml(in, integral-4)
		if !ok {
			return
		}
		tp.day = tp.day + tp.hour/24
		tp.hour = tp.hour % 24
		integral = 4
		fallthrough

	case 3, 4:
		tp.min, in, ok = getnuml(in, integral-2)
		if !ok || tp.min > 59 {
			return "", false
		}
		integral = 2
		fallthrough

	case 1, 2:
		tp.sec, in, ok = getnuml(in, integral)
		if !ok || tp.sec > 59 {
			return "", false
		}
	case 0:
		return "", false
	}

	if len(in) > 1 && in[0] == '.' && isDigit(in, 1) {
		n := 1
		for ; n < len(in) && isDigit(in, n); n++ {
		}
		var l int
		tp.nsec, l, ok = parseNanoseconds(in, n)
		tp.prec = uint8(l)
		in = in[n:]
	}

	return in, clampTimeparts(tp) && ok
}

func clampTimeparts(tp *timeparts) bool {
	// Maximum time is 838:59:59, so we have to clamp
	// it to that value here if we otherwise successfully
	// parser the time.
	if tp.day > 34 || tp.day == 34 && tp.hour > 22 {
		tp.day = 34
		tp.hour = 22
		tp.min = 59
		tp.sec = 59
		return false
	}
	return true
}

func ParseTime(in string, prec int) (t Time, l int, ok bool) {
	in = strings.Trim(in, " \t\r\n")
	if len(in) == 0 {
		return Time{}, 0, false
	}
	var neg bool
	if in[0] == '-' {
		neg = true
		in = in[1:]
	}

	var tp timeparts
	in, ok = parsetimeAny(&tp, in)
	ok = clampTimeparts(&tp) && ok

	hours := uint16(24*tp.day + tp.hour)
	if !tp.isZero() && neg {
		hours |= negMask
	}

	t = Time{
		hour:       hours,
		minute:     uint8(tp.min),
		second:     uint8(tp.sec),
		nanosecond: uint32(tp.nsec),
	}

	if prec < 0 {
		prec = int(tp.prec)
	} else {
		t = t.Round(prec)
	}

	return t, prec, ok && len(in) == 0
}

func ParseDate(s string) (Date, bool) {
	if _, ok := isNumber(s); ok {
		if len(s) >= 8 {
			dt, _, ok := Date_YYYYMMDD.Parse(s, 0)
			return dt.Date, ok
		}
		dt, _, ok := Date_YYMMDD.Parse(s, 0)
		return dt.Date, ok
	}

	if len(s) >= 8 {
		if t, _, ok := Date_YYYY_M_D.Parse(s, 0); ok {
			return t.Date, true
		}
	}
	if len(s) >= 6 {
		if t, _, ok := Date_YY_M_D.Parse(s, 0); ok {
			return t.Date, true
		}
	}
	return Date{}, false
}

func ParseDateTime(s string, l int) (DateTime, int, bool) {
	if sl, ok := isNumber(s); ok {
		if sl >= 14 {
			return DateTime_YYYYMMDDhhmmss.Parse(s, l)
		}
		return DateTime_YYMMDDhhmmss.Parse(s, l)
	}
	if t, l, ok := DateTime_YYYY_M_D_h_m_s.Parse(s, l); ok {
		return t, l, true
	}
	if t, l, ok := DateTime_YY_M_D_h_m_s.Parse(s, l); ok {
		return t, l, true
	}
	return DateTime{}, 0, false
}

func ParseDateInt64(i int64) (d Date, ok bool) {
	if i == 0 {
		return d, true
	}

	d.day = uint8(i % 100)
	i /= 100
	if d.day == 0 || d.day > 31 {
		return d, false
	}

	d.month = uint8(i % 100)
	i /= 100
	if d.month == 0 || d.month > 12 {
		return d, false
	}

	d.year = uint16(i)
	if d.year == 0 {
		return d, false
	}
	if d.year < 100 {
		if d.year < 70 {
			d.year += 2000
		} else {
			d.year += 1900
		}
	}
	if d.year < 1000 || d.year > 9999 {
		return d, false
	}
	return d, true
}

func ParseTimeInt64(i int64) (t Time, ok bool) {
	if i == 0 {
		return t, true
	}
	neg := false
	if i < 0 {
		i = -i
		neg = true
	}

	t.second = uint8(i % 100)
	i /= 100
	if t.second > 59 {
		return t, false
	}

	t.minute = uint8(i % 100)
	i /= 100
	if t.minute > 59 {
		return t, false
	}

	if i > 838 {
		return t, false
	}
	t.hour = uint16(i)
	if neg {
		t.hour |= negMask
	}
	return t, true
}

func ParseDateTimeInt64(i int64) (dt DateTime, ok bool) {
	t := i % 1000000
	d := i / 1000000

	if i == 0 {
		return dt, true
	}
	if d == 0 {
		return dt, false
	}
	dt.Time, ok = ParseTimeInt64(t)
	if !ok {
		return dt, false
	}
	dt.Date, ok = ParseDateInt64(d)
	return dt, ok
}

func ParseDateTimeFloat(f float64, prec int) (DateTime, int, bool) {
	i, frac := math.Modf(f)
	dt, ok := ParseDateTimeInt64(int64(i))
	nsec := int(frac * 1e9)
	dt.Time.nanosecond = uint32(nsec)
	if prec < 0 {
		prec = DefaultPrecision
	} else {
		dt = dt.Round(prec)
	}
	return dt, prec, ok
}

func ParseDateTimeDecimal(d decimal.Decimal, l int32, prec int) (DateTime, int, bool) {
	id, frac := d.QuoRem(decimal.New(1, 0), 0)
	i, _ := id.Int64()
	dt, ok := ParseDateTimeInt64(i)

	rem, _ := frac.Mul(decimal.New(1, 9)).Int64()
	dt.Time.nanosecond = uint32(rem)

	if prec < 0 {
		prec = int(l)
	} else {
		dt = dt.Round(prec)
	}
	return dt, prec, ok
}

func ParseDateFloat(f float64) (Date, bool) {
	i, _ := math.Modf(f)
	return ParseDateInt64(int64(i))
}

func ParseDateDecimal(d decimal.Decimal) (Date, bool) {
	id, _ := d.QuoRem(decimal.New(1, 0), 0)
	i, _ := id.Int64()
	return ParseDateInt64(i)
}

func ParseTimeFloat(f float64, prec int) (Time, int, bool) {
	i, frac := math.Modf(f)
	t, ok := ParseTimeInt64(int64(i))
	ns := int(math.Abs(frac * 1e9))
	t.nanosecond = uint32(ns)

	if prec < 0 {
		prec = DefaultPrecision
	} else {
		t = t.Round(prec)
	}
	return t, prec, ok
}

func ParseTimeDecimal(d decimal.Decimal, l int32, prec int) (Time, int, bool) {
	id, frac := d.QuoRem(decimal.New(1, 0), 0)
	i, _ := id.Int64()

	t, ok := ParseTimeInt64(i)
	rem, _ := frac.Abs().Mul(decimal.New(1e9, 0)).Int64()
	t.nanosecond = uint32(rem)

	if prec < 0 {
		prec = int(l)
	} else {
		t = t.Round(prec)
	}
	return t, prec, ok
}
