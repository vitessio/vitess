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

	"vitess.io/vitess/go/mysql/json/fastparse"
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
			tp.nsec, ok = parseNanoseconds(in, n)
			return "", ok && len(in) == n
		}
	}
	return "", false
}

func parsetimeAny(tp *timeparts, in string) (out string, ok bool) {
	for i := 0; i < len(in); i++ {
		switch r := in[i]; {
		case isSpace(r):
			tp.day, in, ok = getnum(in, false)
			if !ok || tp.day > 34 || !isSpace(in[0]) {
				tp.day = 0
				return "", false
			}

			for len(in) > 0 && isSpace(in[0]) {
				in = in[1:]
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
		tp.nsec, ok = parseNanoseconds(in, n)
		in = in[n:]
	}

	// Maximum time is 838:59:59, so we have to clamp
	// it to that value here if we otherwise successfully
	// parser the time.
	if tp.day > 34 || tp.day == 34 && tp.hour > 22 {
		tp.day = 34
		tp.hour = 22
		tp.min = 59
		tp.sec = 59
		ok = false
	}

	return in, ok
}

func ParseTime(in string) (t Time, ok bool) {
	if len(in) == 0 {
		return Time{}, false
	}
	var neg bool
	if in[0] == '-' {
		neg = true
		in = in[1:]
	}

	var tp timeparts
	in, ok = parsetimeAny(&tp, in)

	hours := uint16(24*tp.day + tp.hour)
	if !tp.isZero() && neg {
		hours |= negMask
	}

	return Time{
		hour:       hours,
		minute:     uint8(tp.min),
		second:     uint8(tp.sec),
		nanosecond: uint32(tp.nsec),
	}, ok && len(in) == 0
}

func ParseDate(s string) (Date, bool) {
	if _, ok := isNumber(s); ok {
		if len(s) >= 8 {
			dt, ok := Date_YYYYMMDD.Parse(s)
			return dt.Date, ok
		}
		dt, ok := Date_YYMMDD.Parse(s)
		return dt.Date, ok
	}

	if len(s) >= 8 {
		if t, ok := Date_YYYY_M_D.Parse(s); ok {
			return t.Date, true
		}
	}
	if len(s) >= 6 {
		if t, ok := Date_YY_M_D.Parse(s); ok {
			return t.Date, true
		}
	}
	return Date{}, false
}

func ParseDateTime(s string) (DateTime, bool) {
	if l, ok := isNumber(s); ok {
		if l >= 14 {
			return DateTime_YYYYMMDDhhmmss.Parse(s)
		}
		return DateTime_YYMMDDhhmmss.Parse(s)
	}
	if t, ok := DateTime_YYYY_M_D_h_m_s.Parse(s); ok {
		return t, true
	}
	if t, ok := DateTime_YY_M_D_h_m_s.Parse(s); ok {
		return t, true
	}
	return DateTime{}, false
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

	t.hour = uint16(i % 100)
	if i/100 != 0 {
		return t, false
	}
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
	if t == 0 || d == 0 {
		return dt, false
	}
	dt.Time, ok = ParseTimeInt64(t)
	if !ok {
		return dt, false
	}
	dt.Date, ok = ParseDateInt64(d)
	return dt, ok
}
