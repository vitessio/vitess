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
	"strings"
	"time"
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
		case len(in) > 2 && in[0] == '.' && isDigit(in, 1):
			n := 2
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
		case (r >= '0' && r <= '9') || r == '.':
			continue
		case isSpace(r):
			tp.day, in, ok = getnum(in, false)
			if !ok || tp.day > 34 {
				return "", false
			}
			for len(in) > 0 && isSpace(in[0]) {
				in = in[1:]
			}
			return parsetimeHours(tp, in)
		case r == ':':
			return parsetimeHours(tp, in)
		default:
			return "", false
		}
	}
	return parsetimeNoDelimiters(tp, in)
}

func parsetimeNoDelimiters(tp *timeparts, in string) (out string, ok bool) {
	integral := strings.IndexByte(in, '.')
	if integral < 0 {
		integral = len(in)
	}

	switch integral {
	case 6:
		tp.hour, in, ok = getnum(in, true)
		if !ok {
			return
		}
		tp.day = tp.day + tp.hour/24
		tp.hour = tp.hour % 24
		fallthrough

	case 4:
		tp.min, in, ok = getnum(in, true)
		if !ok || tp.min > 59 {
			return "", false
		}
		fallthrough

	case 2:
		tp.sec, in, ok = getnum(in, true)
		if !ok || tp.sec > 59 {
			return "", false
		}

	default:
		return "", false
	}

	if len(in) > 2 && in[0] == '.' && isDigit(in, 1) {
		n := 2
		for ; n < len(in) && isDigit(in, n); n++ {
		}
		tp.nsec, ok = parseNanoseconds(in, n)
		in = in[n:]
	}

	return in, ok
}

type SQLTime struct {
	Time  time.Time
	Parts struct {
		Hours int
		Mins  int
		Secs  int
		Nsec  int
	}
}

func (t *SQLTime) AppendFormat(b []byte, prec uint8) []byte {
	h := t.Parts.Hours
	if h < 0 {
		h = -h
		b = append(b, '-')
	}
	b = appendInt(b, h, 2)
	b = append(b, ':')
	b = appendInt(b, t.Parts.Mins, 2)
	b = append(b, ':')
	b = appendInt(b, t.Parts.Secs, 2)
	if prec > 0 && t.Parts.Nsec != 0 {
		b = append(b, '.')
		b = appendNsec(b, t.Parts.Nsec, int(prec))
	}
	return b
}

func (t *SQLTime) FormatInt64() (n int64) {
	if t.Parts.Hours < 0 {
		return -(int64(-t.Parts.Hours)*10000 + int64(t.Parts.Mins)*100 + int64(t.Parts.Secs))
	}
	return int64(t.Parts.Hours)*10000 + int64(t.Parts.Mins)*100 + int64(t.Parts.Secs)
}

func ParseTime(in string) (t SQLTime, ok bool) {
	if len(in) == 0 {
		return SQLTime{}, false
	}
	var neg bool
	if in[0] == '-' {
		neg = true
		in = in[1:]
	}

	var tp timeparts
	in, ok = parsetimeAny(&tp, in)
	if !ok || len(in) > 0 {
		return SQLTime{}, false
	}

	year, month, day := time.Now().Date()

	t.Parts.Hours = 24*tp.day + tp.hour
	t.Parts.Mins = tp.min
	t.Parts.Secs = tp.sec
	t.Parts.Nsec = tp.nsec

	if neg {
		duration := time.Duration(tp.day)*24*time.Hour +
			time.Duration(tp.hour)*time.Hour +
			time.Duration(tp.min)*time.Minute +
			time.Duration(tp.sec)*time.Second +
			time.Duration(tp.nsec)*time.Nanosecond

		// If we have a negative time, we start with the start of today
		// and substract the total duration of the parsed time.
		t.Time = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
		t.Time = t.Time.Add(-duration)
		t.Parts.Hours = -t.Parts.Hours
	} else {
		t.Time = time.Date(0, 1, 1, tp.hour, tp.min, tp.sec, tp.nsec, time.UTC)
		t.Time = t.Time.AddDate(year, int(month-1), day-1+tp.day)
	}
	return t, true
}

func ParseDate(s string) (time.Time, bool) {
	if len(s) >= 8 {
		if t, ok := Date_YYYY_M_D.Parse(s); ok {
			return t, true
		}
	}
	if len(s) >= 6 {
		if t, ok := Date_YY_M_D.Parse(s); ok {
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
	if t, ok := DateTime_YYYY_M_D_h_m_s.Parse(s); ok {
		return t, true
	}
	if t, ok := DateTime_YY_M_D_h_m_s.Parse(s); ok {
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
