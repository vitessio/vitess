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

func parsetimeDays(tp *timeparts, in string) (out string, ok bool) {
	var x int
	if x, in, ok = getnum(in, false); ok {
		switch {
		case len(in) == 0:
			tp.sec = x
			if tp.sec > 59 {
				return "", false
			}
			return "", true
		case in[0] == ':':
			tp.day = tp.day + x/24
			tp.hour = x % 24
			return parsetimeMinutes(tp, in[1:])
		case isDigit(in, 0):
			return parsetimeNoDelimiters(tp, in, x)
		case isSpace(in[0]):
			tp.day = x
			for len(in) > 0 && isSpace(in[0]) {
				in = in[1:]
			}
			if tp.day > 34 {
				return "", false
			}
			return parsetimeHours(tp, in)
		}
	}
	return "", false
}

func parsetimeNoDelimiters(tp *timeparts, in string, x int) (string, bool) {
	var ok bool
	var decimal string

	if dot := strings.IndexByte(in, '.'); dot >= 0 {
		decimal = in[dot:]
		in = in[:dot]
	}

	switch len(in) {
	case 4:
		tp.day = tp.day + x/24
		tp.hour = x % 24
		tp.min, in, ok = getnum(in, true)
		if !ok || tp.min > 59 {
			return "", false
		}
		tp.sec, in, ok = getnum(in, true)
		if !ok || tp.sec > 59 {
			return "", false
		}
	case 2:
		tp.min = x
		if tp.min > 59 {
			return "", false
		}
		tp.sec, in, ok = getnum(in, true)
		if !ok || tp.sec > 59 {
			return "", false
		}
	default:
		return "", false
	}
	if decimal != "" {
		if len(decimal) > 2 && isDigit(decimal, 1) {
			n := 2
			for ; n < len(decimal) && isDigit(decimal, n); n++ {
			}
			tp.nsec, ok = parseNanoseconds(decimal, n)
			return "", ok && len(decimal) == n
		}
		return "", false
	}
	return "", true
}

func parsetimeHours(tp *timeparts, in string) (out string, ok bool) {
	if tp.hour, in, ok = getnum(in, false); ok {
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

func ParseTime(in string, normalize *Strftime) (t time.Time, normalized []byte, ok bool) {
	if len(in) == 0 {
		return time.Time{}, nil, false
	}
	var neg bool
	if in[0] == '-' {
		neg = true
		in = in[1:]
	}

	var tp timeparts
	_, ok = parsetimeDays(&tp, in)
	if !ok {
		return time.Time{}, nil, false
	}

	year, month, day := time.Now().Date()

	if neg {
		duration := time.Duration(tp.day)*24*time.Hour +
			time.Duration(tp.hour)*time.Hour +
			time.Duration(tp.min)*time.Minute +
			time.Duration(tp.sec)*time.Second +
			time.Duration(tp.nsec)*time.Nanosecond

		// If we have a negative time, we start with the start of today
		// and substract the total duration of the parsed time.
		t = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
		t = t.Add(-duration)
	} else {
		t = time.Date(0, 1, 1, tp.hour, tp.min, tp.sec, tp.nsec, time.UTC)
		t = t.AddDate(year, int(month-1), day-1+tp.day)
	}

	if normalize != nil {
		if neg {
			normalized = append(normalized, '-')
		}
		normalized = normalize.format(&timeparts{
			hour: 24*tp.day + tp.hour,
			min:  tp.min,
			sec:  tp.sec,
			nsec: tp.nsec,
			prec: 9,
		}, normalized, t)
	}
	return t, normalized, true
}
