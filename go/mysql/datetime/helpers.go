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

package datetime

import (
	"time"
)

// appendInt appends the decimal form of x to b and returns the result.
// If the decimal form (excluding sign) is shorter than width, the result is padded with leading 0's.
// Duplicates functionality in strconv, but avoids dependency.
func appendInt(b []byte, x int, width int) []byte {
	u := uint(x)
	if x < 0 {
		b = append(b, '-')
		u = uint(-x)
	}

	// 2-digit and 4-digit fields are the most common in time formats.
	utod := func(u uint) byte { return '0' + byte(u) }
	switch {
	case width == 2 && u < 1e2:
		return append(b, utod(u/1e1), utod(u%1e1))
	case width == 4 && u < 1e4:
		return append(b, utod(u/1e3), utod(u/1e2%1e1), utod(u/1e1%1e1), utod(u%1e1))
	}

	// Compute the number of decimal digits.
	var n int
	if u == 0 {
		n = 1
	}
	for u2 := u; u2 > 0; u2 /= 10 {
		n++
	}

	// Add 0-padding.
	for pad := width - n; pad > 0; pad-- {
		b = append(b, '0')
	}

	// Ensure capacity.
	if len(b)+n <= cap(b) {
		b = b[:len(b)+n]
	} else {
		b = append(b, make([]byte, n)...)
	}

	// Assemble decimal in reverse order.
	i := len(b) - 1
	for u >= 10 && i > 0 {
		q := u / 10
		b[i] = utod(u - q*10)
		u = q
		i--
	}
	b[i] = utod(u)
	return b
}

// match reports whether s1 and s2 match ignoring case.
// It is assumed s1 and s2 are the same length.
func match(s1, s2 string) bool {
	for i := 0; i < len(s1); i++ {
		c1 := s1[i]
		c2 := s2[i]
		if c1 != c2 {
			// Switch to lower-case; 'a'-'A' is known to be a single bit.
			c1 |= 'a' - 'A'
			c2 |= 'a' - 'A'
			if c1 != c2 || c1 < 'a' || c1 > 'z' {
				return false
			}
		}
	}
	return true
}

func lookup(tab []string, val string) (int, string, bool) {
	for i, v := range tab {
		if len(val) >= len(v) && match(val[0:len(v)], v) {
			return i, val[len(v):], true
		}
	}
	return -1, val, false
}

func leadingInt[bytes []byte | string](s bytes) (x uint64, rem bytes, ok bool) {
	i := 0
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		if x > 1<<63/10 {
			// overflow
			return 0, rem, false
		}
		x = x*10 + uint64(c) - '0'
		if x > 1<<63 {
			// overflow
			return 0, rem, false
		}
	}
	return x, s[i:], true
}

func atoi[bytes []byte | string](s bytes) (x int, ok bool) {
	neg := false
	if len(s) > 0 && (s[0] == '-' || s[0] == '+') {
		neg = s[0] == '-'
		s = s[1:]
	}
	q, rem, ok := leadingInt(s)
	x = int(q)
	if !ok || len(rem) > 0 {
		return 0, false
	}
	if neg {
		x = -x
	}
	return x, true
}

// isDigit reports whether s[i] is in range and is a decimal digit.
func isDigit[bytes []byte | string](s bytes, i int) bool {
	if len(s) <= i {
		return false
	}
	c := s[i]
	return '0' <= c && c <= '9'
}

// isNumber reports whether s is an integer or decimal number.
// Returns the length of the integral part of the number if it's
// a valid number.
// It accepts trailing garbage but only after a dot.
func isNumber[bytes []byte | string](s bytes) (int, bool) {
	var dot bool
	pos := -1
	for i := 0; i < len(s); i++ {
		if !isDigit(s, i) {
			if dot {
				return i, true
			}
			if s[i] == '.' {
				dot = true
				continue
			}
			return 0, false
		}
		if !dot {
			pos = i + 1
		}
	}
	return pos, true
}

// getnum parses s[0:1] or s[0:2] (fixed forces s[0:2])
// as a decimal integer and returns the integer and the
// remainder of the string.
func getnum(s string, fixed bool) (int, string, bool) {
	if !isDigit(s, 0) {
		return 0, s, false
	}
	if !isDigit(s, 1) {
		if fixed {
			return 0, s, false
		}
		return int(s[0] - '0'), s[1:], true
	}
	return int(s[0]-'0')*10 + int(s[1]-'0'), s[2:], true
}

func getnuml(s string, l int) (int, string, bool) {
	var res int
	for i := 0; i < l; i++ {
		if !isDigit(s, i) {
			return 0, s, false
		}
		res = res*10 + int(s[i]-'0')
	}
	return res, s[l:], true
}

func getnumn(s string) (int, string, bool) {
	if !isDigit(s, 0) {
		return 0, s, false
	}

	var n int
	for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
		n = n*10 + int(s[0]-'0')
		s = s[1:]
	}
	return n, s, true
}

// daysBefore[m] counts the number of days in a non-leap year
// before month m begins. There is an entry for m=12, counting
// the number of days before January of next year (365).
var daysBefore = [...]int32{
	0,
	31,
	31 + 28,
	31 + 28 + 31,
	31 + 28 + 31 + 30,
	31 + 28 + 31 + 30 + 31,
	31 + 28 + 31 + 30 + 31 + 30,
	31 + 28 + 31 + 30 + 31 + 30 + 31,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31,
}

func daysIn(m time.Month, year int) int {
	if m == time.February && isLeap(year) {
		return 29
	}
	return int(daysBefore[m] - daysBefore[m-1])
}

func isLeap(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

func parseNanoseconds[bytes []byte | string](value bytes, nbytes int) (ns int, l int, ok bool) {
	if value[0] != '.' {
		return 0, 0, false
	}
	if nbytes > 10 {
		value = value[:10]
		nbytes = 10
	}
	if ns, ok = atoi(value[1:nbytes]); !ok {
		return 0, 0, false
	}
	if ns < 0 {
		return 0, 0, false
	}
	// We need nanoseconds, which means scaling by the number
	// of missing digits in the format, maximum length 10.
	scaleDigits := 10 - nbytes
	for i := 0; i < scaleDigits; i++ {
		ns *= 10
	}

	l = nbytes - 1
	if l > 6 {
		l = 6
	}

	return
}
