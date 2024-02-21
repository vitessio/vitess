/*
Copyright 2018 Aliaksandr Valialkin
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

package fastparse

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"vitess.io/vitess/go/hack"
)

func ParseUint64(s string, base int) (uint64, error) {
	return parseUint64(s, base, false)
}

func ParseUint64WithNeg(s string, base int) (uint64, error) {
	return parseUint64(s, base, true)
}

// ParseUint64 parses uint64 from s.
//
// It is equivalent to strconv.ParseUint(s, base, 64) in case it succeeds,
// but on error it will return the best effort value of what it has parsed so far.
func parseUint64(s string, base int, allowNeg bool) (uint64, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("cannot parse uint64 from empty string")
	}
	if base < 2 || base > 36 {
		return 0, fmt.Errorf("invalid base %d; must be in [2, 36]", base)
	}
	i := uint(0)
	for i < uint(len(s)) {
		if !isSpace(s[i]) {
			break
		}
		i++
	}

	if i >= uint(len(s)) {
		return 0, fmt.Errorf("cannot parse uint64 from %q", s)
	}
	// For some reason, MySQL parses things as uint64 even with
	// a negative sign and then turns it into the 2s complement value.
	minus := s[i] == '-'
	if minus {
		if !allowNeg {
			return 0, fmt.Errorf("cannot parse uint64 from %q", s)
		}
		i++
		if i >= uint(len(s)) {
			return 0, fmt.Errorf("cannot parse uint64 from %q", s)
		}
	}

	d := uint64(0)
	j := i
next:
	for i < uint(len(s)) {
		var b byte
		switch {
		case s[i] >= '0' && s[i] <= '9':
			b = s[i] - '0'
		case s[i] >= 'a' && s[i] <= 'z':
			b = s[i] - 'a' + 10
		case s[i] >= 'A' && s[i] <= 'Z':
			b = s[i] - 'A' + 10
		default:
			break next
		}

		if b >= byte(base) {
			break next
		}

		var cutoff uint64
		switch base {
		case 10:
			cutoff = math.MaxUint64/10 + 1
		case 16:
			cutoff = math.MaxUint64/16 + 1
		default:
			cutoff = math.MaxUint64/uint64(base) + 1
		}
		if d >= cutoff {
			if minus {
				return 0, fmt.Errorf("cannot parse uint64 from %q: %w", s, ErrOverflow)
			}
			return math.MaxUint64, fmt.Errorf("cannot parse uint64 from %q: %w", s, ErrOverflow)
		}
		v := d*uint64(base) + uint64(b)
		if v < d {
			if minus {
				return 0, fmt.Errorf("cannot parse uint64 from %q: %w", s, ErrOverflow)
			}
			return math.MaxUint64, fmt.Errorf("cannot parse uint64 from %q: %w", s, ErrOverflow)
		}
		d = v
		i++
	}
	if i <= j {
		return uValue(d, minus), fmt.Errorf("cannot parse uint64 from %q", s)
	}

	for i < uint(len(s)) {
		if !isSpace(s[i]) {
			break
		}
		i++
	}

	if i < uint(len(s)) {
		// Unparsed tail left.
		return uValue(d, minus), fmt.Errorf("unparsed tail left after parsing uint64 from %q: %q", s, s[i:])
	}
	return uValue(d, minus), nil
}

var ErrOverflow = errors.New("overflow")

// ParseInt64 parses int64 number s.
//
// It is equivalent to strconv.ParseInt(s, base, 64) in case it succeeds,
// but on error it will return the best effort value of what it has parsed so far.
func ParseInt64(s string, base int) (int64, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("cannot parse int64 from empty string")
	}
	if base < 2 || base > 36 {
		return 0, fmt.Errorf("invalid base %d; must be in [2, 36]", base)
	}
	i := uint(0)
	for i < uint(len(s)) {
		if !isSpace(s[i]) {
			break
		}
		i++
	}

	if i >= uint(len(s)) {
		return 0, fmt.Errorf("cannot parse int64 from %q", s)
	}
	minus := s[i] == '-'
	if minus {
		i++
		if i >= uint(len(s)) {
			return 0, fmt.Errorf("cannot parse int64 from %q", s)
		}
	}

	d := uint64(0)
	j := i
next:
	for i < uint(len(s)) {
		var b byte
		switch {
		case s[i] >= '0' && s[i] <= '9':
			b = s[i] - '0'
		case s[i] >= 'a' && s[i] <= 'z':
			b = s[i] - 'a' + 10
		case s[i] >= 'A' && s[i] <= 'Z':
			b = s[i] - 'A' + 10
		default:
			break next
		}

		if b >= byte(base) {
			break next
		}

		var cutoff uint64
		switch base {
		case 10:
			cutoff = math.MaxInt64/10 + 1
		case 16:
			cutoff = math.MaxInt64/16 + 1
		default:
			cutoff = math.MaxInt64/uint64(base) + 1
		}
		if !minus && d >= cutoff {
			return math.MaxInt64, fmt.Errorf("cannot parse int64 from %q: %w", s, ErrOverflow)
		}

		if minus && d > cutoff {
			return math.MinInt64, fmt.Errorf("cannot parse int64 from %q: %w", s, ErrOverflow)
		}

		d = d*uint64(base) + uint64(b)
		i++
	}

	v := int64(d)
	if d > math.MaxInt64 && !minus {
		return math.MaxInt64, fmt.Errorf("cannot parse int64 from %q: %w", s, ErrOverflow)
	} else if d > math.MaxInt64+1 && minus {
		return math.MinInt64, fmt.Errorf("cannot parse int64 from %q: %w", s, ErrOverflow)
	}

	if minus {
		v = -v
		if d == math.MaxInt64+1 {
			v = math.MinInt64
		}
	}

	if i <= j {
		return v, fmt.Errorf("cannot parse int64 from %q", s)
	}

	for i < uint(len(s)) {
		if !isSpace(s[i]) {
			break
		}
		i++
	}

	if i < uint(len(s)) {
		// Unparsed tail left.
		return v, fmt.Errorf("unparsed tail left after parsing int64 from %q: %q", s, s[i:])
	}
	if d == math.MaxInt64+1 && minus {
		v = math.MinInt64
	}

	return v, nil
}

// ParseFloat64 parses floating-point number s.
//
// It is equivalent to strconv.ParseFloat(s, 64) in case it succeeds,
// but on error it will return the best effort value of what it has parsed so far.
func ParseFloat64(s string) (float64, error) {
	if len(s) == 0 {
		return 0.0, fmt.Errorf("cannot parse float64 from empty string")
	}
	i := uint(0)
	for i < uint(len(s)) {
		if !isSpace(s[i]) {
			break
		}
		i++
	}
	ws := i

	// We only care to parse as many of the initial float characters of the
	// string as possible. This functionality is implemented in the `strconv` package
	// of the standard library, but not exposed, so we hook into it.
	val, l, err := hack.Atof64(s[ws:])
	for l < len(s[ws:]) {
		if !isSpace(s[ws+uint(l)]) {
			break
		}
		l++
	}

	if l < len(s[ws:]) {
		return val, fmt.Errorf("unparsed tail left after parsing float64 from %q: %q", s, s[ws+uint(l):])
	}
	if errors.Is(err, strconv.ErrRange) {
		if val < 0 {
			val = -math.MaxFloat64
		} else {
			val = math.MaxFloat64
		}
	}

	return val, err
}

func isSpace(c byte) bool {
	switch c {
	case ' ', '\t':
		return true
	default:
		return false
	}
}

func uValue(v uint64, neg bool) uint64 {
	if neg {
		return -v
	}
	return v
}
