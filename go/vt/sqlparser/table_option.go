/*
Copyright 2026 The Vitess Authors.

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

package sqlparser

import (
	"math"
	"strconv"
)

// MySQL reads the number of a numeric table option the way its my_strtoll10
// does: it takes the digits up to the first character that is not a digit, so
// `AUTO_INCREMENT = 1.9` and `AUTO_INCREMENT = 1e5` both mean 1. The parser
// reads the number the same way, and writes out the integer that MySQL uses,
// so that MySQL reads the formatted option as the same number.

// mysqlUnsignedOption returns the integer that MySQL reads from the number of a
// numeric table option. A hexadecimal number is allowed only where MySQL takes
// one. It reports false for a number that does not fit in 64 bits.
func mysqlUnsignedOption(token int, text string) (uint64, bool) {
	if token == HEXNUM {
		value, err := strconv.ParseUint(text[2:], 16, 64)
		return value, err == nil
	}
	end := 0
	for end < len(text) && isDigit(uint16(text[end])) {
		end++
	}
	if end == 0 {
		// A decimal such as `.5` has no digits before its point.
		return 0, true
	}
	value, err := strconv.ParseUint(text[:end], 10, 64)
	return value, err == nil
}

// The ranges below are those of the table options that MySQL stores in fewer
// than eight bytes.
const (
	maxAvgRowLength   = math.MaxUint32
	maxKeyBlockSize   = math.MaxUint16
	maxStatsSamplePgs = math.MaxUint16
)

// tableOptionNumberOrError returns the number of a numeric table option as an
// integer literal, or reports an error to the lexer and returns nil.
func tableOptionNumberOrError(yylex yyLexer, token int, text string) *Literal {
	value, ok := mysqlUnsignedOption(token, text)
	if !ok {
		yylex.Error("table option value is out of range")
		return nil
	}
	return NewIntLiteral(strconv.FormatUint(value, 10))
}

// checkTableOptionMaximum returns the number of a numeric table option, or
// reports an error to the lexer and returns nil when it is larger than maximum.
func checkTableOptionMaximum(yylex yyLexer, number *Literal, maximum uint64) *Literal {
	value, err := strconv.ParseUint(number.Val, 10, 64)
	if err != nil {
		yylex.Error("table option value is out of range")
		return nil
	}
	if value > maximum {
		yylex.Error("table option value is out of range")
		return nil
	}
	return number
}

// tableOptionSize returns the number of bytes that a size such as `4M` stands
// for, or an error message when MySQL rejects it. MySQL reads the digits and
// one suffix, K, M or G, and limits the digits to 31 bits.
func tableOptionSize(text string) (*Literal, string) {
	if text == "" {
		return nil, "invalid size number"
	}
	// As in MySQL, the digits can be missing, and then the size is 0.
	digits, suffix := text[:len(text)-1], text[len(text)-1]
	for i := range len(digits) {
		if !isDigit(uint16(digits[i])) {
			return nil, "invalid size number"
		}
	}
	var shift uint
	switch suffix {
	case 'k', 'K':
		shift = 10
	case 'm', 'M':
		shift = 20
	case 'g', 'G':
		shift = 30
	default:
		return nil, "invalid size number"
	}
	var prefix uint64
	var err error
	if digits != "" {
		prefix, err = strconv.ParseUint(digits, 10, 64)
	}
	if err != nil {
		return nil, "size number overflow"
	}
	if prefix > math.MaxInt32 {
		return nil, "size number overflow"
	}
	return NewIntLiteral(strconv.FormatUint(prefix<<shift, 10)), ""
}
