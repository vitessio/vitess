/*
Copyright 2024 The Vitess Authors.

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

package logstats

import (
	"bytes"
	"encoding/json/jsontext"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type logbv struct {
	Name string
	BVar *querypb.BindVariable
}

// Logger formats logstats using reusable buffers.
// It can output logs as JSON or as plaintext, following the commonly used
// logstats format that is shared between the tablets and the gates.
type Logger struct {
	b     []byte
	bvars []logbv
	n     int
	json  bool
}

func sortBVars(sorted []logbv, bvars map[string]*querypb.BindVariable) []logbv {
	for k, bv := range bvars {
		sorted = append(sorted, logbv{k, bv})
	}
	slices.SortFunc(sorted, func(a, b logbv) int {
		return strings.Compare(a.Name, b.Name)
	})
	return sorted
}

func (log *Logger) appendBVarsJSON(b []byte, bvars map[string]*querypb.BindVariable, full bool) []byte {
	log.bvars = sortBVars(log.bvars[:0], bvars)

	b = append(b, '{')
	for i, bv := range log.bvars {
		if i > 0 {
			b = append(b, ',', ' ')
		}
		b = appendQuote(b, bv.Name, log.json)
		// Generated enum names are ASCII identifiers and need no escaping.
		b = append(b, `: {"type": "`...)
		b = append(b, querypb.Type_name[int32(bv.BVar.Type)]...)
		b = append(b, `", "value": `...)

		if sqltypes.IsIntegral(bv.BVar.Type) {
			b = appendJSONInteger(b, bv.BVar.Value, sqltypes.IsUnsigned(bv.BVar.Type))
		} else if sqltypes.IsFloat(bv.BVar.Type) {
			b = appendJSONNumber(b, bv.BVar.Value)
		} else if bv.BVar.Type == sqltypes.Tuple {
			b = append(b, '"')
			b = strconv.AppendInt(b, int64(len(bv.BVar.Values)), 10)
			b = append(b, ` items"`...)
		} else {
			if full {
				b = appendQuote(b, hack.String(bv.BVar.Value), log.json)
			} else {
				b = append(b, '"')
				b = strconv.AppendInt(b, int64(len(bv.BVar.Value)), 10)
				b = append(b, ` bytes"`...)
			}
		}
		b = append(b, '}')
	}
	return append(b, '}')
}

func (log *Logger) Init(json bool) {
	log.n = 0
	log.json = json
	if log.json {
		log.b = append(log.b, '{')
	}
}

func (log *Logger) Redacted() {
	log.String("[REDACTED]")
}

func (log *Logger) Key(key string) {
	if log.json {
		if log.n > 0 {
			log.b = append(log.b, ',', ' ')
		}
		log.b = append(log.b, '"')
		log.b = append(log.b, key...)
		log.b = append(log.b, '"', ':', ' ')
	} else {
		if log.n > 0 {
			log.b = append(log.b, '\t')
		}
	}
	log.n++
}

func (log *Logger) StringUnquoted(value string) {
	if log.json {
		log.b = appendJSONQuote(log.b, value)
	} else {
		log.b = append(log.b, value...)
	}
}

func (log *Logger) TabTerminated() {
	if !log.json {
		log.b = append(log.b, '\t')
	}
}

func (log *Logger) String(value string) {
	log.b = appendQuote(log.b, value, log.json)
}

func (log *Logger) StringSingleQuoted(value string) {
	if log.json {
		log.b = appendJSONQuote(log.b, value)
	} else {
		log.b = append(log.b, '\'')
		log.b = append(log.b, value...)
		log.b = append(log.b, '\'')
	}
}

func (log *Logger) Time(t time.Time) {
	const timeFormat = "2006-01-02 15:04:05.000000"
	if log.json {
		log.b = append(log.b, '"')
		log.b = t.AppendFormat(log.b, timeFormat)
		log.b = append(log.b, '"')
	} else {
		log.b = t.AppendFormat(log.b, timeFormat)
	}
}

func (log *Logger) Duration(t time.Duration) {
	log.b = strconv.AppendFloat(log.b, t.Seconds(), 'f', 6, 64)
}

func (log *Logger) BindVariables(bvars map[string]*querypb.BindVariable, full bool) {
	// Text logs retain Go string escaping inside the JSON-shaped object for
	// compatibility, so their bind-variable field is not always valid JSON.
	log.b = log.appendBVarsJSON(log.b, bvars, full)
}

func (log *Logger) Int(i int64) {
	log.b = strconv.AppendInt(log.b, i, 10)
}

func (log *Logger) Uint(u uint64) {
	log.b = strconv.AppendUint(log.b, u, 10)
}

func (log *Logger) Bool(b bool) {
	log.b = strconv.AppendBool(log.b, b)
}

func (log *Logger) Strings(strs []string) {
	log.b = append(log.b, '[')
	for i, t := range strs {
		if i > 0 {
			log.b = append(log.b, ',')
		}
		log.b = appendQuote(log.b, t, log.json)
	}
	log.b = append(log.b, ']')
}

func (log *Logger) Flush(w io.Writer) (err error) {
	if log.json {
		log.b = append(log.b, '}')
	}
	log.b = append(log.b, '\n')
	_, err = w.Write(log.b)

	clear(log.bvars)
	log.bvars = log.bvars[:0]
	log.b = log.b[:0]
	log.n = 0

	loggerPool.Put(log)
	return err
}

func appendJSONInteger(dst, value []byte, unsigned bool) []byte {
	// Normalise decimal digits without narrowing the value to a machine integer.
	number := value
	if len(number) > 0 && (number[0] <= ' ' || number[len(number)-1] <= ' ') {
		number = bytes.Trim(number, " \t\r\n")
	}
	negative := len(number) > 0 && number[0] == '-'
	if len(number) > 0 && (negative || number[0] == '+') {
		number = number[1:]
	}
	if len(number) == 0 || unsigned && negative {
		return appendJSONQuote(dst, hack.String(value))
	}
	for len(number) > 1 && number[0] == '0' {
		number = number[1:]
	}
	for _, digit := range number {
		if digit < '0' || digit > '9' {
			return appendJSONQuote(dst, hack.String(value))
		}
	}
	if negative {
		dst = append(dst, '-')
	}
	return append(dst, number...)
}

func appendJSONNumber(dst, value []byte) []byte {
	// Preserve canonical numbers without parsing or rounding their value.
	number := value
	if len(number) > 0 && (number[0] <= ' ' || number[len(number)-1] <= ' ') {
		number = bytes.Trim(number, " \t\r\n")
	}
	if !sqltypes.IsNumericLiteral(number, true) {
		return appendJSONQuote(dst, hack.String(value))
	}
	switch number[0] {
	case '-':
		dst = append(dst, '-')
		number = number[1:]
	case '+':
		number = number[1:]
	}
	// JSON allows a leading zero only before a point, an exponent or the end.
	for len(number) > 1 && number[0] == '0' && number[1] >= '0' && number[1] <= '9' {
		number = number[1:]
	}
	if number[0] == '.' {
		dst = append(dst, '0')
	}
	// JSON also needs a digit after the point.
	if point := bytes.IndexByte(number, '.'); point >= 0 && (point+1 == len(number) || number[point+1] == 'e' || number[point+1] == 'E') {
		dst = append(dst, number[:point+1]...)
		dst = append(dst, '0')
		return append(dst, number[point+1:]...)
	}
	return append(dst, number...)
}

func appendQuote(dst []byte, s string, json bool) []byte {
	if json {
		return appendJSONQuote(dst, s)
	}
	return strconv.AppendQuote(dst, s)
}

// appendJSONQuote preserves the log format while using the JSON string encoder.
func appendJSONQuote(dst []byte, s string) []byte {
	start := len(dst)
	// AppendQuote replaces invalid UTF-8 even when it returns an error.
	dst, _ = jsontext.AppendQuote(dst, s)
	// U+2028 and U+2029 are valid in JSON strings, but pre-ES2019 JavaScript and
	// some line splitters treat them as line breaks, so keep them escaped.
	first := nextLineSeparator(dst[start:])
	if first < 0 {
		return dst
	}
	first += start
	n := 1
	for q := dst[first+3:]; ; n++ {
		i := nextLineSeparator(q)
		if i < 0 {
			break
		}
		q = q[i+3:]
	}
	// Shift everything from the first separator right by the 3 bytes each escape
	// adds, then copy it back while escaping, so every byte moves at most twice.
	end := len(dst)
	dst = slices.Grow(dst, 3*n)[:end+3*n]
	copy(dst[first+3*n:], dst[first:end])
	w, r := first, first+3*n
	for ; n > 0; n-- {
		i := nextLineSeparator(dst[r:])
		w += copy(dst[w:], dst[r:r+i])
		// Read the separator before the escape overwrites it.
		last := byte('8')
		if dst[r+i+2] == 0xa9 {
			last = '9'
		}
		w += copy(dst[w:], `\u202`)
		dst[w] = last
		w, r = w+1, r+i+3
	}
	// Once every escape is written, w has caught up with r, so the rest is in place.
	return dst
}

// nextLineSeparator returns the index of the first U+2028 or U+2029 in b, or -1.
func nextLineSeparator(b []byte) int {
	for off := 0; ; off++ {
		i := bytes.IndexByte(b[off:], 0xe2)
		if i < 0 || off+i+2 >= len(b) {
			return -1
		}
		off += i
		if b[off+1] == 0x80 && (b[off+2] == 0xa8 || b[off+2] == 0xa9) {
			return off
		}
	}
}

var loggerPool = sync.Pool{New: func() any {
	return &Logger{}
}}

// NewLogger returns a new Logger instance to perform logstats logging.
// The logger must be initialized with (*Logger).Init before usage and
// flushed with (*Logger).Flush once all the key-values have been written
// to it.
func NewLogger() *Logger {
	return loggerPool.Get().(*Logger)
}
