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

	"github.com/tidwall/gjson"

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
	if len(number) > 0 && (number[0] == '-' || number[0] >= '0' && number[0] <= '9') && gjson.ValidBytes(number) {
		return append(dst, number...)
	}
	var ok bool
	dst, ok = appendNormalisedJSONNumber(dst, number)
	if ok {
		return dst
	}
	return appendJSONQuote(dst, hack.String(value))
}

func appendNormalisedJSONNumber(dst, number []byte) ([]byte, bool) {
	start := len(dst)
	if len(number) > 0 {
		switch number[0] {
		case '-':
			dst = append(dst, '-')
			number = number[1:]
		case '+':
			number = number[1:]
		}
	}
	if len(number) == 0 {
		return dst[:start], false
	}
	beforeZeros := len(number)
	number = bytes.TrimLeft(number, "0")
	if len(number) == 0 {
		return append(dst, '0'), true
	}
	switch number[0] {
	case '.':
		// A leading decimal point needs an original digit, not just an inserted zero.
		if beforeZeros == len(number) && (len(number) == 1 || number[1] < '0' || number[1] > '9') {
			return dst[:start], false
		}
		dst = append(dst, '0')
	case 'e', 'E':
		if beforeZeros == len(number) {
			return dst[:start], false
		}
		dst = append(dst, '0')
	default:
		if number[0] < '1' || number[0] > '9' {
			return dst[:start], false
		}
	}
	point := bytes.IndexByte(number, '.')
	if point >= 0 && (point+1 == len(number) || number[point+1] == 'e' || number[point+1] == 'E') {
		dst = append(dst, number[:point+1]...)
		dst = append(dst, '0')
		dst = append(dst, number[point+1:]...)
	} else {
		dst = append(dst, number...)
	}
	if gjson.ValidBytes(dst[start:]) {
		return dst, true
	}
	return dst[:start], false
}

func appendQuote(dst []byte, s string, json bool) []byte {
	return appendQuoteDispatch(dst, s, json, strconv.AppendQuote, appendJSONQuote)
}

// Passing the quote functions keeps the dispatch and stdlib text path inlineable.
func appendQuoteDispatch(dst []byte, s string, json bool, quoteText, quoteJSON func([]byte, string) []byte) []byte {
	if json {
		return quoteJSON(dst, s)
	}
	return quoteText(dst, s)
}

// appendJSONQuote preserves the log format while using the JSON string encoder.
func appendJSONQuote(dst []byte, s string) []byte {
	if strings.Contains(s, "\u2028") || strings.Contains(s, "\u2029") {
		return appendJSONQuoteWithOptions(dst, s)
	}
	// AppendQuote replaces invalid UTF-8 even when it returns an error.
	dst, _ = jsontext.AppendQuote(dst, s)
	return dst
}

type jsonQuoteEncoder struct {
	buf bytes.Buffer
	enc jsontext.Encoder
}

var jsonQuoteEncoderPool = sync.Pool{New: func() any {
	return &jsonQuoteEncoder{}
}}

func appendJSONQuoteWithOptions(dst []byte, s string) []byte {
	// Handle separator escaping and UTF-8 replacement in one pass without an error allocation.
	quote := jsonQuoteEncoderPool.Get().(*jsonQuoteEncoder)
	quote.buf = *bytes.NewBuffer(dst)
	quote.enc.Reset(&quote.buf, jsontext.AllowInvalidUTF8(true), jsontext.EscapeForJS(true))
	_ = quote.enc.WriteToken(jsontext.String(s))
	dst = quote.buf.Bytes()
	dst = dst[:len(dst)-1] // The streaming encoder appends a record-ending newline.
	// Do not retain the caller's buffer in the pool.
	quote.buf = bytes.Buffer{}
	quote.enc.Reset(&quote.buf)
	jsonQuoteEncoderPool.Put(quote)
	return dst
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
