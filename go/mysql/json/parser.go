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

package json

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/mysql/fastparse"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/format"
)

// Parser parses JSON.
//
// Parser may be re-used for subsequent parsing.
//
// Parser cannot be used from concurrent goroutines.
// Use per-goroutine parsers or ParserPool instead.
type Parser struct {
	// b contains working copy of the string to be parsed.
	b []byte

	// c is a cache for json values.
	c cache
}

func startEndString(s string) string {
	const maxStartEndStringLen = 80

	if len(s) <= maxStartEndStringLen {
		return s
	}
	start := s[:40]
	end := s[len(s)-40:]
	return start + "..." + end
}

// Parse parses s containing JSON.
//
// The returned value is valid until the next call to Parse*.
//
// Use Scanner if a stream of JSON values must be parsed.
func (p *Parser) Parse(s string) (*Value, error) {
	s = skipWS(s)
	p.b = append(p.b[:0], s...)
	p.c.reset()

	v, tail, err := parseValue(hack.String(p.b), &p.c, 0)
	if err != nil {
		return nil, fmt.Errorf("cannot parse JSON: %s; unparsed tail: %q", err, startEndString(tail))
	}
	tail = skipWS(tail)
	if len(tail) > 0 {
		return nil, fmt.Errorf("unexpected tail: %q", startEndString(tail))
	}
	return v, nil
}

// ParseBytes parses b containing JSON.
//
// The returned Value is valid until the next call to Parse*.
//
// Use Scanner if a stream of JSON values must be parsed.
func (p *Parser) ParseBytes(b []byte) (*Value, error) {
	return p.Parse(hack.String(b))
}

type cache struct {
	vs []Value
}

func (c *cache) reset() {
	c.vs = c.vs[:0]
}

func (c *cache) getValue() *Value {
	if cap(c.vs) > len(c.vs) {
		c.vs = c.vs[:len(c.vs)+1]
	} else {
		c.vs = append(c.vs, Value{})
	}
	// Do not reset the value, since the caller must properly init it.
	return &c.vs[len(c.vs)-1]
}

func skipWS(s string) string {
	if len(s) == 0 || s[0] > 0x20 {
		// Fast path.
		return s
	}
	return skipWSSlow(s)
}

func skipWSSlow(s string) string {
	if len(s) == 0 || s[0] != 0x20 && s[0] != 0x0A && s[0] != 0x09 && s[0] != 0x0D {
		return s
	}
	for i := 1; i < len(s); i++ {
		if s[i] != 0x20 && s[i] != 0x0A && s[i] != 0x09 && s[i] != 0x0D {
			return s[i:]
		}
	}
	return ""
}

type kv struct {
	k string
	v *Value
}

// MaxDepth is the maximum depth for nested JSON.
const MaxDepth = 300

func parseValue(s string, c *cache, depth int) (*Value, string, error) {
	if len(s) == 0 {
		return nil, s, fmt.Errorf("cannot parse empty string")
	}
	depth++
	if depth > MaxDepth {
		return nil, s, fmt.Errorf("too big depth for the nested JSON; it exceeds %d", MaxDepth)
	}

	if s[0] == '{' {
		v, tail, err := parseObject(s[1:], c, depth)
		if err != nil {
			return nil, tail, fmt.Errorf("cannot parse object: %s", err)
		}
		return v, tail, nil
	}
	if s[0] == '[' {
		v, tail, err := parseArray(s[1:], c, depth)
		if err != nil {
			return nil, tail, fmt.Errorf("cannot parse array: %s", err)
		}
		return v, tail, nil
	}
	if s[0] == '"' {
		ss, tail, err := parseRawString(s[1:])
		if err != nil {
			return nil, tail, fmt.Errorf("cannot parse string: %s", err)
		}
		v := c.getValue()
		v.t = typeRawString
		v.s = ss
		return v, tail, nil
	}
	if s[0] == 't' {
		if len(s) < len("true") || s[:len("true")] != "true" {
			return nil, s, fmt.Errorf("unexpected value found: %q", s)
		}
		return ValueTrue, s[len("true"):], nil
	}
	if s[0] == 'f' {
		if len(s) < len("false") || s[:len("false")] != "false" {
			return nil, s, fmt.Errorf("unexpected value found: %q", s)
		}
		return ValueFalse, s[len("false"):], nil
	}
	if s[0] == 'n' {
		if len(s) < len("null") || s[:len("null")] != "null" {
			// Try parsing NaN
			if len(s) >= 3 && strings.EqualFold(s[:3], "nan") {
				v := c.getValue()
				v.t = TypeNumber
				v.s = s[:3]
				return v, s[3:], nil
			}
			return nil, s, fmt.Errorf("unexpected value found: %q", s)
		}
		return ValueNull, s[len("null"):], nil
	}

	flen, ok := readFloat(s)
	if !ok {
		return nil, s[flen:], fmt.Errorf("invalid number in JSON string: %q", s)
	}

	v := c.getValue()
	v.t = TypeNumber
	v.s = s[:flen]
	v.n = numberTypeRaw
	return v, s[flen:], nil
}

func parseArray(s string, c *cache, depth int) (*Value, string, error) {
	s = skipWS(s)
	if len(s) == 0 {
		return nil, s, fmt.Errorf("missing ']'")
	}

	if s[0] == ']' {
		v := c.getValue()
		v.t = TypeArray
		v.a = v.a[:0]
		return v, s[1:], nil
	}

	a := c.getValue()
	a.t = TypeArray
	a.a = a.a[:0]
	for {
		var v *Value
		var err error

		s = skipWS(s)
		v, s, err = parseValue(s, c, depth)
		if err != nil {
			return nil, s, fmt.Errorf("cannot parse array value: %s", err)
		}
		a.a = append(a.a, v)

		s = skipWS(s)
		if len(s) == 0 {
			return nil, s, fmt.Errorf("unexpected end of array")
		}
		if s[0] == ',' {
			s = s[1:]
			continue
		}
		if s[0] == ']' {
			s = s[1:]
			return a, s, nil
		}
		return nil, s, fmt.Errorf("missing ',' after array value")
	}
}

func parseObject(s string, c *cache, depth int) (*Value, string, error) {
	s = skipWS(s)
	if len(s) == 0 {
		return nil, s, fmt.Errorf("missing '}'")
	}

	if s[0] == '}' {
		v := c.getValue()
		v.t = TypeObject
		v.o.reset()
		return v, s[1:], nil
	}

	o := c.getValue()
	o.t = TypeObject
	o.o.reset()
	for {
		var err error
		var unescape bool
		kv := o.o.getKV()

		// Parse key.
		s = skipWS(s)
		if len(s) == 0 || s[0] != '"' {
			return nil, s, fmt.Errorf(`cannot find opening '"" for object key`)
		}
		kv.k, s, unescape, err = parseRawKey(s[1:])
		if err != nil {
			return nil, s, fmt.Errorf("cannot parse object key: %s", err)
		}
		if unescape {
			kv.k = unescapeStringBestEffort(kv.k)
		}
		s = skipWS(s)
		if len(s) == 0 || s[0] != ':' {
			return nil, s, fmt.Errorf("missing ':' after object key")
		}
		s = s[1:]

		// Parse value
		s = skipWS(s)
		kv.v, s, err = parseValue(s, c, depth)
		if err != nil {
			return nil, s, fmt.Errorf("cannot parse object value: %s", err)
		}
		s = skipWS(s)
		if len(s) == 0 {
			return nil, s, fmt.Errorf("unexpected end of object")
		}
		if s[0] == ',' {
			s = s[1:]
			continue
		}
		if s[0] == '}' {
			o.o.sort()
			return o, s[1:], nil
		}
		return nil, s, fmt.Errorf("missing ',' after object value")
	}
}

func escapeString(dst []byte, s string) []byte {
	if !hasSpecialChars(s) {
		// Fast path - nothing to escape.
		dst = append(dst, '"')
		dst = append(dst, s...)
		dst = append(dst, '"')
		return dst
	}

	// Slow path.
	return strconv.AppendQuote(dst, s)
}

func hasSpecialChars(s string) bool {
	if strings.IndexByte(s, '"') >= 0 || strings.IndexByte(s, '\\') >= 0 {
		return true
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 {
			return true
		}
	}
	return false
}

func unescapeStringBestEffort(s string) string {
	n := strings.IndexByte(s, '\\')
	if n < 0 {
		// Fast path - nothing to unescape.
		return s
	}

	// Slow path - unescape string.
	b := hack.StringBytes(s) // It is safe to do, since s points to a byte slice in Parser.b.
	b = b[:n]
	s = s[n+1:]
	for len(s) > 0 {
		ch := s[0]
		s = s[1:]
		switch ch {
		case '"':
			b = append(b, '"')
		case '\\':
			b = append(b, '\\')
		case '/':
			b = append(b, '/')
		case 'b':
			b = append(b, '\b')
		case 'f':
			b = append(b, '\f')
		case 'n':
			b = append(b, '\n')
		case 'r':
			b = append(b, '\r')
		case 't':
			b = append(b, '\t')
		case 'u':
			if len(s) < 4 {
				// Too short escape sequence. Just store it unchanged.
				b = append(b, "\\u"...)
				break
			}
			xs := s[:4]
			x, err := strconv.ParseUint(xs, 16, 16)
			if err != nil {
				// Invalid escape sequence. Just store it unchanged.
				b = append(b, "\\u"...)
				break
			}
			s = s[4:]
			if !utf16.IsSurrogate(rune(x)) {
				b = append(b, string(rune(x))...)
				break
			}

			// Surrogate.
			// See https://en.wikipedia.org/wiki/Universal_Character_Set_characters#Surrogates
			if len(s) < 6 || s[0] != '\\' || s[1] != 'u' {
				b = append(b, "\\u"...)
				b = append(b, xs...)
				break
			}
			x1, err := strconv.ParseUint(s[2:6], 16, 16)
			if err != nil {
				b = append(b, "\\u"...)
				b = append(b, xs...)
				break
			}
			r := utf16.DecodeRune(rune(x), rune(x1))
			b = append(b, string(r)...)
			s = s[6:]
		default:
			// Unknown escape sequence. Just store it unchanged.
			b = append(b, '\\', ch)
		}
		n = strings.IndexByte(s, '\\')
		if n < 0 {
			b = append(b, s...)
			break
		}
		b = append(b, s[:n]...)
		s = s[n+1:]
	}
	return hack.String(b)
}

// parseRawKey is similar to parseRawString, but is optimized
// for small-sized keys without escape sequences.
func parseRawKey(s string) (string, string, bool, error) {
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			// Fast path.
			return s[:i], s[i+1:], false, nil
		}
		if s[i] == '\\' {
			s, t, err := parseRawString(s)
			return s, t, true, err
		}
	}
	return s, "", false, fmt.Errorf(`missing closing '"'`)
}

func parseRawString(s string) (string, string, error) {
	n := strings.IndexByte(s, '"')
	if n < 0 {
		return s, "", fmt.Errorf(`missing closing '"'`)
	}
	if n == 0 || s[n-1] != '\\' {
		// Fast path. No escaped ".
		return s[:n], s[n+1:], nil
	}

	// Slow path - possible escaped " found.
	ss := s
	for {
		i := n - 1
		for i > 0 && s[i-1] == '\\' {
			i--
		}
		if uint(n-i)%2 == 0 {
			return ss[:len(ss)-len(s)+n], s[n+1:], nil
		}
		s = s[n+1:]

		n = strings.IndexByte(s, '"')
		if n < 0 {
			return ss, "", fmt.Errorf(`missing closing '"'`)
		}
		if n == 0 || s[n-1] != '\\' {
			return ss[:len(ss)-len(s)+n], s[n+1:], nil
		}
	}
}

func readFloat(s string) (i int, ok bool) {
	// optional sign
	if i >= len(s) {
		return
	}
	if s[i] == '+' || s[i] == '-' {
		i++
	}

	// digits
	sawdot := false
	sawdigits := false
	nd := 0
loop:
	for ; i < len(s); i++ {
		switch c := s[i]; true {
		case c == '.':
			if sawdot {
				break loop
			}
			sawdot = true
			continue

		case '0' <= c && c <= '9':
			sawdigits = true
			if c == '0' && nd == 0 { // ignore leading zeros
				continue
			}
			nd++
			continue
		}
		break
	}
	if !sawdigits {
		return
	}

	// optional exponent moves decimal point.
	// if we read a very large, very long number,
	// just be sure to move the decimal point by
	// a lot (say, 100000).  it doesn't matter if it's
	// not the exact number.
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i >= len(s) {
			return
		}
		if s[i] == '+' || s[i] == '-' {
			i++
		}
		if i >= len(s) || s[i] < '0' || s[i] > '9' {
			return
		}
		for ; i < len(s) && ('0' <= s[i] && s[i] <= '9'); i++ {
		}
	}
	return i, true
}

// Object represents JSON object.
//
// Object cannot be used from concurrent goroutines.
// Use per-goroutine parsers or ParserPool instead.
type Object struct {
	kvs []kv
}

func (o *Object) reset() {
	o.kvs = o.kvs[:0]
}

func (o *Object) Keys() []string {
	keys := make([]string, 0, len(o.kvs))
	for _, kv := range o.kvs {
		keys = append(keys, kv.k)
	}
	return keys
}

// MarshalTo appends marshaled o to dst and returns the result.
func (o *Object) MarshalTo(dst []byte) []byte {
	dst = append(dst, '{')
	for i, kv := range o.kvs {
		dst = escapeString(dst, kv.k)
		dst = append(dst, ':', ' ')
		dst = kv.v.MarshalTo(dst)
		if i != len(o.kvs)-1 {
			dst = append(dst, ',', ' ')
		}
	}
	dst = append(dst, '}')
	return dst
}

// String returns string representation for the o.
//
// This function is for debugging purposes only. It isn't optimized for speed.
// See MarshalTo instead.
func (o *Object) String() string {
	b := o.MarshalTo(nil)
	// It is safe converting b to string without allocation, since b is no longer
	// reachable after this line.
	return hack.String(b)
}

func (o *Object) getKV() *kv {
	if cap(o.kvs) > len(o.kvs) {
		o.kvs = o.kvs[:len(o.kvs)+1]
	} else {
		o.kvs = append(o.kvs, kv{})
	}
	return &o.kvs[len(o.kvs)-1]
}

func (o *Object) sort() {
	if len(o.kvs) < 2 {
		return
	}

	slices.SortStableFunc(o.kvs, func(a, b kv) int {
		// TODO: switch to cmp.Compare for Go 1.21+.
		//
		// https://pkg.go.dev/cmp@master#Compare.
		switch {
		case a.k < b.k:
			return -1
		case a.k > b.k:
			return 1
		default:
			return 0
		}
	})
	uniq := o.kvs[:1]
	for _, kv := range o.kvs[1:] {
		if uniq[len(uniq)-1].k == kv.k {
			uniq = uniq[:len(uniq)-1]
		}
		uniq = append(uniq, kv)
	}
	o.kvs = uniq
}

// Len returns the number of items in the o.
func (o *Object) Len() int {
	return len(o.kvs)
}

func (o *Object) find(key string) (int, bool) {
	n := len(o.kvs)
	// Define cmp(x[-1], target) < 0 and cmp(x[n], target) >= 0 .
	// Invariant: cmp(x[i - 1], target) < 0, cmp(x[j], target) >= 0.
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		if o.kvs[h].k < key {
			i = h + 1 // preserves cmp(x[i - 1], target) < 0
		} else {
			j = h // preserves cmp(x[j], target) >= 0
		}
	}
	// i == j, cmp(x[i-1], target) < 0, and cmp(x[j], target) (= cmp(x[i], target)) >= 0  =>  answer is i.
	return i, i < n && o.kvs[i].k == key
}

// Get returns the value for the given key in the o.
//
// Returns nil if the value for the given key isn't found.
//
// The returned value is valid until Parse is called on the Parser returned o.
func (o *Object) Get(key string) *Value {
	if i, ok := o.find(key); ok {
		return o.kvs[i].v
	}
	return nil
}

// Visit calls f for each item in the o in the original order
// of the parsed JSON.
//
// f cannot hold key and/or v after returning.
func (o *Object) Visit(f func(key string, v *Value)) {
	if o == nil {
		return
	}
	for _, kv := range o.kvs {
		f(kv.k, kv.v)
	}
}

// Value represents any JSON value.
//
// Call Type in order to determine the actual type of the JSON value.
//
// Value cannot be used from concurrent goroutines.
// Use per-goroutine parsers or ParserPool instead.
type Value struct {
	o Object
	a []*Value
	s string
	t Type
	n NumberType
}

func (v *Value) MarshalDate() string {
	if d, ok := v.Date(); ok {
		return d.ToStdTime(time.Local).Format("2006-01-02")
	}
	return ""
}

func (v *Value) MarshalDateTime() string {
	if dt, ok := v.DateTime(); ok {
		return dt.ToStdTime(time.Local).Format("2006-01-02 15:04:05.000000")
	}
	return ""
}

func (v *Value) MarshalTime() string {
	if t, ok := v.Time(); ok {
		t = t.RoundForJSON()
		diff := t.ToDuration()
		var neg bool
		if diff < 0 {
			diff = -diff
			neg = true
		}

		b := strings.Builder{}
		if neg {
			b.WriteByte('-')
		}

		hours := diff / time.Hour
		diff -= hours * time.Hour
		fmt.Fprintf(&b, "%02d", hours)
		minutes := diff / time.Minute
		fmt.Fprintf(&b, ":%02d", minutes)
		diff -= minutes * time.Minute
		seconds := diff / time.Second
		fmt.Fprintf(&b, ":%02d", seconds)
		diff -= seconds * time.Second
		fmt.Fprintf(&b, ".%06d", diff/1000)
		return b.String()
	}
	return ""
}

func (v *Value) marshalFloat(dst []byte) []byte {
	f, _ := v.Float64()
	buf := format.FormatFloat(f)
	if bytes.IndexByte(buf, '.') == -1 && bytes.IndexByte(buf, 'e') == -1 {
		buf = append(buf, '.', '0')
	}
	return append(dst, buf...)
}

// MarshalTo appends marshaled v to dst and returns the result.
func (v *Value) MarshalTo(dst []byte) []byte {
	switch v.t {
	case typeRawString:
		dst = append(dst, '"')
		dst = append(dst, v.s...)
		dst = append(dst, '"')
		return dst
	case TypeObject:
		return v.o.MarshalTo(dst)
	case TypeArray:
		dst = append(dst, '[')
		for i, vv := range v.a {
			dst = vv.MarshalTo(dst)
			if i != len(v.a)-1 {
				dst = append(dst, ',', ' ')
			}
		}
		dst = append(dst, ']')
		return dst
	case TypeString:
		return escapeString(dst, v.s)
	case TypeDate:
		return escapeString(dst, v.MarshalDate())
	case TypeDateTime:
		return escapeString(dst, v.MarshalDateTime())
	case TypeTime:
		return escapeString(dst, v.MarshalTime())
	case TypeBlob, TypeBit:
		const prefix = "base64:type15:"

		size := 2 + len(prefix) + base64.StdEncoding.EncodedLen(len(v.s))
		dst := make([]byte, size)
		dst[0] = '"'
		copy(dst[1:], prefix)
		base64.StdEncoding.Encode(dst[len(prefix)+1:], []byte(v.s))
		dst[size-1] = '"'
		return dst
	case TypeNumber:
		if v.NumberType() == NumberTypeFloat {
			return v.marshalFloat(dst)
		}
		return append(dst, v.s...)
	case TypeBoolean:
		if v == ValueTrue {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)
	case TypeNull:
		return append(dst, "null"...)
	default:
		panic(fmt.Errorf("BUG: unexpected Value type: %d", v.t))
	}
}

// String returns string representation of the v.
//
// The function is for debugging purposes only. It isn't optimized for speed.
// See MarshalTo instead.
//
// Don't confuse this function with StringBytes, which must be called
// for obtaining the underlying JSON string for the v.
func (v *Value) String() string {
	b := v.MarshalTo(nil)
	// It is safe converting b to string without allocation, since b is no longer
	// reachable after this line.
	return hack.String(b)
}

func (v *Value) ToBoolean() bool {
	switch v.Type() {
	case TypeNumber:
		switch v.NumberType() {
		case NumberTypeSigned:
			i, _ := v.Int64()
			return i != 0
		case NumberTypeUnsigned:
			i, _ := v.Uint64()
			return i != 0
		case NumberTypeFloat:
			f, _ := v.Float64()
			return f != 0.0
		case NumberTypeDecimal:
			d, _ := v.Decimal()
			return !d.IsZero()
		}
	}
	return true
}

// Type represents JSON type.
type Type int32

// See https://dev.mysql.com/doc/refman/8.0/en/json.html#json-comparison for the ordering here
const (
	// TypeNull is JSON null.
	TypeNull Type = iota

	// TypeNumber is JSON number type.
	TypeNumber

	// TypeString is JSON string type.
	TypeString

	// TypeObject is JSON object type.
	TypeObject

	// TypeArray is JSON array type.
	TypeArray

	// TypeBoolean is JSON boolean.
	TypeBoolean

	// TypeDate is JSON date.
	TypeDate

	// TypeTime is JSON time.
	TypeTime

	// TypeDateTime is JSON time.
	TypeDateTime

	// TypeOpaque is JSON opaque type.
	TypeOpaque

	// TypeBit is JSON bit string.
	TypeBit

	// TypeBlob is JSON blob.
	TypeBlob

	typeRawString
)

type NumberType int32

const (
	NumberTypeUnknown NumberType = iota
	NumberTypeSigned
	NumberTypeUnsigned
	NumberTypeDecimal
	NumberTypeFloat
	numberTypeRaw
)

// String returns string representation of t.
func (t Type) String() string {
	switch t {
	case TypeObject:
		return "object"
	case TypeArray:
		return "array"
	case TypeString:
		return "string"
	case TypeNumber:
		return "number"
	case TypeBoolean:
		return "boolean"
	case TypeBlob:
		return "blob"
	case TypeBit:
		return "bit"
	case TypeDate:
		return "date"
	case TypeTime:
		return "time"
	case TypeDateTime:
		return "datetime"
	case TypeOpaque:
		return "opaque"
	case TypeNull:
		return "null"

	// typeRawString is skipped intentionally,
	// since it shouldn't be visible to user.
	default:
		panic(fmt.Errorf("BUG: unknown Value type: %d", t))
	}
}

// Type returns the type of the v.
func (v *Value) Type() Type {
	if v == nil {
		return TypeNull
	}
	if v.t == typeRawString {
		v.s = unescapeStringBestEffort(v.s)
		v.t = TypeString
	}
	return v.t
}

func (v *Value) Date() (datetime.Date, bool) {
	switch v.t {
	case TypeDate:
		return datetime.ParseDate(v.s)
	case TypeDateTime:
		dt, _, ok := datetime.ParseDateTime(v.s, datetime.DefaultPrecision)
		return dt.Date, ok
	}
	return datetime.Date{}, false
}

func (v *Value) DateTime() (datetime.DateTime, bool) {
	switch v.t {
	case TypeDate:
		d, ok := datetime.ParseDate(v.s)
		return datetime.DateTime{Date: d}, ok
	case TypeDateTime:
		dt, _, ok := datetime.ParseDateTime(v.s, datetime.DefaultPrecision)
		return dt, ok
	}
	return datetime.DateTime{}, false
}

func (v *Value) Time() (datetime.Time, bool) {
	if v.t != TypeTime {
		return datetime.Time{}, false
	}
	t, _, ok := datetime.ParseTime(v.s, datetime.DefaultPrecision)
	return t, ok
}

// Object returns the underlying JSON object for the v.
func (v *Value) Object() (*Object, bool) {
	if v.t != TypeObject {
		return nil, false
	}
	return &v.o, true
}

// Array returns the underlying JSON array for the v.
func (v *Value) Array() ([]*Value, bool) {
	if v.t != TypeArray {
		return nil, false
	}
	return v.a, true
}

// StringBytes returns the underlying JSON string for the v.
func (v *Value) StringBytes() ([]byte, bool) {
	if v.Type() != TypeString {
		return nil, false
	}
	return hack.StringBytes(v.s), true
}

func (v *Value) Raw() string {
	return v.s
}

func (v *Value) NumberType() NumberType {
	if v.t != TypeNumber {
		return NumberTypeUnknown
	}
	if v.n == numberTypeRaw {
		v.n = parseNumberType(v.s)
	}
	return v.n
}

func parseNumberType(ns string) NumberType {
	_, err := fastparse.ParseInt64(ns, 10)
	if err == nil {
		return NumberTypeSigned
	}
	_, err = fastparse.ParseUint64(ns, 10)
	if err == nil {
		return NumberTypeUnsigned
	}
	_, err = fastparse.ParseFloat64(ns)
	if err == nil {
		return NumberTypeFloat
	}
	return NumberTypeUnknown
}

func (v *Value) Int64() (int64, bool) {
	i, err := fastparse.ParseInt64(v.s, 10)
	if err != nil {
		return i, false
	}
	return i, true
}

func (v *Value) Uint64() (uint64, bool) {
	u, err := fastparse.ParseUint64(v.s, 10)
	if err != nil {
		return u, false
	}
	return u, true
}

func (v *Value) Float64() (float64, bool) {
	val, err := fastparse.ParseFloat64(v.s)
	if err != nil {
		return val, false
	}
	return val, true
}

func (v *Value) Decimal() (decimal.Decimal, bool) {
	dec, err := decimal.NewFromString(v.s)
	if err != nil {
		return decimal.Zero, false
	}
	return dec, true
}

// Bool returns the underlying JSON bool for the v.
//
// Use GetBool if you don't need error handling.
func (v *Value) Bool() (bool, bool) {
	if v == ValueTrue {
		return true, true
	}
	if v == ValueFalse {
		return false, true
	}
	return false, false
}

var (
	ValueTrue  = &Value{t: TypeBoolean}
	ValueFalse = &Value{t: TypeBoolean}
	ValueNull  = &Value{t: TypeNull}
)
