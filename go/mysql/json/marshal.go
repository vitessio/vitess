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

package json

import (
	"errors"
	"fmt"
	"math/big"
	"unicode/utf16"
	"unicode/utf8"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/hex"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// MarshalSQLTo appends marshaled v to dst and returns the result in
// the form like `JSON_OBJECT` or `JSON_ARRAY` to ensure we don't
// lose any type information.
func (v *Value) MarshalSQLTo(dst []byte) []byte {
	return v.marshalSQLInternal(true, dst)
}

func (v *Value) marshalSQLInternal(top bool, dst []byte) []byte {
	switch v.Type() {
	case TypeObject:
		dst = append(dst, "JSON_OBJECT("...)
		for i, vv := range v.o.kvs {
			if i != 0 {
				dst = append(dst, ", "...)
			}
			dst = append(dst, "_utf8mb4"...)
			dst = append(dst, sqltypes.EncodeStringSQL(vv.k)...)
			dst = append(dst, ", "...)
			dst = vv.v.marshalSQLInternal(false, dst)
		}
		dst = append(dst, ')')
		return dst
	case TypeArray:
		dst = append(dst, "JSON_ARRAY("...)
		for i, vv := range v.a {
			if i != 0 {
				dst = append(dst, ", "...)
			}
			dst = vv.marshalSQLInternal(false, dst)
		}
		dst = append(dst, ')')
		return dst
	case TypeString:
		if top {
			dst = append(dst, "CAST(JSON_QUOTE("...)
		}
		dst = append(dst, "_utf8mb4"...)
		dst = append(dst, sqltypes.EncodeStringSQL(v.s)...)
		if top {
			dst = append(dst, ") as JSON)"...)
		}
		return dst
	case TypeDate:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "date '"...)
		dst = append(dst, v.MarshalDate()...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeDateTime:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "timestamp '"...)
		dst = append(dst, v.MarshalDateTime()...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeTime:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "time '"...)
		dst = append(dst, v.MarshalTime()...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeBlob:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "x'"...)
		dst = append(dst, hex.EncodeBytes(hack.StringBytes(v.s))...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeBit:
		if top {
			dst = append(dst, "CAST("...)
		}
		var i big.Int
		i.SetBytes([]byte(v.s))
		dst = append(dst, "b'"...)
		dst = append(dst, i.Text(2)...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeNumber:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, v.s...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeBoolean:
		if top {
			dst = append(dst, "CAST(_utf8mb4'"...)
		}
		if v == ValueTrue {
			dst = append(dst, "true"...)
		} else {
			dst = append(dst, "false"...)
		}
		if top {
			dst = append(dst, "' as JSON)"...)
		}
		return dst
	case TypeNull:
		if top {
			dst = append(dst, "CAST(_utf8mb4'"...)
		}
		dst = append(dst, "null"...)
		if top {
			dst = append(dst, "' as JSON)"...)
		}
		return dst
	default:
		panic(fmt.Errorf("BUG: unexpected Value type: %d", v.t))
	}
}

// MarshalSQLValue converts text JSON bytes into a SQL expression using
// JSON_OBJECT/JSON_ARRAY syntax. It scans the raw bytes directly without
// building an intermediate tree, avoiding the ~60x memory amplification
// of tree-based encoding.
func MarshalSQLValue(raw []byte) (*sqltypes.Value, error) {
	if len(raw) == 0 {
		raw = sqltypes.NullBytes
	}
	buf := &bytes2.Buffer{}
	if err := AppendMarshalSQL(buf, raw); err != nil {
		return nil, err
	}
	v := sqltypes.MakeTrusted(querypb.Type_RAW, buf.Bytes())
	return &v, nil
}

// AppendMarshalSQL converts text JSON into a SQL expression using
// JSON_OBJECT/JSON_ARRAY syntax, writing directly to buf. It scans
// the raw bytes directly without building an intermediate tree.
//
// This has O(recursion depth) memory overhead versus O(total nodes * 72
// bytes) for the tree-based Value.MarshalSQLTo, and avoids the per-token
// heap allocations of encoding/json.Decoder.Token.
//
// The output format matches the tree-based encoder so that MySQL stores
// identical binary JSON, including preservation of large integer precision
// via bare numeric literals.
func AppendMarshalSQL(buf *bytes2.Buffer, raw []byte) error {
	w := sqlWriter{
		data: raw,
		buf:  buf,
	}
	if err := w.writeValue(true, 0); err != nil {
		return err
	}
	w.skipWhitespace()
	if w.pos != len(w.data) {
		return errors.New("unexpected trailing data after JSON value")
	}
	return nil
}

// sqlWriter converts text JSON into SQL expressions by scanning
// the raw bytes directly. It uses O(recursion depth) memory overhead
// plus a reusable scratch buffer for string unescaping.
type sqlWriter struct {
	data    []byte
	pos     int
	buf     *bytes2.Buffer
	scratch []byte
}

func (w *sqlWriter) skipWhitespace() {
	for w.pos < len(w.data) {
		switch w.data[w.pos] {
		case ' ', '\t', '\n', '\r':
			w.pos++
		default:
			return
		}
	}
}

func (w *sqlWriter) writeValue(top bool, depth int) error {
	if depth >= MaxDepth {
		return fmt.Errorf("too big depth for the nested JSON; it exceeds %d", MaxDepth)
	}
	w.skipWhitespace()
	if w.pos >= len(w.data) {
		return errors.New("unexpected end of JSON input")
	}
	switch w.data[w.pos] {
	case '{':
		w.pos++
		return w.writeObject(depth)
	case '[':
		w.pos++
		return w.writeArray(depth)
	case '"':
		return w.writeString(top)
	case 't', 'f':
		return w.writeBool(top)
	case 'n':
		return w.writeNull(top)
	default:
		if w.data[w.pos] >= '0' && w.data[w.pos] <= '9' || w.data[w.pos] == '-' {
			return w.writeNumber(top)
		}
		return fmt.Errorf("unexpected character %q in JSON", w.data[w.pos])
	}
}

func (w *sqlWriter) writeObject(depth int) error {
	w.buf.WriteString("JSON_OBJECT(")
	first := true
	for {
		w.skipWhitespace()
		if w.pos >= len(w.data) {
			return errors.New("unexpected end of JSON input in object")
		}
		if w.data[w.pos] == '}' {
			w.pos++
			w.buf.WriteByte(')')
			return nil
		}
		if !first {
			if w.data[w.pos] != ',' {
				return fmt.Errorf("expected ',' or '}' in object, got %q", w.data[w.pos])
			}
			w.pos++
			w.buf.WriteString(", ")
			w.skipWhitespace()
		}
		first = false

		// Key (always a string).
		if w.pos >= len(w.data) || w.data[w.pos] != '"' {
			return errors.New("expected string key in JSON object")
		}
		w.buf.WriteString("_utf8mb4")
		if err := w.writeStringContent(); err != nil {
			return fmt.Errorf("reading JSON object key: %w", err)
		}
		w.buf.WriteString(", ")

		// Colon separator.
		w.skipWhitespace()
		if w.pos >= len(w.data) || w.data[w.pos] != ':' {
			return errors.New("expected ':' after object key")
		}
		w.pos++

		// Value.
		if err := w.writeValue(false, depth+1); err != nil {
			return err
		}
	}
}

func (w *sqlWriter) writeArray(depth int) error {
	w.buf.WriteString("JSON_ARRAY(")
	first := true
	for {
		w.skipWhitespace()
		if w.pos >= len(w.data) {
			return errors.New("unexpected end of JSON input in array")
		}
		if w.data[w.pos] == ']' {
			w.pos++
			w.buf.WriteByte(')')
			return nil
		}
		if !first {
			if w.data[w.pos] != ',' {
				return fmt.Errorf("expected ',' or ']' in array, got %q", w.data[w.pos])
			}
			w.pos++
			w.buf.WriteString(", ")
		}
		first = false

		if err := w.writeValue(false, depth+1); err != nil {
			return err
		}
	}
}

func (w *sqlWriter) writeString(top bool) error {
	if top {
		w.buf.WriteString("CAST(JSON_QUOTE(")
	}
	w.buf.WriteString("_utf8mb4")
	if err := w.writeStringContent(); err != nil {
		return err
	}
	if top {
		w.buf.WriteString(") as JSON)")
	}
	return nil
}

// writeStringContent reads a JSON string starting at w.pos (which must point
// at the opening '"'), JSON-unescapes it, SQL-encodes it into w.buf, and
// advances w.pos past the closing '"'.
func (w *sqlWriter) writeStringContent() error {
	if w.pos >= len(w.data) || w.data[w.pos] != '"' {
		return errors.New("expected '\"' at start of string")
	}
	w.pos++ // skip opening '"'

	// Scan to find the closing quote, tracking whether escape sequences exist.
	start := w.pos
	hasEscape := false
	for w.pos < len(w.data) {
		ch := w.data[w.pos]
		if ch == '\\' {
			if w.pos+1 >= len(w.data) {
				return errors.New("unterminated string in JSON")
			}
			hasEscape = true
			w.pos += 2 // skip '\' and the escaped character
			continue
		}
		if ch == '"' {
			break
		}
		w.pos++
	}
	if w.pos >= len(w.data) {
		return errors.New("unterminated string in JSON")
	}

	content := w.data[start:w.pos]
	w.pos++ // skip closing '"'

	if !hasEscape {
		// Fast path: no escape sequences, raw bytes are the decoded string.
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, content).EncodeSQLBytes2(w.buf)
	} else {
		// Slow path: unescape JSON into scratch buffer, then SQL-encode.
		var err error
		w.scratch, err = unescapeJSON(w.scratch[:0], content)
		if err != nil {
			return err
		}
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, w.scratch).EncodeSQLBytes2(w.buf)
	}
	return nil
}

// unescapeJSON appends the unescaped form of a JSON string body
// (the bytes between the quotes, not including the quotes themselves) to
// dst and returns the result. It handles all JSON escape sequences
// including \uXXXX and UTF-16 surrogate pairs.
func unescapeJSON(dst, src []byte) ([]byte, error) {
	i := 0
	for i < len(src) {
		if src[i] != '\\' {
			dst = append(dst, src[i])
			i++
			continue
		}
		if i+1 >= len(src) {
			return dst, errors.New("truncated escape sequence in JSON string")
		}
		i++ // skip '\'
		switch src[i] {
		case '"', '\\', '/':
			dst = append(dst, src[i])
			i++
		case 'b':
			dst = append(dst, '\b')
			i++
		case 'f':
			dst = append(dst, '\f')
			i++
		case 'n':
			dst = append(dst, '\n')
			i++
		case 'r':
			dst = append(dst, '\r')
			i++
		case 't':
			dst = append(dst, '\t')
			i++
		case 'u':
			i++ // skip 'u'
			if i+4 > len(src) {
				return dst, errors.New("truncated \\u escape in JSON string")
			}
			r := parseHex4(src[i : i+4])
			if r < 0 {
				return dst, fmt.Errorf("invalid hex digit in \\u escape: %q", src[i:i+4])
			}
			i += 4

			// Handle UTF-16 surrogate pairs.
			if utf16.IsSurrogate(r) {
				if i+6 <= len(src) && src[i] == '\\' && src[i+1] == 'u' {
					r2 := parseHex4(src[i+2 : i+6])
					if r2 >= 0 {
						combined := utf16.DecodeRune(r, r2)
						if combined != utf8.RuneError {
							dst = utf8.AppendRune(dst, combined)
							i += 6
							continue
						}
					}
				}
				// Lone surrogate: encode as replacement character.
				dst = utf8.AppendRune(dst, utf8.RuneError)
				continue
			}

			dst = utf8.AppendRune(dst, r)
		default:
			return dst, fmt.Errorf("invalid escape character %q in JSON string", src[i])
		}
	}
	return dst, nil
}

// parseHex4 parses exactly 4 hex digits into a rune. Returns -1 on error.
func parseHex4(s []byte) rune {
	var r rune
	for _, ch := range s {
		r <<= 4
		switch {
		case ch >= '0' && ch <= '9':
			r |= rune(ch - '0')
		case ch >= 'a' && ch <= 'f':
			r |= rune(ch - 'a' + 10)
		case ch >= 'A' && ch <= 'F':
			r |= rune(ch - 'A' + 10)
		default:
			return -1
		}
	}
	return r
}

func (w *sqlWriter) writeNumber(top bool) error {
	// Use the parser's readFloat to validate number grammar, rejecting
	// malformed inputs like "1+2", "1..2", or "1e+" that a simple
	// character-class loop would accept.
	n, ok := readFloat(hack.String(w.data[w.pos:]))
	if !ok || n == 0 {
		return fmt.Errorf("invalid number at position %d in JSON", w.pos)
	}
	if top {
		w.buf.WriteString("CAST(")
	}
	w.buf.Write(w.data[w.pos : w.pos+n])
	w.pos += n
	if top {
		w.buf.WriteString(" as JSON)")
	}
	return nil
}

func (w *sqlWriter) writeBool(top bool) error {
	if top {
		w.buf.WriteString("CAST(_utf8mb4'")
	}
	if w.pos+4 <= len(w.data) && string(w.data[w.pos:w.pos+4]) == "true" {
		w.buf.WriteString("true")
		w.pos += 4
	} else if w.pos+5 <= len(w.data) && string(w.data[w.pos:w.pos+5]) == "false" {
		w.buf.WriteString("false")
		w.pos += 5
	} else {
		return fmt.Errorf("unexpected token at position %d in JSON", w.pos)
	}
	if top {
		w.buf.WriteString("' as JSON)")
	}
	return nil
}

func (w *sqlWriter) writeNull(top bool) error {
	if w.pos+4 > len(w.data) || string(w.data[w.pos:w.pos+4]) != "null" {
		return fmt.Errorf("unexpected token at position %d in JSON", w.pos)
	}
	if top {
		w.buf.WriteString("CAST(_utf8mb4'")
	}
	w.buf.WriteString("null")
	w.pos += 4
	if top {
		w.buf.WriteString("' as JSON)")
	}
	return nil
}
