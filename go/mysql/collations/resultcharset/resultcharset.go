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

// Package resultcharset converts the text of a query result from one
// character set to another, the way MySQL does it for character_set_results.
//
// There are two conversions:
//
//   - ToConnection takes a result that MySQL sent with character_set_results =
//     NULL, where every text column is in the character set of the column, and
//     converts the text to the connection character set. A latin1 column with
//     "Müller" (4D FC 6C 6C 65 72) becomes utf8mb4 (4D C3 BC 6C 6C 65 72).
//   - ToClient takes a result with all text in the connection character set
//     and converts it to the character_set_results of a client.
//
// MySQL writes "?" for a character that the target character set does not
// have, and so do these conversions. Neither changes a result that needs no
// conversion, and neither changes the result it is given: a converted result
// has new fields and new rows.
package resultcharset

import (
	"fmt"
	"math"
	"sync"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Conversion converts the results of one query. It is built from the fields of
// the result, which a streamed result has in its first packet only, and then
// converts the rows of every packet.
type Conversion struct {
	// fields are the converted fields, and nil when the fields do not change.
	fields []*querypb.Field
	// columns has the conversion of each column that has one. It is nil when
	// no column changes its bytes.
	columns []column
}

// column is the conversion of the values of one column. A column without a
// conversion has a nil from.
type column struct {
	from, to charset.Charset
	// ascii is true when a value that is all ASCII is the same in from and
	// to, which holds for every character set but ucs2, utf16 and utf32.
	ascii bool
	// ucs2 is true for a conversion from ucs2 to utf8mb3 or utf8mb4, which
	// MySQL does for every code unit, surrogates included (see
	// appendUCS2AsUTF8).
	ucs2 bool
}

// isText reports whether MySQL converts the values of a column of this type
// for character_set_results.
func isText(t querypb.Type) bool {
	return sqltypes.IsText(t) || t == sqltypes.Enum || t == sqltypes.Set
}

// lookupCharset returns the character set of a collation, and nil for a
// collation that Vitess does not have and for the binary collation.
func lookupCharset(id collations.ID) charset.Charset {
	if id == collations.Unknown || id == collations.CollationBinaryID {
		return nil
	}
	coll := colldata.Lookup(id)
	if coll == nil {
		return nil
	}
	return coll.Charset()
}

// isASCIICompatible reports whether ASCII text has the same bytes in cs as in
// ASCII. ucs2, utf16, utf16le and utf32 are the character sets where it does
// not.
func isASCIICompatible(cs charset.Charset) bool {
	var buf [4]byte
	return cs.EncodeRune(buf[:], 'A') == 1 && buf[0] == 'A'
}

// connectionLength returns the column length that MySQL sends for a column of
// the character set from when character_set_results has the character set to.
//
// MySQL divides the length of the column by the longest character of from and
// multiplies it by the longest character of to, so a latin1 VARCHAR(10) has
// the length 10, and 40 as utf8mb4. For the MySQL types TINY_BLOB to BLOB,
// which carry the TEXT types and long string expressions, it divides by the
// shortest character instead: a ucs2 TEXT has the length 65535, and 131068 as
// utf8mb4. The length is at most 4294967295, so a latin1 LONGTEXT keeps
// 4294967295.
func connectionLength(field *querypb.Field, from, to charset.Charset) uint32 {
	width := from.MaxWidth()
	if field.Type == sqltypes.Text || field.Type == sqltypes.Blob {
		width = minWidth(from)
	}
	scaled := uint64(field.ColumnLength) / uint64(width) * uint64(to.MaxWidth())
	return uint32(min(scaled, math.MaxUint32))
}

// sameCharset reports whether a and b are the same character set.
func sameCharset(a, b charset.Charset) bool {
	return a.Name() == b.Name()
}

// ConnectionLengths returns the conversion that gives each column of a result
// that MySQL sent with character_set_results = NULL whose collation is in the
// character set of the connection collation conn the length that MySQL sends
// for it with character_set_results set to conn, and changes nothing else.
//
// ToConnection leaves the length of such a column alone, because it cannot tell
// it from a column that MySQL converted already: a utf8mb4_0900_ai_ci TEXT has
// the length 65535 with character_set_results = NULL, and 262140 with
// utf8mb4, with the same collation in both. A vttablet that returns a result
// as MySQL sent it applies ConnectionLengths first, so that ToConnection gives
// the lengths that MySQL sends, and leaves a result that a vttablet without
// character_set_results = NULL returns as it is.
func ConnectionLengths(fields []*querypb.Field, conn collations.ID) *Conversion {
	c := &Conversion{}
	to := lookupCharset(conn)
	if to == nil {
		return c
	}
	for i, field := range fields {
		from := lookupCharset(collations.ID(field.Charset))
		if from == nil || !sameCharset(from, to) {
			continue
		}
		if length := connectionLength(field, from, to); length != field.ColumnLength {
			converted := field.CloneVT()
			converted.ColumnLength = length
			c.setField(fields, i, converted)
		}
	}
	return c
}

// ToConnection returns the conversion of a result that MySQL sent with
// character_set_results = NULL to the connection collation conn.
//
// The text of every column whose collation is not binary is converted from
// the character set of that collation to the character set of conn, and the
// column length is the one of the converted column, except for a column whose
// collation is in the character set of conn, which keeps its length (see
// ConnectionLengths). Applied to a result that MySQL sent with
// character_set_results set to conn, it changes nothing. When relabel is true, the
// field also gets conn as its collation, and the result is the one MySQL
// sends with character_set_results set to conn. When relabel is false, the
// field keeps the collation of the column.
//
// MySQL also converts the text of JSON, which keeps the binary collation in
// its field, and the names of the fields. Both are utf8mb4 or utf8mb3 with
// character_set_results = NULL, so for a utf8mb4 connection neither changes.
func ToConnection(fields []*querypb.Field, conn collations.ID, relabel bool) *Conversion {
	c := &Conversion{}
	to := lookupCharset(conn)
	if to == nil {
		return c
	}

	// MySQL sends the names of the fields in its system character set,
	// utf8mb3, for character_set_results = NULL, and converts them for
	// any other character_set_results.
	names := !to.IsSuperset(systemCharset)
	for i, field := range fields {
		converted := field
		newField := func() *querypb.Field {
			if converted == field {
				converted = field.CloneVT()
			}
			return converted
		}
		if names {
			convertNames(field, to, newField)
		}

		// MySQL converts the text of every column whose collation is not
		// binary, whatever its type. A utf8mb4_bin VARCHAR has the binary
		// flag, which makes it a VarBinary here, and JSON_EXTRACT returns
		// JSON with utf8mb4_bin: both arrive with the collation 46, and
		// MySQL sends them with the collation and the length of the
		// connection for character_set_results = utf8mb4.
		from := lookupCharset(collations.ID(field.Charset))
		switch {
		case from != nil:
			// A column in the character set of conn keeps its length:
			// see ConnectionLengths.
			if !sameCharset(from, to) {
				if length := connectionLength(field, from, to); length != field.ColumnLength {
					newField().ColumnLength = length
				}
			}
			if relabel && field.Charset != uint32(conn) {
				newField().Charset = uint32(conn)
			}
		case field.Type == sqltypes.TypeJSON:
			// A JSON column has the binary collation and its text is
			// utf8mb4. MySQL converts the text, and keeps the field: a
			// JSON value with "Müller" arrives as 4D FC 6C 6C 65 72 for
			// character_set_results = latin1, with the collation 63.
			from = jsonCharset
		}
		if converted != field {
			c.setField(fields, i, converted)
		}
		if from != nil && !to.IsSuperset(from) {
			c.setColumn(len(fields), i, from, to)
			// swe7 has Ä, Ö, Å, Ü, é, ä, ö, å and ü at the bytes of
			// [ \ ] ^ ` { | } and ~, so a value of these bytes is not
			// the same in utf8mb4: MySQL sends 5B as C3 84.
			col := &c.columns[i]
			col.ascii = col.ascii && isASCIIIdentical(from) && isASCIIIdentical(to)
			_, fromUCS2 := from.(charset.Charset_ucs2)
			col.ucs2 = fromUCS2 && to.IsSuperset(systemCharset)
		}
	}
	return c
}

var (
	// systemCharset is the character set of the names of the fields
	// that MySQL sends for character_set_results = NULL.
	systemCharset = lookupCharset(collations.ID(33))
	// jsonCharset is the character set of the text of JSON.
	jsonCharset = lookupCharset(collations.CollationUtf8mb4ID)
)

// convertNames converts the names of a field from the system character set
// to the character set to, in the field that newField returns.
func convertNames(field *querypb.Field, to charset.Charset, newField func() *querypb.Field) {
	convert := func(name string) (string, bool) {
		if isASCII(name) && isASCIIIdentical(to) {
			return name, false
		}
		return convertName(name, systemCharset, to)
	}
	if name, ok := convert(field.Name); ok {
		newField().Name = name
	}
	if name, ok := convert(field.OrgName); ok {
		newField().OrgName = name
	}
	if name, ok := convert(field.Table); ok {
		newField().Table = name
	}
	if name, ok := convert(field.OrgTable); ok {
		newField().OrgTable = name
	}
	if name, ok := convert(field.Database); ok {
		newField().Database = name
	}
}

// isASCIIIdentical reports whether every byte below 0x80 is the same ASCII
// character in cs. It is false for swe7, and for ucs2, utf16, utf16le and
// utf32, where ASCII needs more than one byte.
func isASCIIIdentical(cs charset.Charset) bool {
	if v, ok := asciiIdentical.Load(cs.Name()); ok {
		return v.(bool)
	}
	identical := true
	var buf [4]byte
	for b := 0; b < 0x80 && identical; b++ {
		r, width, ok := cs.DecodeRune([]byte{byte(b)})
		identical = ok && width == 1 && r == rune(b) && cs.EncodeRune(buf[:], rune(b)) == 1 && buf[0] == byte(b)
	}
	asciiIdentical.Store(cs.Name(), identical)
	return identical
}

// asciiIdentical caches isASCIIIdentical by the name of the character set.
var asciiIdentical sync.Map

// ToClient returns the conversion of a result that a tablet returned to the
// character_set_results of a client.
//
// The text of each column is in the character set of the collation in its
// field, which is how a result has it that MySQL sent with
// character_set_results = NULL, and how one has it that MySQL converted to the
// connection collation conn already. A latin1_german2_ci column with 'Müller'
// is 4D FC 6C 6C 65 72 with the collation 31, or 4D C3 BC 6C 6C 65 72 with the
// collation 255.
//
// results is the collation of character_set_results. The text of every
// column, and the names in every field, are converted to the character set of
// results, and every text field gets results as its collation and the length
// that MySQL reports for it: the length in characters times the longest
// character of the character set of results. For gbk the column is
// 4D A8 B9 6C 6C 65 72 with the collation 28 and the length 20. Each value is
// converted once, from the character set of its column.
//
// null is true for character_set_results = NULL, and results can be the
// binary collation. MySQL then sends the text of every column as it is
// stored, and so does this conversion: a value that is in the character set
// of its column is not touched. With NULL the fields keep their collation and
// get the length of the column in its own character set, and with binary they
// get the binary collation and a length of one byte per character. The names
// stay as they are for both.
//
// The fields and lengths are the ones of ToConnection followed by the
// conversion from the connection character set, see fromConnection: both were
// checked against MySQL 8.0.46 and 8.4.13.
func ToClient(fields []*querypb.Field, conn, results collations.ID, null bool) *Conversion {
	// first makes the result that MySQL sends for character_set_results =
	// conn, with the collation of each column kept, and second converts
	// that to the client. Their fields are the ones to send. Their column
	// conversions are joined into one for each column, so that no value
	// goes through the connection character set on its way.
	first := ToConnection(fields, conn, false)
	inner := first.Fields(fields)
	second := fromConnection(inner, conn, results, null)

	c := &Conversion{fields: second.fields}
	if c.fields == nil {
		c.fields = first.fields
	}
	for i := range fields {
		var in, out *column
		if first.columns != nil && first.columns[i].from != nil {
			in = &first.columns[i]
		}
		if second.columns != nil && second.columns[i].from != nil {
			out = &second.columns[i]
		}
		switch {
		case in != nil && out != nil:
			if out.to.IsSuperset(in.from) || sameCharset(in.from, out.to) {
				// The column is in the character set the client asked
				// for, as with NULL and binary.
				continue
			}
			c.setColumn(len(fields), i, in.from, out.to)
			joined := &c.columns[i]
			joined.ascii = isASCIIIdentical(in.from) && isASCIIIdentical(out.to)
			joined.ucs2 = in.ucs2 && out.to.IsSuperset(systemCharset)
		case in != nil:
			if c.columns == nil {
				c.columns = make([]column, len(fields))
			}
			c.columns[i] = *in
		case out != nil:
			if c.columns == nil {
				c.columns = make([]column, len(fields))
			}
			c.columns[i] = *out
		}
	}
	return c
}

// fromConnection returns the conversion of a result with all text in the
// character set of the connection collation conn to the character_set_results
// of a client. It is the second half of ToClient.
//
// results is the collation of character_set_results. The text of every
// column, and the names in every field, are converted to the character set of
// results, and every text field gets results as its collation and the length
// that MySQL reports for it: the length in characters times the longest
// character of the character set of results. A utf8mb4 VARCHAR(10) has the
// length 40, and 10 as latin1.
//
// null is true for character_set_results = NULL. The text of every column is
// then converted to the character set of the collation in its field, the
// fields keep their collation and get the length of the column in that
// character set, and the names stay as they are.
//
// results can be the binary collation. MySQL then sends the text of every
// column as it is stored, in the character set of the column, as for NULL, but
// the fields of text columns get the binary collation and a length of one byte
// per character, and the names stay as they are.
//
// The lengths in the fields are taken to be the ones MySQL reports with
// character_set_results set to conn, which is what a tablet returns. A TEXT or
// BLOB column with the length 4294967295 keeps it, as it does in MySQL for a
// LONGTEXT column in every character set.
//
// Which columns are text is decided by the collation in the field, as MySQL
// decides it: a column of a _bin collation is VARBINARY in Vitess, and is
// text all the same. MySQL also sends JSON as text and converts it with the
// rest. A JSON column keeps the binary collation and its length in its
// field, and the field of a JSON function result, which has a text collation,
// gets results like a text column. MySQL converts the text of a DECIMAL
// expression as well, but not of a DECIMAL column: with ucs2, 1.5 is
// 00 31 00 2E 00 35. Vitess tells the two apart by the binary flag, which
// MySQL sets on the field of the expression only. Other numbers, dates and
// binary columns are never converted.
func fromConnection(fields []*querypb.Field, conn, results collations.ID, null bool) *Conversion {
	c := &Conversion{}
	from := lookupCharset(conn)
	if from == nil {
		return c
	}
	asIs := null || results == collations.CollationBinaryID
	var target charset.Charset
	if !asIs {
		if target = lookupCharset(results); target == nil {
			return c
		}
	}

	for i, field := range fields {
		converted := field
		newField := func() *querypb.Field {
			if converted == field {
				converted = field.CloneVT()
			}
			return converted
		}
		relabel := func(collation collations.ID, to charset.Charset) {
			if length := clientLength(field, from, to, false); length != field.ColumnLength {
				newField().ColumnLength = length
			}
			if field.Charset != uint32(collation) {
				newField().Charset = uint32(collation)
			}
		}

		// to is the character set of the values, and nil when they stay
		// as they are.
		var to charset.Charset
		switch kind := clientKind(field); {
		case kind == kindOther:
		case kind == kindDecimal:
			to = target
		case asIs:
			if kind == kindJSON {
				// JSON is in utf8mb4 already, and the text of a JSON
				// column has no other character set. The field of a
				// JSON function has a text collation.
				own := lookupCharset(collations.ID(field.Charset))
				switch {
				case own == nil:
				case null:
					if length := clientLength(field, from, own, true); length != field.ColumnLength {
						newField().ColumnLength = length
					}
				default:
					relabel(collations.CollationBinaryID, binaryCharset{})
				}
				break
			}
			to = lookupCharset(collations.ID(field.Charset))
			if to == nil {
				break
			}
			if null {
				if length := clientLength(field, from, to, true); length != field.ColumnLength {
					newField().ColumnLength = length
				}
			} else {
				relabel(collations.CollationBinaryID, binaryCharset{})
			}
		default:
			to = target
			if kind == kindJSON && field.Charset == uint32(collations.CollationBinaryID) {
				break
			}
			relabel(results, target)
		}

		if !asIs && !target.IsSuperset(from) {
			// MySQL sends the names of a result in
			// character_set_results as well.
			if name, ok := convertName(field.Name, from, target); ok {
				newField().Name = name
			}
			if name, ok := convertName(field.OrgName, from, target); ok {
				newField().OrgName = name
			}
			if name, ok := convertName(field.Table, from, target); ok {
				newField().Table = name
			}
			if name, ok := convertName(field.OrgTable, from, target); ok {
				newField().OrgTable = name
			}
			if name, ok := convertName(field.Database, from, target); ok {
				newField().Database = name
			}
		}

		if converted != field {
			c.setField(fields, i, converted)
		}
		if to != nil && !to.IsSuperset(from) {
			c.setColumn(len(fields), i, from, to)
		}
	}
	return c
}

// The kinds of columns that fromConnection tells apart.
const (
	kindOther = iota
	kindText
	kindJSON
	kindDecimal
)

// clientKind returns how fromConnection converts a column. A column of a string type
// is text unless its field has the binary collation, so that a VARBINARY
// column of utf8mb4_bin is text. A string column whose field has no collation
// at all is text when its type is a text type, and binary otherwise.
func clientKind(field *querypb.Field) int {
	switch {
	case field.Type == sqltypes.TypeJSON:
		return kindJSON
	case field.Type == sqltypes.Decimal:
		if field.Flags&uint32(querypb.MySqlFlag_BINARY_FLAG) != 0 {
			return kindDecimal
		}
		return kindOther
	case isText(field.Type):
		if field.Charset == uint32(collations.CollationBinaryID) {
			return kindOther
		}
		return kindText
	case field.Type == sqltypes.VarBinary || field.Type == sqltypes.Binary || field.Type == sqltypes.Blob:
		if field.Charset == uint32(collations.CollationBinaryID) || field.Charset == uint32(collations.Unknown) {
			return kindOther
		}
		return kindText
	}
	return kindOther
}

// clientLength returns the column length of a field in the character set to,
// from its length in the character set from. MySQL computes it from the
// length of the column in its own character set: the length in characters
// times the longest character of to, and for a TEXT or BLOB column the
// length in bytes divided by the shortest character of the column. The
// length 4294967295 of a TEXT or BLOB column stays as it is.
//
// own is true when to is the character set of the column, for
// character_set_results = NULL. The length of a TEXT or BLOB column is then
// in bytes, and the length of any other column whose number of characters is
// the largest that fits is 4294967295: MySQL sends 4294967295 for a JSON
// function with NULL and 4294967292 with utf8mb4.
func clientLength(field *querypb.Field, from, to charset.Charset, own bool) uint32 {
	length := field.ColumnLength
	blob := field.Type == sqltypes.Text || field.Type == sqltypes.Blob
	if blob && length == math.MaxUint32 {
		return length
	}
	chars := uint64(length) / uint64(from.MaxWidth())
	if own && !blob && chars == math.MaxUint32/uint64(from.MaxWidth()) {
		return math.MaxUint32
	}
	width := uint64(to.MaxWidth())
	if own && blob {
		width = uint64(minWidth(to))
	}
	if width == uint64(from.MaxWidth()) {
		return length
	}
	return uint32(min(chars*width, math.MaxUint32))
}

// minWidth returns the length of the shortest character of cs: 1, or 2 for
// ucs2, utf16 and utf16le, and 4 for utf32.
func minWidth(cs charset.Charset) int {
	var buf [4]byte
	return max(cs.EncodeRune(buf[:], 'A'), 1)
}

// binaryCharset stands for the binary character set in the length of a field:
// one byte for each character.
type binaryCharset struct{ charset.Charset }

func (binaryCharset) MaxWidth() int { return 1 }

func (binaryCharset) EncodeRune(dst []byte, r rune) int {
	dst[0] = byte(r)
	return 1
}

// ToClientMessage returns the text of an error message, which is in utf8mb4,
// in the character_set_results of a client, as MySQL sends it. results and
// null are as for ToClient.
//
// MySQL writes a character that the character set does not have as a
// backslash and its code point in four hexadecimal digits: with latin1,
// "Table 't.x中' doesn't exist" ends in "x\4E2D'". It sends the message as
// it is with NULL and binary, and an empty message with ucs2, utf16 and
// utf32. A MySQL error message cannot hold a character beyond U+FFFF, it has
// "?" for one, and so does this conversion when results does not have it.
func ToClientMessage(message string, results collations.ID, null bool) string {
	if null || results == collations.CollationBinaryID {
		return message
	}
	to := lookupCharset(results)
	if to == nil || (isASCII(message) && isASCIICompatible(to)) {
		return message
	}
	if !isASCIICompatible(to) {
		return ""
	}
	out := make([]byte, 0, len(message))
	var buf [4]byte
	for i := 0; i < len(message); {
		r, width := utf8.DecodeRuneInString(message[i:])
		if r == utf8.RuneError && width == 1 {
			// MySQL takes a byte that is not UTF-8 as the code
			// point of the same value.
			r = rune(message[i])
		}
		i += width
		if n := to.EncodeRune(buf[:], r); n > 0 {
			out = append(out, buf[:n]...)
		} else if r <= 0xFFFF {
			out = fmt.Appendf(out, "\\%04X", r)
		} else {
			out = append(out, '?')
		}
	}
	return string(out)
}

// convertName converts a name of a field, and reports whether the name
// changed.
func convertName(name string, from, to charset.Charset) (string, bool) {
	if name == "" || (isASCII(name) && isASCIICompatible(to)) {
		return name, false
	}
	converted, _ := charset.Convert(nil, to, []byte(name), from)
	return string(converted), true
}

func isASCII[T string | []byte](s T) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

func (c *Conversion) setField(fields []*querypb.Field, i int, field *querypb.Field) {
	if c.fields == nil {
		c.fields = make([]*querypb.Field, len(fields))
		copy(c.fields, fields)
	}
	c.fields[i] = field
}

func (c *Conversion) setColumn(count, i int, from, to charset.Charset) {
	if c.columns == nil {
		c.columns = make([]column, count)
	}
	c.columns[i] = column{from: from, to: to, ascii: isASCIICompatible(from) && isASCIICompatible(to)}
}

// IsNoOp reports whether the conversion changes neither fields nor rows.
func (c *Conversion) IsNoOp() bool {
	return c.fields == nil && c.columns == nil
}

// Fields returns the converted fields: the fields it is given when they do not
// change.
func (c *Conversion) Fields(fields []*querypb.Field) []*querypb.Field {
	if c.fields == nil {
		return fields
	}
	return c.fields
}

// Rows returns the converted rows. A row without a value that changes is the
// row it was, and so is the slice when no row changes.
func (c *Conversion) Rows(rows []sqltypes.Row) []sqltypes.Row {
	if c.columns == nil {
		return rows
	}

	var out []sqltypes.Row
	for r, row := range rows {
		var newRow sqltypes.Row
		// buf holds the converted values of the row, one after the other,
		// so that a row costs one allocation and not one for each value.
		var buf []byte
		for i, value := range row {
			if i >= len(c.columns) {
				break
			}
			col := &c.columns[i]
			if col.from == nil || value.IsNull() {
				continue
			}
			raw := value.Raw()
			if col.ascii && isASCII(raw) {
				continue
			}
			// The error says that a character has no form in the
			// target character set. The converted text has "?" in
			// its place, which is what MySQL sends.
			if newRow == nil {
				newRow = make(sqltypes.Row, len(row))
				copy(newRow, row)
				buf = make([]byte, 0, c.rowBufferSize(row[i:], i))
			}
			start := len(buf)
			if col.ucs2 {
				buf = appendUCS2AsUTF8(buf, raw)
			} else {
				buf, _ = charset.Convert(buf, col.to, raw, col.from)
			}
			converted := buf[start:len(buf):len(buf)]
			newRow[i] = sqltypes.MakeTrusted(value.Type(), converted)
		}
		if newRow == nil {
			continue
		}
		if out == nil {
			out = make([]sqltypes.Row, len(rows))
			copy(out, rows)
		}
		out[r] = newRow
	}
	if out == nil {
		return rows
	}
	return out
}

// rowBufferSize returns the size of the buffer for the converted values of
// the columns of a row from the column first on: twice their length, which
// is enough for latin1 text as utf8mb4, and for utf8mb4 text in any character
// set but utf16 and utf32. charset.Convert makes the buffer larger when it
// has to.
func (c *Conversion) rowBufferSize(values sqltypes.Row, first int) int {
	size := 0
	for i, value := range values {
		if first+i >= len(c.columns) {
			break
		}
		if c.columns[first+i].from != nil {
			size += value.Len()
		}
	}
	// charset.Convert needs a buffer that is not empty to make it larger.
	return 2*size + 4
}

// appendUCS2AsUTF8 appends ucs2 text as utf8mb3, which is utf8mb4 as well,
// the way MySQL converts it for character_set_results.
//
// MySQL writes each 2-byte code unit as UTF-8, and a surrogate as well: a
// ucs2 column with D8 00 arrives as ED A0 80 for character_set_results =
// utf8mb4, where charset.Convert writes "?". It drops a last byte that has
// no second byte.
func appendUCS2AsUTF8(dst, src []byte) []byte {
	for ; len(src) >= 2; src = src[2:] {
		r := uint16(src[0])<<8 | uint16(src[1])
		switch {
		case r < 0x80:
			dst = append(dst, byte(r))
		case r < 0x800:
			dst = append(dst, 0xC0|byte(r>>6), 0x80|byte(r&0x3F))
		default:
			dst = append(dst, 0xE0|byte(r>>12), 0x80|byte(r>>6&0x3F), 0x80|byte(r&0x3F))
		}
	}
	return dst
}

// Result returns the converted result: the result it is given when nothing
// changes, and otherwise a new result that shares what did not change.
func (c *Conversion) Result(qr *sqltypes.Result) *sqltypes.Result {
	if qr == nil || c.IsNoOp() {
		return qr
	}
	var fields []*querypb.Field
	if qr.Fields != nil {
		fields = c.Fields(qr.Fields)
	}
	rows := c.Rows(qr.Rows)
	if len(fields) == len(qr.Fields) && (len(fields) == 0 || &fields[0] == &qr.Fields[0]) &&
		len(rows) == len(qr.Rows) && (len(rows) == 0 || &rows[0] == &qr.Rows[0]) {
		return qr
	}
	return &sqltypes.Result{
		Fields:              fields,
		RowsAffected:        qr.RowsAffected,
		InsertID:            qr.InsertID,
		InsertIDChanged:     qr.InsertIDChanged,
		Rows:                rows,
		SessionStateChanges: qr.SessionStateChanges,
		StatusFlags:         qr.StatusFlags,
		Info:                qr.Info,
	}
}

// Stream converts the packets of a streamed result, which has its fields in
// the first packet only.
type Stream struct {
	build func(fields []*querypb.Field) *Conversion
	conv  *Conversion
}

// NewStream returns a Stream that builds its conversion with build when the
// fields arrive.
func NewStream(build func(fields []*querypb.Field) *Conversion) *Stream {
	return &Stream{build: build}
}

// Result returns the converted packet, see Conversion.Result.
func (s *Stream) Result(qr *sqltypes.Result) *sqltypes.Result {
	if qr != nil && len(qr.Fields) > 0 {
		s.conv = s.build(qr.Fields)
	}
	if s.conv == nil {
		return qr
	}
	return s.conv.Result(qr)
}
