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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
}

// isText reports whether MySQL converts the values of a column of this type
// for character_set_results.
func isText(t querypb.Type) bool {
	return sqltypes.IsText(t) || t == sqltypes.Enum || t == sqltypes.Set
}

// lookupCharset returns the character set of a collation, and nil for the
// binary collation and for a collation whose character set Vitess does not
// implement.
//
// Converting text needs the character set of a collation only, so a collation
// that Vitess does not implement, such as latin1_german2_ci (31) or
// gb18030_chinese_ci (248), has the character set of the collations that
// Vitess implements for it: latin1 and gb18030. The exception is
// latin2_czech_cs (2), whose text MySQL converts with tables of its own:
// CONVERT((CONVERT(0x80 USING latin2) COLLATE latin2_czech_cs) USING utf8mb4)
// is "?", and with latin2_general_ci it is U+0080.
func lookupCharset(id collations.ID) charset.Charset {
	if id == collations.Unknown || id == collations.CollationBinaryID {
		return nil
	}
	if coll := colldata.Lookup(id); coll != nil {
		return coll.Charset()
	}
	if id == latin2CzechCS {
		return nil
	}
	return implementedCharsets()[charsetName(id)]
}

// latin2CzechCS is the collation ID of latin2_czech_cs.
const latin2CzechCS = collations.ID(2)

// charsetName returns the name of the character set of a collation that MySQL
// defines, whether or not Vitess implements it, as MySQL 8.0 names it, and ""
// for an ID that MySQL does not define.
func charsetName(id collations.ID) string {
	env := collations.MySQL8()
	name, _ := env.ConnectionCharset(id)
	if alias, ok := env.CharsetAlias(name); ok {
		return alias
	}
	return name
}

// implementedCharsets maps the name of every character set that Vitess
// implements a collation for to that character set.
var implementedCharsets = sync.OnceValue(func() map[string]charset.Charset {
	env := collations.MySQL8()
	byName := make(map[string]charset.Charset)
	for _, id := range env.AllCollationIDs() {
		name := env.LookupCharsetName(id)
		if _, ok := byName[name]; ok {
			continue
		}
		if coll := colldata.Lookup(id); coll != nil {
			byName[name] = coll.Charset()
		}
	}
	return byName
})

// CheckFields returns an error for the first field of a result whose text
// cannot be converted because Vitess does not implement the character set of
// its collation, and nil when there is none. MySQL sends such a column in its
// own character set with character_set_results = NULL: a gbk column with
// 'Müller' arrives as 4D A8 B9 6C 6C 65 72 with the collation 28. Vitess does
// not implement gbk, big5, tis620 or the tables of latin2_czech_cs, and
// returning such text unconverted would give a client bytes in a character
// set that it did not ask for.
func CheckFields(fields []*querypb.Field) error {
	for _, field := range fields {
		id := collations.ID(field.Charset)
		if id == collations.Unknown || id == collations.CollationBinaryID || lookupCharset(id) != nil {
			continue
		}
		name := charsetName(id)
		if id == latin2CzechCS {
			name = "latin2 with the collation latin2_czech_cs"
		} else if name == "" {
			name = fmt.Sprintf("of the collation %d", id)
		}
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot convert the text of column '%s': Vitess does not implement the character set %s", field.Name, name)
	}
	return nil
}

// isASCIICompatible reports whether ASCII text has the same bytes in cs as in
// ASCII. ucs2, utf16, utf16le and utf32 are the character sets where it does
// not.
func isASCIICompatible(cs charset.Charset) bool {
	var buf [4]byte
	return cs.EncodeRune(buf[:], 'A') == 1 && buf[0] == 'A'
}

// scaleLength returns the column length that MySQL reports for a column after
// a conversion: the length in characters times the longest character of the
// target character set. A latin1 VARCHAR(10) has the length 10, and 40 as
// utf8mb4.
func scaleLength(length uint32, from, to charset.Charset) uint32 {
	if from.MaxWidth() == to.MaxWidth() {
		return length
	}
	scaled := uint64(length) / uint64(from.MaxWidth()) * uint64(to.MaxWidth())
	return uint32(min(scaled, math.MaxUint32))
}

// ToConnection returns the conversion of a result that MySQL sent with
// character_set_results = NULL to the connection collation conn.
//
// The text of every column is converted from the character set of the
// collation in its field to the character set of conn, and the column length
// is the one of the converted column. When relabel is true, the field also
// gets conn as its collation, and the result is the one MySQL sends with
// character_set_results set to conn. When relabel is false, the field keeps
// the collation of the column.
func ToConnection(fields []*querypb.Field, conn collations.ID, relabel bool) *Conversion {
	c := &Conversion{}
	to := lookupCharset(conn)
	if to == nil {
		return c
	}

	for i, field := range fields {
		if !isText(field.Type) {
			continue
		}
		from := lookupCharset(collations.ID(field.Charset))
		if from == nil {
			continue
		}

		converted := field
		newField := func() *querypb.Field {
			if converted == field {
				converted = field.CloneVT()
			}
			return converted
		}
		if length := scaleLength(field.ColumnLength, from, to); length != field.ColumnLength {
			newField().ColumnLength = length
		}
		if relabel && field.Charset != uint32(conn) {
			newField().Charset = uint32(conn)
		}
		if converted != field {
			c.setField(fields, i, converted)
		}
		if !to.IsSuperset(from) {
			c.setColumn(len(fields), i, from, to)
		}
	}
	return c
}

// ToClient returns the conversion of a result with all text in the character
// set of the connection collation conn to the character_set_results of a
// client.
//
// results is the collation of character_set_results. The text of every
// column, and the names in every field, are converted to the character set of
// results, and every text field gets results as its collation, as MySQL
// reports it.
//
// null is true for character_set_results = NULL. The text of every column is
// then converted to the character set of the collation in its field, and the
// fields keep their collation and their names.
//
// MySQL sends JSON as text and converts it with the rest, with the binary
// collation in its field. This conversion does the same.
func ToClient(fields []*querypb.Field, conn, results collations.ID, null bool) *Conversion {
	c := &Conversion{}
	from := lookupCharset(conn)
	if from == nil {
		return c
	}
	target := lookupCharset(results)
	if !null && target == nil {
		return c
	}

	for i, field := range fields {
		converted := field
		newField := func() *querypb.Field {
			if converted == field {
				converted = field.CloneVT()
			}
			return converted
		}

		to := target
		switch {
		case field.Type == sqltypes.TypeJSON:
			// The field of a JSON column keeps the binary collation
			// and its length. With NULL the text stays as it is.
			if null {
				continue
			}
		case !isText(field.Type):
			to = nil
		case null:
			to = lookupCharset(collations.ID(field.Charset))
		default:
			if length := scaleLength(field.ColumnLength, from, to); length != field.ColumnLength {
				newField().ColumnLength = length
			}
			if field.Charset != uint32(results) {
				newField().Charset = uint32(results)
			}
		}

		if !null && !target.IsSuperset(from) {
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
			converted, _ := charset.Convert(nil, col.to, raw, col.from)
			if newRow == nil {
				newRow = make(sqltypes.Row, len(row))
				copy(newRow, row)
			}
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
