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

package resultcharset

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// The collation IDs of MySQL that the tests use.
const (
	latin1SwedishCI  = collations.ID(8)
	gbkChineseCI     = collations.ID(28)
	latin1German2CI  = collations.ID(31)
	ucs2GeneralCI    = collations.ID(35)
	utf8mb4Bin       = collations.ID(46)
	utf8mb40900AICI  = collations.ID(255)
	binaryCollation  = collations.CollationBinaryID
	utf8mb3GeneralCI = collations.ID(33)
)

func field(name string, typ querypb.Type, collation collations.ID, length uint32) *querypb.Field {
	return &querypb.Field{Name: name, Type: typ, Charset: uint32(collation), ColumnLength: length}
}

func row(values ...string) sqltypes.Row {
	out := make(sqltypes.Row, len(values))
	for i, v := range values {
		out[i] = sqltypes.MakeTrusted(sqltypes.VarChar, []byte(v))
	}
	return out
}

// rawResult is a row of a table with a latin1, a gbk and a utf8mb4 column as
// MySQL 8.4.13 and 8.0.46 send it with character_set_results = NULL: "Müller"
// is 4D FC 6C 6C 65 72 in the latin1 column, with the collation and the
// length of the column in the field.
func rawResult() *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			field("l", sqltypes.VarChar, latin1German2CI, 10),
			field("g", sqltypes.VarChar, gbkChineseCI, 20),
			field("u", sqltypes.VarChar, utf8mb40900AICI, 40),
			field("n", sqltypes.Int32, binaryCollation, 11),
			field("b", sqltypes.VarBinary, binaryCollation, 4),
			field("tx", sqltypes.Text, latin1SwedishCI, 65535),
			field("e", sqltypes.Enum, latin1SwedishCI, 2),
		},
		Rows: []sqltypes.Row{{
			sqltypes.MakeTrusted(sqltypes.VarChar, []byte("M\xFCller")),
			sqltypes.MakeTrusted(sqltypes.VarChar, []byte("\xD6\xD0")),
			sqltypes.MakeTrusted(sqltypes.VarChar, []byte("é€")),
			sqltypes.NewInt32(1),
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("\xFF")),
			sqltypes.MakeTrusted(sqltypes.Text, []byte("\xFC")),
			sqltypes.MakeTrusted(sqltypes.Enum, []byte("\xE9")),
		}},
		RowsAffected: 7,
	}
}

// TestToConnection converts a result of character_set_results = NULL to what
// MySQL sends for the same query with character_set_results = utf8mb4: the
// latin1 VARCHAR(10) has the length 40 and the collation of the connection.
func TestToConnection(t *testing.T) {
	raw := rawResult()

	t.Run("relabel", func(t *testing.T) {
		conv := ToConnection(raw.Fields, utf8mb40900AICI, true)
		require.False(t, conv.IsNoOp())
		got := conv.Result(raw)

		type meta struct {
			collation collations.ID
			length    uint32
		}
		want := []meta{
			{utf8mb40900AICI, 40},
			{utf8mb40900AICI, 40},
			{utf8mb40900AICI, 40},
			{binaryCollation, 11},
			{binaryCollation, 4},
			{utf8mb40900AICI, 262140},
			{utf8mb40900AICI, 8},
		}
		for i, f := range got.Fields {
			got := meta{collations.ID(f.Charset), f.ColumnLength}
			assert.Equal(t, want[i], got, "field %s", f.Name)
		}

		assert.Equal(t, "Müller", got.Rows[0][0].ToString())
		assert.Equal(t, "中", got.Rows[0][1].ToString())
		assert.Equal(t, "é€", got.Rows[0][2].ToString())
		assert.Equal(t, "1", got.Rows[0][3].ToString())
		assert.Equal(t, "\xFF", got.Rows[0][4].ToString())
		assert.Equal(t, "ü", got.Rows[0][5].ToString())
		assert.Equal(t, "é", got.Rows[0][6].ToString())
		assert.Equal(t, uint64(7), got.RowsAffected)

		// The result it was given is as it was.
		assert.Equal(t, rawResult(), raw)
	})

	t.Run("keep the collation of the column", func(t *testing.T) {
		got := ToConnection(raw.Fields, utf8mb40900AICI, false).Result(raw)
		assert.Equal(t, uint32(latin1German2CI), got.Fields[0].Charset)
		assert.Equal(t, uint32(40), got.Fields[0].ColumnLength)
		assert.Equal(t, uint32(gbkChineseCI), got.Fields[1].Charset)
		assert.Equal(t, "Müller", got.Rows[0][0].ToString())
		assert.Equal(t, "中", got.Rows[0][1].ToString())
	})

	t.Run("nothing to convert", func(t *testing.T) {
		qr := &sqltypes.Result{
			Fields: []*querypb.Field{field("u", sqltypes.VarChar, utf8mb40900AICI, 40), field("n", sqltypes.Int32, binaryCollation, 11)},
			Rows:   []sqltypes.Row{{sqltypes.NewVarChar("é"), sqltypes.NewInt32(1)}},
		}
		conv := ToConnection(qr.Fields, utf8mb40900AICI, true)
		assert.True(t, conv.IsNoOp())
		assert.Same(t, qr, conv.Result(qr))
	})

	t.Run("utf8mb3 text needs no conversion", func(t *testing.T) {
		qr := &sqltypes.Result{
			Fields: []*querypb.Field{field("u", sqltypes.VarChar, utf8mb3GeneralCI, 30)},
			Rows:   []sqltypes.Row{row("é")},
		}
		got := ToConnection(qr.Fields, utf8mb40900AICI, true).Result(qr)
		assert.Equal(t, uint32(utf8mb40900AICI), got.Fields[0].Charset)
		assert.Equal(t, uint32(40), got.Fields[0].ColumnLength)
		// The rows are the rows it was given.
		assert.Same(t, &qr.Rows[0], &got.Rows[0])
	})

	t.Run("a character that the connection character set does not have", func(t *testing.T) {
		qr := &sqltypes.Result{
			Fields: []*querypb.Field{field("u", sqltypes.VarChar, utf8mb40900AICI, 40)},
			Rows:   []sqltypes.Row{row("a中é")},
		}
		got := ToConnection(qr.Fields, latin1SwedishCI, true).Result(qr)
		assert.Equal(t, "a?\xE9", got.Rows[0][0].ToString())
		assert.Equal(t, uint32(10), got.Fields[0].ColumnLength)
	})
}

// TestToConnectionMeasured converts a row as MySQL 8.0.46 and 8.4.13 send it
// with character_set_results = NULL, and compares it with what they send for
// the same row with character_set_results = utf8mb4, field by field. The table
// is
//
//	create table t (lb varchar(10) charset latin1 collate latin1_bin,
//	  g text charset gbk, u text charset utf8mb4, uc text charset ucs2,
//	  ll longtext charset latin1, w varchar(10) charset swe7,
//	  s varchar(5) charset ucs2, j json)
//
// with ('Müller', '中', 'é', 'Ü', 'x', 'Ä', _ucs2 x'D800', '{"a": "Müller"}'),
// and the query is select lb, g, u, uc, ll, w, s, j, j->'$.a' from t.
func TestToConnectionMeasured(t *testing.T) {
	const (
		latin1Bin     = collations.ID(47)
		swe7SwedishCI = collations.ID(10)
	)
	trusted := func(typ querypb.Type, hex string) sqltypes.Value {
		return sqltypes.MakeTrusted(typ, []byte(hex))
	}
	flagged := func(f *querypb.Field, flags uint32, decimals uint32) *querypb.Field {
		f.Flags, f.Decimals = flags, decimals
		return f
	}
	raw := &sqltypes.Result{
		Fields: []*querypb.Field{
			flagged(field("lb", sqltypes.VarBinary, latin1Bin, 10), 128, 0),
			flagged(field("g", sqltypes.Text, gbkChineseCI, 65535), 16, 0),
			flagged(field("u", sqltypes.Text, utf8mb40900AICI, 65535), 16, 0),
			flagged(field("uc", sqltypes.Text, ucs2GeneralCI, 65535), 16, 0),
			flagged(field("ll", sqltypes.Text, latin1SwedishCI, 4294967295), 16, 0),
			flagged(field("w", sqltypes.VarChar, swe7SwedishCI, 10), 0, 0),
			flagged(field("s", sqltypes.VarChar, ucs2GeneralCI, 10), 0, 0),
			flagged(field("j", sqltypes.TypeJSON, binaryCollation, 4294967295), 144, 0),
			flagged(field("j->'$.a'", sqltypes.TypeJSON, utf8mb4Bin, 4294967295), 128, 31),
		},
		Rows: []sqltypes.Row{{
			trusted(sqltypes.VarBinary, "M\xFCller"),
			trusted(sqltypes.Text, "\xD6\xD0"),
			trusted(sqltypes.Text, "\xC3\xA9"),
			trusted(sqltypes.Text, "\x00\xDC"),
			trusted(sqltypes.Text, "x"),
			trusted(sqltypes.VarChar, "\x5B"),
			trusted(sqltypes.VarChar, "\xD8\x00"),
			trusted(sqltypes.TypeJSON, `{"a": "M`+"\xC3\xBC"+`ller"}`),
			trusted(sqltypes.TypeJSON, `"M`+"\xC3\xBC"+`ller"`),
		}},
	}
	want := &sqltypes.Result{
		Fields: []*querypb.Field{
			flagged(field("lb", sqltypes.VarBinary, utf8mb40900AICI, 40), 128, 0),
			flagged(field("g", sqltypes.Text, utf8mb40900AICI, 262140), 16, 0),
			flagged(field("u", sqltypes.Text, utf8mb40900AICI, 262140), 16, 0),
			flagged(field("uc", sqltypes.Text, utf8mb40900AICI, 131068), 16, 0),
			flagged(field("ll", sqltypes.Text, utf8mb40900AICI, 4294967295), 16, 0),
			flagged(field("w", sqltypes.VarChar, utf8mb40900AICI, 40), 0, 0),
			flagged(field("s", sqltypes.VarChar, utf8mb40900AICI, 20), 0, 0),
			flagged(field("j", sqltypes.TypeJSON, binaryCollation, 4294967295), 144, 0),
			flagged(field("j->'$.a'", sqltypes.TypeJSON, utf8mb40900AICI, 4294967292), 128, 31),
		},
		Rows: []sqltypes.Row{{
			trusted(sqltypes.VarBinary, "M\xC3\xBCller"),
			trusted(sqltypes.Text, "\xE4\xB8\xAD"),
			trusted(sqltypes.Text, "\xC3\xA9"),
			trusted(sqltypes.Text, "\xC3\x9C"),
			trusted(sqltypes.Text, "x"),
			trusted(sqltypes.VarChar, "\xC3\x84"),
			trusted(sqltypes.VarChar, "\xED\xA0\x80"),
			trusted(sqltypes.TypeJSON, `{"a": "M`+"\xC3\xBC"+`ller"}`),
			trusted(sqltypes.TypeJSON, `"M`+"\xC3\xBC"+`ller"`),
		}},
	}
	lengths := ConnectionLengths(raw.Fields, utf8mb40900AICI).Result(raw)
	got := ToConnection(lengths.Fields, utf8mb40900AICI, true).Result(lengths)
	for i := range want.Fields {
		assert.Equal(t, want.Fields[i], got.Fields[i], "field %s", want.Fields[i].Name)
		assert.Equal(t, want.Rows[0][i].Raw(), got.Rows[0][i].Raw(), "value of %s", want.Fields[i].Name)
	}

	// ConnectionLengths gives the utf8mb4 columns their length and changes
	// nothing else; ToConnection alone leaves their length as it is.
	assert.Equal(t, uint32(262140), lengths.Fields[2].ColumnLength)
	assert.Equal(t, uint32(4294967292), lengths.Fields[8].ColumnLength)
	assert.Equal(t, raw.Fields[0], lengths.Fields[0])
	assert.Equal(t, raw.Rows, lengths.Rows)
	alone := ToConnection(raw.Fields, utf8mb40900AICI, true).Result(raw)
	assert.Equal(t, uint32(65535), alone.Fields[2].ColumnLength)
	assert.Equal(t, uint32(4294967295), alone.Fields[8].ColumnLength)

	// What MySQL sends with character_set_results = utf8mb4, which is what a
	// vttablet without character_set_results = NULL returns, does not change.
	conv := ToConnection(want.Fields, utf8mb40900AICI, true)
	assert.True(t, conv.IsNoOp())

	// With character_set_results = latin1 MySQL converts the text of the
	// JSON column and the names of the fields as well. For
	// select j, lb as `Ü`, g from t on a latin1 connection the name is
	// C3 83 C5 93 with NULL, the latin1 text "Ãœ" in utf8mb3, and C3 9C
	// with latin1.
	raw = &sqltypes.Result{
		Fields: []*querypb.Field{
			field("j", sqltypes.TypeJSON, binaryCollation, 4294967295),
			field("\xC3\x83\xC5\x93", sqltypes.VarBinary, latin1Bin, 10),
			field("g", sqltypes.Text, gbkChineseCI, 65535),
		},
		Rows: []sqltypes.Row{{
			trusted(sqltypes.TypeJSON, `{"a": "M`+"\xC3\xBC"+`ller"}`),
			trusted(sqltypes.VarBinary, "M\xFCller"),
			trusted(sqltypes.Text, "\xD6\xD0"),
		}},
	}
	got = ToConnection(raw.Fields, latin1SwedishCI, true).Result(raw)
	assert.Equal(t, field("j", sqltypes.TypeJSON, binaryCollation, 4294967295), got.Fields[0])
	assert.Equal(t, field("\xC3\x9C", sqltypes.VarBinary, latin1SwedishCI, 10), got.Fields[1])
	assert.Equal(t, field("g", sqltypes.Text, latin1SwedishCI, 65535), got.Fields[2])
	assert.Equal(t, `{"a": "M`+"\xFC"+`ller"}`, got.Rows[0][0].ToString())
	assert.Equal(t, "M\xFCller", got.Rows[0][1].ToString())
	assert.Equal(t, "?", got.Rows[0][2].ToString())
}

// connectionResult is a result with all text in utf8mb4, as vtgate holds it.
func connectionResult() *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "né", OrgName: "né", Table: "t", Type: sqltypes.VarChar, Charset: uint32(latin1German2CI), ColumnLength: 40},
			field("u", sqltypes.VarChar, utf8mb40900AICI, 40),
			field("j", sqltypes.TypeJSON, binaryCollation, 4294967295),
			field("n", sqltypes.Int32, binaryCollation, 11),
			field("b", sqltypes.VarBinary, binaryCollation, 4),
		},
		Rows: []sqltypes.Row{{
			sqltypes.NewVarChar("Müller"),
			sqltypes.NewVarChar("é€中"),
			sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`{"a": "é"}`)),
			sqltypes.NewInt32(1),
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("\xC3\xA9")),
		}},
	}
}

// TestFromConnection converts a result with all text in the connection
// character set to the character_set_results of a client.
// The expected bytes are the ones MySQL 8.4.13 and 8.0.46 send: with latin1,
// "é€" is E9 80, "中" is "?", and the JSON text is converted while its field
// keeps the binary collation.
func TestFromConnection(t *testing.T) {
	t.Run("latin1", func(t *testing.T) {
		qr := connectionResult()
		got := fromConnection(qr.Fields, utf8mb40900AICI, latin1SwedishCI, false).Result(qr)

		assert.Equal(t, "M\xFCller", got.Rows[0][0].ToString())
		assert.Equal(t, "\xE9\x80?", got.Rows[0][1].ToString())
		assert.Equal(t, "{\"a\": \"\xE9\"}", got.Rows[0][2].ToString())
		assert.Equal(t, "1", got.Rows[0][3].ToString())
		assert.Equal(t, "\xC3\xA9", got.Rows[0][4].ToString())

		assert.Equal(t, "n\xE9", got.Fields[0].Name)
		assert.Equal(t, "n\xE9", got.Fields[0].OrgName)
		assert.Equal(t, "t", got.Fields[0].Table)
		assert.Equal(t, uint32(latin1SwedishCI), got.Fields[0].Charset)
		assert.Equal(t, uint32(10), got.Fields[0].ColumnLength)
		assert.Equal(t, uint32(latin1SwedishCI), got.Fields[1].Charset)
		assert.Equal(t, uint32(binaryCollation), got.Fields[2].Charset)
		assert.Equal(t, uint32(4294967295), got.Fields[2].ColumnLength)
		assert.Equal(t, uint32(binaryCollation), got.Fields[4].Charset)

		assert.Equal(t, connectionResult(), qr)
	})

	t.Run("gbk", func(t *testing.T) {
		qr := connectionResult()
		got := fromConnection(qr.Fields, utf8mb40900AICI, gbkChineseCI, false).Result(qr)
		// gbk has "ü" (A8 B9), "é" (A8 A6) and "中" (D6 D0), and no "€".
		assert.Equal(t, "M\xA8\xB9ller", got.Rows[0][0].ToString())
		assert.Equal(t, "\xA8\xA6?\xD6\xD0", got.Rows[0][1].ToString())
		assert.Equal(t, uint32(20), got.Fields[0].ColumnLength)
		assert.Equal(t, uint32(gbkChineseCI), got.Fields[0].Charset)
	})

	t.Run("ucs2", func(t *testing.T) {
		qr := connectionResult()
		got := fromConnection(qr.Fields, utf8mb40900AICI, ucs2GeneralCI, false).Result(qr)
		assert.Equal(t, "\x00M\x00\xFC\x00l\x00l\x00e\x00r", got.Rows[0][0].ToString())
		// MySQL does not convert a number.
		assert.Equal(t, "1", got.Rows[0][3].ToString())
		assert.Equal(t, "\x00n\x00\xE9", got.Fields[0].Name)
		assert.Equal(t, "\x00t", got.Fields[0].Table)
		assert.Equal(t, uint32(20), got.Fields[0].ColumnLength)
	})

	t.Run("another collation of the connection character set", func(t *testing.T) {
		qr := connectionResult()
		conv := fromConnection(qr.Fields, utf8mb40900AICI, utf8mb4Bin, false)
		got := conv.Result(qr)
		assert.Equal(t, uint32(utf8mb4Bin), got.Fields[0].Charset)
		assert.Equal(t, uint32(utf8mb4Bin), got.Fields[1].Charset)
		assert.Equal(t, uint32(40), got.Fields[0].ColumnLength)
		assert.Equal(t, "né", got.Fields[0].Name)
		// The rows are the rows it was given.
		assert.Same(t, &qr.Rows[0], &got.Rows[0])
	})

	t.Run("the collation of the connection", func(t *testing.T) {
		qr := &sqltypes.Result{
			Fields: []*querypb.Field{field("u", sqltypes.VarChar, utf8mb40900AICI, 40)},
			Rows:   []sqltypes.Row{row("é")},
		}
		conv := fromConnection(qr.Fields, utf8mb40900AICI, utf8mb40900AICI, false)
		assert.True(t, conv.IsNoOp())
		assert.Same(t, qr, conv.Result(qr))
	})

	t.Run("NULL", func(t *testing.T) {
		qr := connectionResult()
		got := fromConnection(qr.Fields, utf8mb40900AICI, collations.Unknown, true).Result(qr)
		// Each column is in the character set of its own collation.
		assert.Equal(t, "M\xFCller", got.Rows[0][0].ToString())
		assert.Equal(t, "é€中", got.Rows[0][1].ToString())
		assert.Equal(t, `{"a": "é"}`, got.Rows[0][2].ToString())
		assert.Equal(t, uint32(latin1German2CI), got.Fields[0].Charset)
		assert.Equal(t, "né", got.Fields[0].Name)
	})
}

// TestStream converts a streamed result, which has its fields in the first
// packet only.
func TestStream(t *testing.T) {
	stream := NewStream(func(fields []*querypb.Field) *Conversion {
		return ToConnection(fields, utf8mb40900AICI, true)
	})

	first := stream.Result(&sqltypes.Result{Fields: []*querypb.Field{field("l", sqltypes.VarChar, latin1SwedishCI, 10)}})
	assert.Equal(t, uint32(utf8mb40900AICI), first.Fields[0].Charset)

	second := stream.Result(&sqltypes.Result{Rows: []sqltypes.Row{row("M\xFCller")}})
	assert.Nil(t, second.Fields)
	assert.Equal(t, "Müller", second.Rows[0][0].ToString())
}

// TestFromConnectionColumns covers the columns whose conversion depends on more
// than their type. The fields have the text in utf8mb4
// and the length that MySQL reports for utf8mb4, and the expected values are
// what MySQL 8.4.13 and 8.0.46 send with character_set_results = latin1.
func TestFromConnectionColumns(t *testing.T) {
	const decimalFlags = uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_BINARY_FLAG)
	qr := &sqltypes.Result{
		Fields: []*querypb.Field{
			// u4 COLLATE utf8mb4_bin is VARBINARY in Vitess, and text in MySQL.
			{Name: "b", Type: sqltypes.VarBinary, Charset: uint32(utf8mb4Bin), ColumnLength: 40, Flags: uint32(querypb.MySqlFlag_BINARY_FLAG)},
			// CAST(u4 AS BINARY)
			{Name: "cb", Type: sqltypes.VarBinary, Charset: uint32(binaryCollation), ColumnLength: 40, Flags: uint32(querypb.MySqlFlag_BINARY_FLAG)},
			// JSON_OBJECT('k', u4) has a text collation.
			{Name: "jo", Type: sqltypes.TypeJSON, Charset: uint32(utf8mb40900AICI), ColumnLength: 4294967292},
			// A utf8mb4 TEXT column.
			field("tx", sqltypes.Text, utf8mb40900AICI, 262140),
			// A LONGTEXT column.
			field("lt", sqltypes.Text, utf8mb40900AICI, 4294967295),
			// 1.5
			{Name: "d", Type: sqltypes.Decimal, Charset: uint32(binaryCollation), ColumnLength: 4, Flags: decimalFlags},
		},
		Rows: []sqltypes.Row{{
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("é€😀")),
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("é")),
			sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`{"k": "é€😀"}`)),
			sqltypes.MakeTrusted(sqltypes.Text, []byte("Grüße")),
			sqltypes.MakeTrusted(sqltypes.Text, []byte("x")),
			sqltypes.MakeTrusted(sqltypes.Decimal, []byte("1.5")),
		}},
	}

	type meta struct {
		collation collations.ID
		length    uint32
	}
	fieldsOf := func(got *sqltypes.Result) []meta {
		var out []meta
		for _, f := range got.Fields {
			out = append(out, meta{collations.ID(f.Charset), f.ColumnLength})
		}
		return out
	}

	t.Run("latin1", func(t *testing.T) {
		got := fromConnection(qr.Fields, utf8mb40900AICI, latin1SwedishCI, false).Result(qr)
		assert.Equal(t, []meta{
			{latin1SwedishCI, 10},
			{binaryCollation, 40},
			{latin1SwedishCI, 1073741823},
			{latin1SwedishCI, 65535},
			{latin1SwedishCI, 4294967295},
			{binaryCollation, 4},
		}, fieldsOf(got))
		assert.Equal(t, "\xE9\x80?", got.Rows[0][0].ToString())
		assert.Equal(t, "é", got.Rows[0][1].ToString())
		assert.Equal(t, "{\"k\": \"\xE9\x80?\"}", got.Rows[0][2].ToString())
		assert.Equal(t, "Gr\xFC\xDFe", got.Rows[0][3].ToString())
		assert.Equal(t, "1.5", got.Rows[0][5].ToString())
	})

	t.Run("ucs2", func(t *testing.T) {
		got := fromConnection(qr.Fields, utf8mb40900AICI, ucs2GeneralCI, false).Result(qr)
		// MySQL converts the text of a DECIMAL expression.
		assert.Equal(t, "\x001\x00.\x005", got.Rows[0][5].ToString())
		assert.Equal(t, uint32(binaryCollation), got.Fields[5].Charset)

		// A DECIMAL column has no binary flag, and MySQL does not convert it.
		column := &sqltypes.Result{
			Fields: []*querypb.Field{{Name: "dc", Type: sqltypes.Decimal, Charset: uint32(binaryCollation), ColumnLength: 8, Flags: uint32(querypb.MySqlFlag_NUM_FLAG)}},
			Rows:   []sqltypes.Row{{sqltypes.MakeTrusted(sqltypes.Decimal, []byte("1234.56"))}},
		}
		assert.Equal(t, "1234.56", fromConnection(column.Fields, utf8mb40900AICI, ucs2GeneralCI, false).Result(column).Rows[0][0].ToString())
	})

	t.Run("binary", func(t *testing.T) {
		// MySQL sends the text of every column in the character set of
		// the column, as for NULL, and gives the fields the binary
		// collation and one byte per character.
		raw := connectionResult()
		raw.Fields = append(raw.Fields, qr.Fields[2], qr.Fields[3])
		raw.Rows[0] = append(raw.Rows[0], qr.Rows[0][2], qr.Rows[0][3])
		got := fromConnection(raw.Fields, utf8mb40900AICI, binaryCollation, false).Result(raw)
		assert.Equal(t, []meta{
			{binaryCollation, 10},
			{binaryCollation, 10},
			{binaryCollation, 4294967295},
			{binaryCollation, 11},
			{binaryCollation, 4},
			{binaryCollation, 1073741823},
			{binaryCollation, 65535},
		}, fieldsOf(got))
		assert.Equal(t, "M\xFCller", got.Rows[0][0].ToString())
		assert.Equal(t, "é€中", got.Rows[0][1].ToString())
		assert.Equal(t, `{"a": "é"}`, got.Rows[0][2].ToString())
		assert.Equal(t, "né", got.Fields[0].Name)
	})

	t.Run("NULL", func(t *testing.T) {
		// The fields have the length of the column in its own character
		// set: 10 for a latin1 VARCHAR(10), 65535 for a utf8mb4 TEXT, and
		// 4294967295 for JSON_OBJECT().
		raw := connectionResult()
		raw.Fields = append(raw.Fields, qr.Fields[2], qr.Fields[3])
		raw.Rows[0] = append(raw.Rows[0], qr.Rows[0][2], qr.Rows[0][3])
		got := fromConnection(raw.Fields, utf8mb40900AICI, collations.Unknown, true).Result(raw)
		assert.Equal(t, []meta{
			{latin1German2CI, 10},
			{utf8mb40900AICI, 40},
			{binaryCollation, 4294967295},
			{binaryCollation, 11},
			{binaryCollation, 4},
			{utf8mb40900AICI, 4294967295},
			{utf8mb40900AICI, 65535},
		}, fieldsOf(got))
	})
}

// TestToClientMessage converts error messages as MySQL 8.4.13 and 8.0.46 send
// them.
func TestToClientMessage(t *testing.T) {
	const sjisJapaneseCI = collations.ID(13)
	tests := []struct {
		message string
		results collations.ID
		null    bool
		want    string
	}{
		{"Duplicate entry 'Müller' for key 'gate_m.uk_l'", gbkChineseCI, false, "Duplicate entry 'M\xA8\xB9ller' for key 'gate_m.uk_l'"},
		{"Duplicate entry 'Müller' for key 'gate_m.uk_l'", latin1SwedishCI, false, "Duplicate entry 'M\xFCller' for key 'gate_m.uk_l'"},
		{"Duplicate entry 'Müller' for key 'gate_m.uk_l'", sjisJapaneseCI, false, "Duplicate entry 'M\\00FCller' for key 'gate_m.uk_l'"},
		{"Table 't.nosuch_中€?' doesn't exist", latin1SwedishCI, false, "Table 't.nosuch_\\4E2D\x80?' doesn't exist"},
		{"Table 't.nosuch_中€?' doesn't exist", gbkChineseCI, false, "Table 't.nosuch_\xD6\xD0\\20AC?' doesn't exist"},
		{"Duplicate entry 'Müller' for key 'gate_m.uk_l'", collations.Unknown, true, "Duplicate entry 'Müller' for key 'gate_m.uk_l'"},
		{"Duplicate entry 'Müller' for key 'gate_m.uk_l'", binaryCollation, false, "Duplicate entry 'Müller' for key 'gate_m.uk_l'"},
		{"Duplicate entry 'Müller' for key 'gate_m.uk_l'", utf8mb3GeneralCI, false, "Duplicate entry 'Müller' for key 'gate_m.uk_l'"},
		// MySQL sends an empty message with ucs2.
		{"Table 't.nosuch' doesn't exist", ucs2GeneralCI, false, ""},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, ToClientMessage(tc.message, tc.results, tc.null), "%q in %d", tc.message, tc.results)
	}
}

// TestToClient converts a result as a tablet returns it, with the text of
// each column in the character set of the column, to the character_set_results
// of a client. The row is the one of a table with a latin1_german2_ci, a gbk
// and a utf8mb4 VARCHAR(10) with 'Müller', '中文' and 'é€😀' and a JSON column,
// and the expected fields and bytes are what MySQL 8.0.46 and 8.4.13 send for
// it with each character_set_results.
//
// A value that is in the character set the client asked for is not touched:
// with latin1 the latin1 column is the same bytes in the same memory, and with
// NULL and binary the whole row is.
func TestToClient(t *testing.T) {
	raw := func() *sqltypes.Result {
		return &sqltypes.Result{
			Fields: []*querypb.Field{
				{Name: "l", Type: sqltypes.VarChar, Charset: 31, ColumnLength: 10},
				{Name: "g", Type: sqltypes.VarChar, Charset: 28, ColumnLength: 20},
				{Name: "u4", Type: sqltypes.VarChar, Charset: 255, ColumnLength: 40},
				{Name: "j", Type: sqltypes.TypeJSON, Charset: 63, ColumnLength: 4294967295},
			},
			Rows: []sqltypes.Row{{
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("M\xFCller")),
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("\xD6\xD0\xCE\xC4")),
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("é€😀")),
				sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`{"a": "é€😀"}`)),
			}},
		}
	}
	type column struct {
		collation collations.ID
		length    uint32
		value     string
	}
	tests := []struct {
		name    string
		results collations.ID
		null    bool
		text    [3]column
		json    string
		// same lists the columns whose value is the one of the tablet, in
		// the same memory.
		same []int
	}{{
		name:    "utf8mb4",
		results: 255,
		text:    [3]column{{255, 40, "4dc3bc6c6c6572"}, {255, 40, "e4b8ade69687"}, {255, 40, "c3a9e282acf09f9880"}},
		json:    "7b2261223a2022c3a9e282acf09f9880227d",
		same:    []int{2, 3},
	}, {
		name:    "latin1",
		results: 8,
		text:    [3]column{{8, 10, "4dfc6c6c6572"}, {8, 10, "3f3f"}, {8, 10, "e9803f"}},
		json:    "7b2261223a2022e9803f227d",
		same:    []int{0},
	}, {
		name:    "gbk",
		results: 28,
		text:    [3]column{{28, 20, "4da8b96c6c6572"}, {28, 20, "d6d0cec4"}, {28, 20, "a8a63f3f"}},
		json:    "7b2261223a2022a8a63f3f227d",
		same:    []int{1},
	}, {
		name:    "sjis",
		results: 13,
		text:    [3]column{{13, 20, "4d3f6c6c6572"}, {13, 20, "928695b6"}, {13, 20, "3f3f3f"}},
		json:    "7b2261223a20223f3f3f227d",
	}, {
		name:    "ucs2",
		results: 35,
		text:    [3]column{{35, 20, "004d00fc006c006c00650072"}, {35, 20, "4e2d6587"}, {35, 20, "00e920ac003f"}},
		json:    "007b002200610022003a0020002200e920ac003f0022007d",
	}, {
		name:    "binary",
		results: binaryCollation,
		text:    [3]column{{63, 10, "4dfc6c6c6572"}, {63, 10, "d6d0cec4"}, {63, 10, "c3a9e282acf09f9880"}},
		json:    "7b2261223a2022c3a9e282acf09f9880227d",
		same:    []int{0, 1, 2, 3},
	}, {
		name: "NULL",
		null: true,
		text: [3]column{{31, 10, "4dfc6c6c6572"}, {28, 20, "d6d0cec4"}, {255, 40, "c3a9e282acf09f9880"}},
		json: "7b2261223a2022c3a9e282acf09f9880227d",
		same: []int{0, 1, 2, 3},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := raw()
			got := ToClient(in.Fields, utf8mb40900AICI, tc.results, tc.null).Result(in)
			require.Len(t, got.Rows, 1)
			for i, want := range tc.text {
				f := got.Fields[i]
				assert.Equal(t, uint32(want.collation), f.Charset, "collation of %s", f.Name)
				assert.Equal(t, want.length, f.ColumnLength, "length of %s", f.Name)
				assert.Equal(t, want.value, hex.EncodeToString(got.Rows[0][i].Raw()), "value of %s", f.Name)
			}
			assert.Equal(t, uint32(binaryCollation), got.Fields[3].Charset)
			assert.Equal(t, uint32(4294967295), got.Fields[3].ColumnLength)
			assert.Equal(t, tc.json, hex.EncodeToString(got.Rows[0][3].Raw()))
			for _, i := range tc.same {
				assert.Same(t, &in.Rows[0][i].Raw()[0], &got.Rows[0][i].Raw()[0], "column %d", i)
			}

			// The result of the tablet is not changed.
			assert.Equal(t, raw(), in)
		})
	}

	t.Run("a result that MySQL converted to the connection", func(t *testing.T) {
		in := &sqltypes.Result{
			Fields: []*querypb.Field{{Name: "l", Type: sqltypes.VarChar, Charset: 255, ColumnLength: 40}},
			Rows:   []sqltypes.Row{{sqltypes.NewVarChar("Müller")}},
		}
		got := ToClient(in.Fields, utf8mb40900AICI, gbkChineseCI, false).Result(in)
		assert.Equal(t, uint32(28), got.Fields[0].Charset)
		assert.Equal(t, uint32(20), got.Fields[0].ColumnLength)
		assert.Equal(t, "4da8b96c6c6572", hex.EncodeToString(got.Rows[0][0].Raw()))
		assert.True(t, ToClient(in.Fields, utf8mb40900AICI, utf8mb40900AICI, false).IsNoOp())
	})
}
