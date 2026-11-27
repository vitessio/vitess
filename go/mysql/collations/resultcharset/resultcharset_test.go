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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// The collation IDs of MySQL that the tests use.
const (
	latin1SwedishCI  = collations.ID(8)
	gb2312ChineseCI  = collations.ID(24)
	latin1German1CI  = collations.ID(5)
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

// rawResult is a row of a table with a latin1, a gb2312 and a utf8mb4 column as
// MySQL 8.4.13 and 8.0.46 send it with character_set_results = NULL: "Müller"
// is 4D FC 6C 6C 65 72 in the latin1 column, with the collation and the
// length of the column in the field.
func rawResult() *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			field("l", sqltypes.VarChar, latin1German1CI, 10),
			field("g", sqltypes.VarChar, gb2312ChineseCI, 20),
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
			assert.Equal(t, want[i], meta{collations.ID(f.Charset), f.ColumnLength}, "field %s", f.Name)
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
		assert.Equal(t, uint32(latin1German1CI), got.Fields[0].Charset)
		assert.Equal(t, uint32(40), got.Fields[0].ColumnLength)
		assert.Equal(t, uint32(gb2312ChineseCI), got.Fields[1].Charset)
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

// connectionResult is a result with all text in utf8mb4, as vtgate holds it.
func connectionResult() *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "né", OrgName: "né", Table: "t", Type: sqltypes.VarChar, Charset: uint32(latin1German1CI), ColumnLength: 40},
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

// TestToClient converts a result to the character_set_results of a client.
// The expected bytes are the ones MySQL 8.4.13 and 8.0.46 send: with latin1,
// "é€" is E9 80, "中" is "?", and the JSON text is converted while its field
// keeps the binary collation.
func TestToClient(t *testing.T) {
	t.Run("latin1", func(t *testing.T) {
		qr := connectionResult()
		got := ToClient(qr.Fields, utf8mb40900AICI, latin1SwedishCI, false).Result(qr)

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

	t.Run("gb2312", func(t *testing.T) {
		qr := connectionResult()
		got := ToClient(qr.Fields, utf8mb40900AICI, gb2312ChineseCI, false).Result(qr)
		// gb2312 has "ü" (A8 B9), "é" (A8 A6) and "中" (D6 D0), and no "€".
		assert.Equal(t, "M\xA8\xB9ller", got.Rows[0][0].ToString())
		assert.Equal(t, "\xA8\xA6?\xD6\xD0", got.Rows[0][1].ToString())
		assert.Equal(t, uint32(20), got.Fields[0].ColumnLength)
		assert.Equal(t, uint32(gb2312ChineseCI), got.Fields[0].Charset)
	})

	t.Run("ucs2", func(t *testing.T) {
		qr := connectionResult()
		got := ToClient(qr.Fields, utf8mb40900AICI, ucs2GeneralCI, false).Result(qr)
		assert.Equal(t, "\x00M\x00\xFC\x00l\x00l\x00e\x00r", got.Rows[0][0].ToString())
		// MySQL does not convert a number.
		assert.Equal(t, "1", got.Rows[0][3].ToString())
		assert.Equal(t, "\x00n\x00\xE9", got.Fields[0].Name)
		assert.Equal(t, "\x00t", got.Fields[0].Table)
		assert.Equal(t, uint32(20), got.Fields[0].ColumnLength)
	})

	t.Run("another collation of the connection character set", func(t *testing.T) {
		qr := connectionResult()
		conv := ToClient(qr.Fields, utf8mb40900AICI, utf8mb4Bin, false)
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
		conv := ToClient(qr.Fields, utf8mb40900AICI, utf8mb40900AICI, false)
		assert.True(t, conv.IsNoOp())
		assert.Same(t, qr, conv.Result(qr))
	})

	t.Run("NULL", func(t *testing.T) {
		qr := connectionResult()
		got := ToClient(qr.Fields, utf8mb40900AICI, collations.Unknown, true).Result(qr)
		// Each column is in the character set of its own collation.
		assert.Equal(t, "M\xFCller", got.Rows[0][0].ToString())
		assert.Equal(t, "é€中", got.Rows[0][1].ToString())
		assert.Equal(t, `{"a": "é"}`, got.Rows[0][2].ToString())
		assert.Equal(t, uint32(latin1German1CI), got.Fields[0].Charset)
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

// TestToConnectionUnimplementedCollation converts the text of columns whose
// collation Vitess does not implement, in the character set of the
// collations that Vitess implements for it. MySQL 8.4.13 sends a
// latin1_german2_ci VARCHAR(10) and a gb18030_chinese_ci VARCHAR(10) with
// 'Müller' and '中' as 4D FC 6C 6C 65 72 (collation 31, length 10) and D6 D0
// (collation 248, length 40) with character_set_results = NULL, and as
// 4D C3 BC 6C 6C 65 72 and E4 B8 AD (collation 255, length 40) with
// character_set_results = utf8mb4.
func TestToConnectionUnimplementedCollation(t *testing.T) {
	raw := &sqltypes.Result{
		Fields: []*querypb.Field{
			field("l", sqltypes.VarChar, collations.ID(31), 10),
			field("g", sqltypes.VarChar, collations.ID(248), 40),
		},
		Rows: []sqltypes.Row{{
			sqltypes.MakeTrusted(sqltypes.VarChar, []byte("M\xFCller")),
			sqltypes.MakeTrusted(sqltypes.VarChar, []byte("\xD6\xD0")),
		}},
	}
	got := ToConnection(raw.Fields, utf8mb40900AICI, true).Result(raw)
	assert.Equal(t, field("l", sqltypes.VarChar, utf8mb40900AICI, 40), got.Fields[0])
	assert.Equal(t, field("g", sqltypes.VarChar, utf8mb40900AICI, 40), got.Fields[1])
	require.Len(t, got.Rows, 1)
	assert.Equal(t, "Müller", got.Rows[0][0].ToString())
	assert.Equal(t, "中", got.Rows[0][1].ToString())
}

// TestCheckFields checks which collations CheckFields refuses: those of the
// character sets that Vitess does not implement, gbk (28), big5 (1) and
// tis620 (18), and latin2_czech_cs (2). It accepts every other collation,
// whether or not Vitess implements it: latin1_german2_ci (31),
// gb18030_chinese_ci (248), utf8mb3_tolower_ci (76),
// utf8mb3_general_mysql500_ci (223) and cp1250_czech_cs (34) have the
// character set of other collations.
func TestCheckFields(t *testing.T) {
	for _, id := range []collations.ID{collations.Unknown, binaryCollation, latin1SwedishCI, utf8mb40900AICI, 31, 248, 76, 223, 34} {
		require.NoError(t, CheckFields([]*querypb.Field{field("c", sqltypes.VarChar, id, 10)}), "collation %d", id)
	}
	for _, tc := range []struct {
		id      collations.ID
		charset string
	}{
		{28, "gbk"},
		{1, "big5"},
		{18, "tis620"},
		{2, "latin2 with the collation latin2_czech_cs"},
	} {
		err := CheckFields([]*querypb.Field{
			field("ok", sqltypes.VarChar, utf8mb40900AICI, 40),
			field("c", sqltypes.VarChar, tc.id, 10),
		})
		require.Error(t, err, "collation %d", tc.id)
		assert.Equal(t, vtrpcpb.Code_UNIMPLEMENTED, vterrors.Code(err))
		assert.EqualError(t, err, "cannot convert the text of column 'c': Vitess does not implement the character set "+tc.charset)
	}
}
