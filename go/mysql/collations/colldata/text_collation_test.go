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

package colldata

import (
	"encoding/hex"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/vt/vthash"
)

const (
	latin1German2CI collations.ID = 31
	gbkChineseCI    collations.ID = 28
	utf8mb3GeneralC collations.ID = 33
	utf8mb4GeneralC collations.ID = 45
	utf8mb40900AICI collations.ID = 255
	latin1SwedishCI collations.ID = 8
	swe7SwedishCI   collations.ID = 10
	ucs2GeneralCI   collations.ID = 35
)

// TestForTextComparesInTheCollation checks the collation of ForText with what
// MySQL 8.0.46 does with the column itself. For a latin1_german2_ci column
// with 'Müller', 4D FC 6C 6C 65 72:
//
//	l = 'Mueller'       1
//	l LIKE 'M_ller'     1
//	l LIKE 'mü%'        1, and 0 for 'MUELLER'
//	WEIGHT_STRING(l)    4D55454C4C4552
//
// and for its utf8mb4 bytes, 4D C3 BC 6C 6C 65 72, taken as latin1 text:
//
//	l = 'Mueller'       0
//	WEIGHT_STRING(l)    4D41BC4C4C4552
//
// The collation of ForText gets the utf8mb4 bytes, and has to answer as MySQL
// does for the column.
func TestForTextComparesInTheCollation(t *testing.T) {
	const mueller = "M\xc3\xbcller"

	plain := Lookup(latin1German2CI)
	require.NotNil(t, plain)
	// This is what the planned collation does with the text of the connection.
	assert.NotZero(t, plain.Collate([]byte(mueller), []byte("Mueller"), false))
	assert.Equal(t, "4D41BC4C4C4552", strings.ToUpper(hex.EncodeToString(plain.WeightString(nil, []byte(mueller), 0))))

	id := ForText(latin1German2CI, utf8mb40900AICI)
	require.NotEqual(t, latin1German2CI, id)
	coll := Lookup(id)
	require.NotNil(t, coll)
	assert.Equal(t, id, coll.ID())
	assert.Equal(t, "latin1_german2_ci", coll.Name())
	assert.Equal(t, "utf8mb4", coll.Charset().Name())

	assert.Zero(t, coll.Collate([]byte(mueller), []byte("Mueller"), false))
	assert.Zero(t, coll.Collate([]byte("Mueller"), []byte(mueller), false))
	assert.Negative(t, coll.Collate([]byte(mueller), []byte("Mz"), false))
	assert.Positive(t, coll.Collate([]byte(mueller), []byte("Mb"), false))
	assert.Zero(t, coll.Collate(nil, nil, false))
	assert.Equal(t, "4D55454C4C4552", strings.ToUpper(hex.EncodeToString(coll.WeightString(nil, []byte(mueller), 0))))

	hash := func(c Collation, text string) vthash.Hash {
		h := vthash.New()
		c.Hash(&h, []byte(text), 0)
		return h.Sum128()
	}
	assert.Equal(t, hash(coll, "MUELLER"), hash(coll, mueller))
	assert.Equal(t, hash(plain, "M\xfcller"), hash(coll, mueller))
	assert.NotEqual(t, hash(plain, "MUELLER"), hash(plain, mueller))

	assert.True(t, coll.Wildcard([]byte("M_ller"), 0, 0, 0).Match([]byte(mueller)))
	assert.False(t, plain.Wildcard([]byte("M_ller"), 0, 0, 0).Match([]byte(mueller)))
	// The pattern is text of the connection too. LIKE compares character by
	// character: 'mü%' matches 'Müller' and not 'MUELLER'.
	assert.True(t, coll.Wildcard([]byte("m\xc3\xbc%"), 0, 0, 0).Match([]byte(mueller)))
	assert.False(t, coll.Wildcard([]byte("m\xc3\xbc%"), 0, 0, 0).Match([]byte("MUELLER")))

	plainTiny, ok := plain.(TinyWeightCollation)
	require.True(t, ok)
	tiny, ok := coll.(TinyWeightCollation)
	require.True(t, ok)
	assert.Equal(t, plainTiny.TinyWeightString([]byte("M\xfcller")), tiny.TinyWeightString([]byte(mueller)))
	assert.NotEqual(t, plainTiny.TinyWeightString([]byte(mueller)), tiny.TinyWeightString([]byte(mueller)))

	// A character that latin1 does not have is "?", as it is in MySQL when
	// it converts the text to latin1.
	assert.Zero(t, coll.Collate([]byte("中"), []byte("?"), false))
}

// TestForTextOtherCharsets checks ForText between character sets of which
// neither is utf8mb4. In MySQL 8.0.46 '中' in gbk is D6D0 and '波' is B2A8, and
// in gbk_chinese_ci '波' sorts before '中', where the bytes of their utf8mb4
// text, E4 B8 AD and E6 B3 A2, sort the other way. 'Ä' in latin1 is C4 and
// equals 'a' in utf8mb4_0900_ai_ci.
func TestForTextOtherCharsets(t *testing.T) {
	gbk := Lookup(ForText(gbkChineseCI, utf8mb40900AICI))
	require.NotNil(t, gbk)
	assert.Negative(t, gbk.Collate([]byte("波"), []byte("中"), false))
	assert.Positive(t, Lookup(gbkChineseCI).Collate([]byte("波"), []byte("中"), false))

	// Rows in latin1 that are compared in a utf8mb4 collation.
	id := ForText(utf8mb40900AICI, latin1German2CI)
	require.NotEqual(t, utf8mb40900AICI, id)
	utf8 := Lookup(id)
	require.NotNil(t, utf8)
	assert.Equal(t, "latin1", utf8.Charset().Name())
	assert.Zero(t, utf8.Collate([]byte("\xc4"), []byte("a"), false))
	assert.NotZero(t, Lookup(utf8mb40900AICI).Collate([]byte("\xc4"), []byte("a"), false))

	// ASCII text is not the same bytes in ucs2.
	ucs2 := Lookup(ForText(ucs2GeneralCI, utf8mb40900AICI))
	require.NotNil(t, ucs2)
	assert.Zero(t, ucs2.Collate([]byte("abc"), []byte("ABC"), false))
	assert.Negative(t, ucs2.Collate([]byte("abc"), []byte("abd"), false))

	// In swe7 the byte 5B, "[" in ASCII, is "Ä", and "[" is a character
	// that swe7 does not have: MySQL converts it to "?".
	swe7 := Lookup(ForText(swe7SwedishCI, utf8mb40900AICI))
	require.NotNil(t, swe7)
	assert.Zero(t, swe7.Collate([]byte("Ä"), []byte("ä"), false))
	assert.NotZero(t, swe7.Collate([]byte("Ä"), []byte("["), false))
}

// TestForTextASCII checks ASCII text in a character set where it does not have
// the bytes of ASCII. In sjis MySQL 8.0.46 writes 'a\b' as 61 81 5F 62, the
// weight string of the backslash in sjis_japanese_ci is 81 5F, and 'a\b' does
// not sort before 'a]b', where the byte 5C does.
func TestForTextASCII(t *testing.T) {
	const sjisJapaneseCI collations.ID = 13
	plain := Lookup(sjisJapaneseCI)
	require.NotNil(t, plain)
	sjis := Lookup(ForText(sjisJapaneseCI, utf8mb40900AICI))
	require.NotNil(t, sjis)

	assert.Equal(t, "815F", strings.ToUpper(hex.EncodeToString(sjis.WeightString(nil, []byte(`\`), 0))))
	assert.Positive(t, sjis.Collate([]byte(`a\b`), []byte("a]b"), false))
	assert.Zero(t, sjis.Collate([]byte(`a\b`), []byte(`a\b`), false))
	// The bytes of the utf8mb4 text, given to sjis_japanese_ci as they are.
	assert.Negative(t, plain.Collate([]byte(`a\b`), []byte("a]b"), false))

	hash := func(c Collation, text string) vthash.Hash {
		h := vthash.New()
		c.Hash(&h, []byte(text), 0)
		return h.Sum128()
	}
	// A row that is in sjis and one that is in utf8mb4 are the same value.
	assert.Equal(t, hash(plain, "a\x81\x5fb"), hash(sjis, `a\b`))

	// Text without a backslash is not converted, and is compared the same.
	assert.Zero(t, sjis.Collate([]byte("abc"), []byte("ABC"), false))
}

// TestIsASCIIIdentical checks which character sets have ASCII text as the
// bytes of ASCII. MySQL 8.0.46 writes '\\' as 81 5F in sjis and as 5C in
// cp932, and 'Ä' as 5B in swe7.
func TestIsASCIIIdentical(t *testing.T) {
	tests := map[string]bool{
		"utf8mb4_0900_ai_ci": true,
		"utf8mb3_general_ci": true,
		"latin1_swedish_ci":  true,
		"gbk_chinese_ci":     true,
		"cp932_japanese_ci":  true,
		"sjis_japanese_ci":   false,
		"swe7_swedish_ci":    false,
		"ucs2_general_ci":    false,
		"utf16_general_ci":   false,
		"utf16le_general_ci": false,
		"utf32_general_ci":   false,
	}
	for name, want := range tests {
		coll := Lookup(collations.MySQL8().LookupByName(name))
		require.NotNil(t, coll, name)
		assert.Equal(t, want, charset.IsASCIIIdentical(coll.Charset()), name)
	}
}

// TestForTextKeepsTheCollation checks when ForText returns the collation that
// it was given: the text is in its character set already, or one of the two
// is the binary collation or a collation that Vitess does not have.
func TestForTextKeepsTheCollation(t *testing.T) {
	tests := []struct {
		name     string
		id, from collations.ID
	}{
		{"the same collation", latin1German2CI, latin1German2CI},
		{"another collation of the character set", latin1German2CI, latin1SwedishCI},
		{"utf8mb3 text in a utf8mb4 collation", utf8mb40900AICI, utf8mb3GeneralC},
		{"binary text", latin1German2CI, collations.CollationBinaryID},
		{"the binary collation", collations.CollationBinaryID, utf8mb40900AICI},
		{"no collation", collations.Unknown, utf8mb40900AICI},
		{"text of no collation", latin1German2CI, collations.Unknown},
		{"a collation that does not exist", 5000, utf8mb40900AICI},
		{"text of a collation that does not exist", latin1German2CI, 5000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.id, ForText(tt.id, tt.from))
			_, _, converts := TextConversion(tt.id, tt.from)
			assert.False(t, converts)
		})
	}

	// utf8mb4 text can have a character that utf8mb3 does not have.
	assert.NotEqual(t, utf8mb3GeneralC, ForText(utf8mb3GeneralC, utf8mb4GeneralC))

	// A collation of ForText is not wrapped again.
	id := ForText(latin1German2CI, utf8mb40900AICI)
	assert.Equal(t, id, ForText(id, utf8mb40900AICI))
	assert.Equal(t, latin1German2CI, ForText(latin1German2CI, id))

	// An ID that ForText did not return has no collation.
	assert.Nil(t, Lookup(0xFFFF))
}

// TestForTextIsOneCollation checks that ForText returns one ID for a
// collation and a character set, for every collation of the character set and
// from every goroutine, and another for another character set.
func TestForTextIsOneCollation(t *testing.T) {
	want := ForText(latin1German2CI, utf8mb40900AICI)
	assert.Equal(t, want, ForText(latin1German2CI, utf8mb4GeneralC))
	assert.NotEqual(t, want, ForText(latin1German2CI, gbkChineseCI))
	assert.NotEqual(t, want, ForText(latin1SwedishCI, utf8mb40900AICI))

	var wg sync.WaitGroup
	ids := make([]collations.ID, 16)
	for i := range ids {
		wg.Go(func() {
			ids[i] = ForText(gbkChineseCI, latin1SwedishCI)
			assert.NotNil(t, Lookup(ids[i]))
		})
	}
	wg.Wait()
	for _, id := range ids {
		assert.Equal(t, ids[0], id)
	}
}

// BenchmarkTextCollation compares latin1_german2_ci on latin1 text with the
// collation of ForText on the same text in utf8mb4, for a text that is ASCII,
// which is not converted, and for one that is not.
func BenchmarkTextCollation(b *testing.B) {
	plain := Lookup(latin1German2CI)
	forText := Lookup(ForText(latin1German2CI, utf8mb40900AICI))
	texts := []struct {
		name          string
		latin1, utf8  string
		latin1B, utfB string
	}{
		{"ASCII", "Mueller, Strasse 12", "Mueller, Strasse 12", "Mueller, Strasse 13", "Mueller, Strasse 13"},
		{"not ASCII", "M\xfcller, Stra\xdfe 12", "Müller, Straße 12", "M\xfcller, Stra\xdfe 13", "Müller, Straße 13"},
	}
	for _, text := range texts {
		for _, c := range []struct {
			name string
			coll Collation
			a, b []byte
		}{
			{"latin1 text", plain, []byte(text.latin1), []byte(text.latin1B)},
			{"utf8mb4 text", forText, []byte(text.utf8), []byte(text.utfB)},
		} {
			b.Run(text.name+"/"+c.name+"/Collate", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if c.coll.Collate(c.a, c.b, false) == 0 {
						b.Fatal("the texts are not equal")
					}
				}
			})
			b.Run(text.name+"/"+c.name+"/Hash", func(b *testing.B) {
				b.ReportAllocs()
				hasher := vthash.New()
				for b.Loop() {
					hasher.Reset()
					c.coll.Hash(&hasher, c.a, 0)
				}
			})
			b.Run(text.name+"/"+c.name+"/TinyWeightString", func(b *testing.B) {
				b.ReportAllocs()
				tiny := c.coll.(TinyWeightCollation)
				for b.Loop() {
					tiny.TinyWeightString(c.a)
				}
			})
			b.Run(text.name+"/"+c.name+"/WeightString", func(b *testing.B) {
				b.ReportAllocs()
				dst := make([]byte, 0, 256)
				for b.Loop() {
					dst = c.coll.WeightString(dst[:0], c.a, 0)
				}
			})
		}
	}
}

// BenchmarkLookup looks up collations that MySQL has, which every comparison
// of text does.
func BenchmarkLookup(b *testing.B) {
	ids := []collations.ID{utf8mb40900AICI, utf8mb4GeneralC, latin1SwedishCI, latin1German2CI, collations.CollationBinaryID, gbkChineseCI}
	var coll Collation
	for b.Loop() {
		for _, id := range ids {
			coll = Lookup(id)
		}
	}
	_ = coll
}
