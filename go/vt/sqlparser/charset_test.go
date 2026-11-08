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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parserFor returns the test parser for a character set.
func parserFor(t *testing.T, cs ClientCharset) *Parser {
	parser, err := NewTestParser().WithCharset(cs)
	require.NoError(t, err)
	return parser
}

// createTableName parses "CREATE TABLE <name> (a int)" and returns the name
// of the table, or an error.
func createTableName(parser *Parser, name string) (string, error) {
	stmt, err := parser.Parse("CREATE TABLE " + name + " (a int)")
	if err != nil {
		return "", err
	}
	return stmt.(*CreateTable).Table.Name.String(), nil
}

// TestUnquotedNameUTF8 tests the bytes that a name can hold when the parser
// reads UTF-8, as MySQL reads a utf8mb4 or utf8mb3 connection. The first byte of
// a name that is a multibyte lead byte must start a valid character, and every
// other byte from 0x80 up is part of the name.
func TestUnquotedNameUTF8(t *testing.T) {
	parser := parserFor(t, ClientCharsetUTF8)
	testcases := []struct {
		name     string
		sql      string
		accepted bool
	}{
		// Measured on MySQL 8.4.13 with a utf8mb4 connection.
		{"lead byte of three bytes followed by ASCII", "\xE9a", false},
		{"stray continuation byte", "\x84a", true},
		{"é", "éa", true},
		{"„", "„a", true},
		{"no-break space", " a", true},
		// The other cases of my_ismbchar_utf8mb4.
		{"character of four bytes", "😀a", true},
		{"character of four bytes cut short", "\xF0\x9F\x98a", false},
		{"overlong character of three bytes", "\xE0\x80\x80a", false},
		{"overlong character of four bytes", "\xF0\x80\x80\x80a", false},
		{"surrogate", "\xED\xA0\x80a", false},
		{"code point above U+10FFFF", "\xF4\x90\x80\x80a", false},
		{"lead byte 0xF5", "\xF5\x80\x80\x80a", false},
		{"lead byte 0xF7", "\xF7\xBF\xBF\xBFa", false},
		{"lead byte at the end", "\xC3", false},
		// MySQL gives these a length of 0, so it reads them as one byte.
		{"0xC0", "\xC0\x80a", true},
		{"0xC1", "\xC1a", true},
		{"0xF8", "\xF8a", true},
		// MySQL's table does not make 0xFF a letter.
		{"0xFF", "\xFFa", false},
		{"0xFF inside a name", "a\xFFa", false},
		// Only the first byte of a name is checked.
		{"invalid lead byte inside a name", "a\xE9a", true},
		{"invalid lead byte after a digit", "1\xE9a", true},
		{"invalid lead byte after the dot of a qualified name", "d.\xE9a", true},
		{"invalid lead byte after a dot and a space", "d. \xE9a", false},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			name, err := createTableName(parser, tc.sql)
			if !tc.accepted {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.NotEmpty(t, name)
		})
	}
}

// TestUnquotedNameLatin1 tests the bytes that a name can hold when the parser
// reads latin1. MySQL's latin1 table decides it: a letter can be part of a
// name, the no-break space is a space, and every other byte from 0x80 up is a
// token of its own, which is a syntax error.
func TestUnquotedNameLatin1(t *testing.T) {
	parser := parserFor(t, ClientCharsetLatin1)
	testcases := []struct {
		name     string
		sql      string
		expected string // the table's name, or empty when the statement is rejected
	}{
		// Measured on MySQL 8.4.13 after SET NAMES latin1.
		{"é", "\xE9a", "\xE9a"},
		{"no-break space before the name", "\xA0a", "a"},
		{"€", "\x80a", ""},
		{"„", "\x84a", ""},
		{"×", "\xD7a", ""},
		// The letters of Windows-1252 and the last byte.
		{"Š", "\x8Aa", "\x8Aa"},
		{"ƒ", "\x83a", "\x83a"},
		{"ÿ", "\xFFa", "\xFFa"},
		{"÷ inside a name", "a\xF7a", ""},
		{"undefined byte 0x81", "\x81a", ""},
		// A lead byte of UTF-8 is a letter in latin1, whatever follows it.
		{"Ã followed by ASCII", "\xC3a", "\xC3a"},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			name, err := createTableName(parser, tc.sql)
			if tc.expected == "" {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, name)
		})
	}
}

// TestUnquotedNameEveryByte tests every byte from 0x80 up as the first byte of
// a name, in "CREATE TABLE <byte>a (a int)", in both character sets. The bytes
// that each character set accepts are written out here from MySQL's tables, as
// a second statement of the rule that the tokenizer's tables hold, and agree
// with what MySQL 8.4.13 rejects as a syntax error for each byte under
// SET NAMES utf8mb4 and SET NAMES latin1.
func TestUnquotedNameEveryByte(t *testing.T) {
	// latin1: the letters of ctype_latin1, and 0xA0, which is a space.
	latin1Accepted := map[byte]bool{0x83: true, 0x8A: true, 0x8C: true, 0x8E: true, 0x9A: true, 0x9C: true, 0x9E: true, 0x9F: true, 0xA0: true}
	for b := 0xC0; b <= 0xFF; b++ {
		if b != 0xD7 && b != 0xF7 {
			latin1Accepted[byte(b)] = true
		}
	}
	// UTF-8: every byte that is not a multibyte lead byte, because 'a' is not
	// a continuation byte, so no lead byte starts a valid character here, and
	// not 0xFF, which MySQL's table does not make a letter.
	utf8Accepted := func(b byte) bool { return b < 0xC2 || b > 0xF7 && b != 0xFF }

	utf8Parser := parserFor(t, ClientCharsetUTF8)
	latin1Parser := parserFor(t, ClientCharsetLatin1)
	for b := 0x80; b <= 0xFF; b++ {
		name := string([]byte{byte(b), 'a'})
		_, err := createTableName(utf8Parser, name)
		assert.Equal(t, utf8Accepted(byte(b)), err == nil, "utf8mb4: byte 0x%02X: %v", b, err)
		_, err = createTableName(latin1Parser, name)
		assert.Equal(t, latin1Accepted[byte(b)], err == nil, "latin1: byte 0x%02X: %v", b, err)
	}
}

// TestLatin1Spaces tests that the no-break space separates tokens and ends a
// "--" comment's prefix in latin1, as MySQL's latin1 table makes it a space,
// and that it does neither in UTF-8.
func TestLatin1Spaces(t *testing.T) {
	latin1Parser := parserFor(t, ClientCharsetLatin1)
	utf8Parser := parserFor(t, ClientCharsetUTF8)

	stmt, err := latin1Parser.Parse("select\xA01\xA0from\xA0t")
	require.NoError(t, err)
	assert.Equal(t, "select 1 from t", String(stmt))
	_, err = utf8Parser.Parse("select\xA01 from t")
	assert.Error(t, err)

	stmt, err = latin1Parser.Parse("select 1 --\xA0x")
	require.NoError(t, err)
	assert.Equal(t, "select 1 from dual", String(stmt))
	// In UTF-8, "--" followed by another byte is two minus signs.
	stmt, err = utf8Parser.Parse("select 1 --\xA0x")
	require.NoError(t, err)
	assert.Equal(t, "select 1 - -`\xA0x` from dual", String(stmt))
}

// TestFormattedLatin1NameParses tests that the formatter writes a latin1 name
// so that the latin1 parser reads it back.
func TestFormattedLatin1NameParses(t *testing.T) {
	parser := parserFor(t, ClientCharsetLatin1)
	stmt, err := parser.Parse("create table \xE9\x8A (a int)")
	require.NoError(t, err)
	formatted := String(stmt)
	reparsed, err := parser.Parse(formatted)
	require.NoError(t, err)
	assert.Equal(t, formatted, String(reparsed))
}

// TestWithCharset tests that WithCharset returns the parser for a character
// set without building a new one, that the parser keeps the settings it was
// built with, and that a value which is not a ClientCharset is an error.
func TestWithCharset(t *testing.T) {
	parser, err := New(Options{MySQLServerVersion: "8.0.30", TruncateUILen: 7, TruncateErrLen: 9})
	require.NoError(t, err)
	assert.Equal(t, ClientCharsetUTF8, parser.Charset())

	latin1, err := parser.WithCharset(ClientCharsetLatin1)
	require.NoError(t, err)
	assert.Equal(t, ClientCharsetLatin1, latin1.Charset())
	again, err := parser.WithCharset(ClientCharsetLatin1)
	require.NoError(t, err)
	assert.Same(t, latin1, again)
	back, err := latin1.WithCharset(ClientCharsetUTF8)
	require.NoError(t, err)
	assert.Same(t, parser, back)

	assert.Equal(t, parser.version, latin1.version)
	assert.Equal(t, 7, latin1.truncateUILen)
	assert.Equal(t, 9, latin1.GetTruncateErrLen())
	parser.SetTruncateErrLen(11)
	assert.Equal(t, 11, latin1.GetTruncateErrLen())

	_, err = parser.WithCharset(charsetCount)
	assert.ErrorContains(t, err, "unknown parser character set")
	_, err = New(Options{Charset: charsetCount})
	assert.ErrorContains(t, err, "unknown parser character set")

	fromOptions, err := New(Options{Charset: ClientCharsetLatin1})
	require.NoError(t, err)
	assert.Equal(t, ClientCharsetLatin1, fromOptions.Charset())
}

// TestClientCharsetFor tests which character set names have a rule.
func TestClientCharsetFor(t *testing.T) {
	testcases := []struct {
		name     string
		expected ClientCharset
		ok       bool
	}{
		{"utf8mb4", ClientCharsetUTF8, true},
		{"utf8mb3", ClientCharsetUTF8, true},
		{"utf8", ClientCharsetUTF8, true},
		{"latin1", ClientCharsetLatin1, true},
		{"koi8r", ClientCharsetUTF8, false},
		{"", ClientCharsetUTF8, false},
	}
	for _, tc := range testcases {
		cs, ok := ClientCharsetFor(tc.name)
		assert.Equal(t, tc.ok, ok, tc.name)
		assert.Equal(t, tc.expected, cs, tc.name)
	}
	assert.Equal(t, "utf8mb4", ClientCharsetUTF8.String())
	assert.Equal(t, "latin1", ClientCharsetLatin1.String())
	assert.Equal(t, "unknown", charsetCount.String())
}

// BenchmarkTokenizer scans statements with ASCII names and with non-ASCII
// names, which are the bytes that the character set decides.
func BenchmarkTokenizer(b *testing.B) {
	queries := []string{
		"select a, b, c from t1 where id = 1 and name = 'x' order by a limit 10",
		"insert into customer (id, name, email) values (1, 'alice', 'a@example.com'), (2, 'bob', 'b@example.com')",
		"create table `t` (id bigint not null auto_increment, name varchar(255), primary key (id)) engine=InnoDB",
		"select prénom, café, naïve from données where clé = 'valeur' and ñ = 1",
	}
	for _, query := range queries {
		b.Run(fmt.Sprintf("%d", len(query)), func(b *testing.B) {
			parser := NewTestParser()
			b.SetBytes(int64(len(query)))
			for b.Loop() {
				tkn := parser.NewStringTokenizer(query)
				for {
					typ, _ := tkn.Scan()
					if typ == 0 || typ == LEX_ERROR {
						break
					}
				}
			}
		})
	}
}
