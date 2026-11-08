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
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// ClientCharset is the character set that the parser reads the bytes of a query
// in. MySQL reads a query in the client's character set (character_set_client),
// and that character set decides which bytes can be part of an unquoted name
// and which bytes are spaces. Only the character set matters: all the
// collations of a character set share the tables that decide this.
//
// The zero value is ClientCharsetUTF8, which is what every parser reads unless
// it is asked for another character set.
type ClientCharset int8

const (
	// ClientCharsetUTF8 reads a query as utf8mb4 or utf8mb3. A byte from 0x80
	// to 0xFE can be part of a name, but a name cannot start with a multibyte
	// lead byte that does not start a valid UTF-8 character.
	//
	// MySQL reads utf8mb3 with the same lexer rule, and then it also rejects
	// a name that is not well-formed utf8mb3, such as one that holds a stray
	// byte or a character of four bytes (IDENT_sys in sql_yacc.yy). The parser
	// does not make that check: under utf8mb4, MySQL replaces such a byte with
	// '?' instead, and Vitess sends the query on a utf8mb4 connection.
	ClientCharsetUTF8 ClientCharset = iota
	// ClientCharsetLatin1 reads a query as latin1. A byte from 0x80 up can be
	// part of a name only if MySQL's latin1 table makes it a letter, and 0xA0,
	// the no-break space, is a space.
	ClientCharsetLatin1

	// charsetCount is the number of character sets. It must stay last.
	charsetCount
)

// String returns the name of the character set as MySQL spells it.
func (cs ClientCharset) String() string {
	switch cs {
	case ClientCharsetUTF8:
		return "utf8mb4"
	case ClientCharsetLatin1:
		return "latin1"
	default:
		return "unknown"
	}
}

// validate returns an error for a value that is not one of the ClientCharset
// constants, so that a parser never reads a query with a table that does not
// exist.
func (cs ClientCharset) validate() error {
	switch cs {
	case ClientCharsetUTF8, ClientCharsetLatin1:
		return nil
	default:
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown parser character set %d", cs)
	}
}

// ClientCharsetFor returns the rule for the character set that a client's
// connection uses, as MySQL names it. utf8 is utf8mb3 in MySQL 8.0 and later.
// ok is false for a character set that the parser has no rule for.
func ClientCharsetFor(name string) (cs ClientCharset, ok bool) {
	switch name {
	case "utf8mb4", "utf8mb3", "utf8":
		return ClientCharsetUTF8, true
	case "latin1":
		return ClientCharsetLatin1, true
	default:
		return ClientCharsetUTF8, false
	}
}

// charClass holds what one byte is to the tokenizer.
type charClass uint8

const (
	// classLetter marks a byte that can be part of an unquoted name and is not
	// a digit: MySQL's ident_map without the digits.
	classLetter charClass = 1 << iota
	// classDigit marks the digits 0 to 9.
	classDigit
	// classSpace marks a byte that MySQL skips between two tokens.
	classSpace
	// classLead marks a UTF-8 multibyte lead byte. MySQL reads a name that
	// starts with such a byte only if the byte starts a valid character;
	// otherwise the byte is a token of its own, which is a syntax error.
	classLead
)

// charTableSize is the size of a charTable: a power of two above eofChar, so
// that class can mask its index rather than check it.
const charTableSize = 0x200

// charTable holds the class of each byte, and of eofChar, which belongs to no
// class, so that the tokenizer can look up the result of peek directly.
type charTable [charTableSize]charClass

// class returns the class of ch, a byte or eofChar. The mask keeps every such
// value as it is, and it lets the compiler leave out the bounds check, which
// costs the tokenizer's hot loops a few percent.
func (table *charTable) class(ch uint16) charClass {
	return table[ch&(charTableSize-1)]
}

// charTables holds the table of each character set.
var charTables = [charsetCount]*charTable{
	ClientCharsetUTF8:   newCharTable(ClientCharsetUTF8),
	ClientCharsetLatin1: newCharTable(ClientCharsetLatin1),
}

// latin1Letters are the bytes from 0x80 up that MySQL's latin1 character set
// marks as an upper or lower case letter (ctype_latin1 in MySQL's
// strings/ctype-latin1.cc). MySQL's latin1 is Windows-1252, so the letters
// include Š, Œ, Ž, š, œ, ž, Ÿ and ƒ. The signs ×, ÷, €, the quotes and the
// other punctuation are not letters, nor are the bytes that Windows-1252 leaves
// undefined: MySQL reads each of those as a token of its own.
var latin1Letters = [][2]byte{
	{0x83, 0x83}, // ƒ
	{0x8A, 0x8A}, // Š
	{0x8C, 0x8C}, // Œ
	{0x8E, 0x8E}, // Ž
	{0x9A, 0x9A}, // š
	{0x9C, 0x9C}, // œ
	{0x9E, 0x9F}, // ž Ÿ
	{0xC0, 0xD6}, // À to Ö
	{0xD8, 0xF6}, // Ø to ö
	{0xF8, 0xFF}, // ø to ÿ
}

// latin1Space is the no-break space, the only byte from 0x80 up that MySQL's
// latin1 character set marks as a space.
const latin1Space = 0xA0

// newCharTable builds the table of a character set. The ASCII bytes are the
// same in every character set that the parser reads.
func newCharTable(cs ClientCharset) *charTable {
	var table charTable
	for ch := range 0x80 {
		switch {
		case 'a' <= ch && ch <= 'z', 'A' <= ch && ch <= 'Z', ch == '_', ch == '$':
			table[ch] = classLetter
		case '0' <= ch && ch <= '9':
			table[ch] = classDigit
		}
	}
	for i := range len(sqlSpaceChars) {
		table[sqlSpaceChars[i]] = classSpace
	}
	switch cs {
	case ClientCharsetUTF8:
		// MySQL's utf8mb4 and utf8mb3 tables mark every byte from 0x80 to 0xFE
		// as a letter, so that the bytes of every character from U+0080 up can
		// be part of a name. Their entry for 0xFF is 0, so MySQL reads that
		// byte as a token of its own, which is a syntax error. No UTF-8
		// character holds it.
		for ch := 0x80; ch <= 0xFE; ch++ {
			table[ch] = classLetter
			if 0xC2 <= ch && ch <= 0xF7 {
				// my_mbcharlen_utf8mb4 gives these bytes a length of 2 to 4.
				// It gives the others a length of 0, and MySQL reads such a
				// byte as a character of one byte without checking it.
				table[ch] |= classLead
			}
		}
	case ClientCharsetLatin1:
		for _, letters := range latin1Letters {
			for ch := int(letters[0]); ch <= int(letters[1]); ch++ {
				table[ch] = classLetter
			}
		}
		table[latin1Space] = classSpace
	default:
		panic(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "no character table for %s", cs))
	}
	return &table
}
