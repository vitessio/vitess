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

package evalengine

import (
	"vitess.io/vitess/go/mysql/collations/charset"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// validateIntroducedBytes returns the error MySQL raises for a hex or bit
// literal whose bytes are not well-formed in the character set of its
// introducer, as in _utf16 X'D800'. Like MySQL, the error shows at most three
// bytes, starting at the first character that is not well-formed.
func validateIntroducedBytes(cs charset.Charset, bytes []byte) error {
	valid, ok := wellFormedLength(cs, bytes)
	if ok {
		return nil
	}
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.InvalidCharacterString,
		"Invalid %s character string: '%X'", cs.Name(), bytes[valid:min(valid+3, len(bytes))])
}

// wellFormedLength returns the length of the longest well-formed prefix of b
// in cs, and whether MySQL accepts b as a string in cs.
//
// It follows MySQL's well_formed_len for each character set rather than the
// decoders, as a decoder can be stricter than MySQL: MySQL accepts any byte in
// a single-byte character set and a lone surrogate in ucs2, and checks only the
// byte ranges in the multi-byte East Asian character sets, where the decoders
// also reject a code point that has no mapping.
func wellFormedLength(cs charset.Charset, b []byte) (int, bool) {
	switch cs.(type) {
	case charset.Charset_ucs2:
		return len(b), true
	case charset.Charset_sjis, charset.Charset_cp932:
		return wellFormedLengthDoubleByte(b, isSJISSingle, isSJISHead, isSJISTail)
	case charset.Charset_ujis:
		return wellFormedLengthEUCJP(b, false)
	case charset.Charset_eucjpms:
		return wellFormedLengthEUCJP(b, true)
	case charset.Charset_euckr:
		return wellFormedLengthDoubleByte(b, isASCII, isEUCKRHead, isEUCKRTail)
	case charset.Charset_gb2312:
		return wellFormedLengthDoubleByte(b, isASCII, isGB2312Head, isGB2312Tail)
	}
	if cs.MaxWidth() == 1 {
		return len(b), true
	}
	valid := 0
	for valid < len(b) {
		_, size, ok := cs.DecodeRune(b[valid:])
		if !ok {
			return valid, false
		}
		valid += size
	}
	return valid, true
}

// wellFormedLengthDoubleByte checks a character set whose characters are
// either a single byte or a lead byte followed by a trail byte.
func wellFormedLengthDoubleByte(b []byte, single, head, tail func(byte) bool) (int, bool) {
	i := 0
	for i < len(b) {
		switch {
		case single(b[i]):
			i++
		case i+1 < len(b) && head(b[i]) && tail(b[i+1]):
			i += 2
		default:
			return i, false
		}
	}
	return i, true
}

// wellFormedLengthEUCJP checks ujis and eucjpms, which share their byte ranges.
// Like MySQL, eucjpms accepts a string that ends in the middle of a character.
func wellFormedLengthEUCJP(b []byte, acceptTruncated bool) (int, bool) {
	i := 0
	for i < len(b) {
		c := b[i]
		if c <= 0x7F {
			i++
			continue
		}
		if i+1 == len(b) {
			return i, acceptTruncated
		}
		switch c {
		case 0x8E: // [8E][A0-DF]
			if b[i+1] < 0xA0 || b[i+1] > 0xDF {
				return i, false
			}
			i += 2
		case 0x8F: // [8F][A1-FE][A1-FE]
			if i+2 == len(b) || !isEUCJPByte(b[i+1]) || !isEUCJPByte(b[i+2]) {
				return i, false
			}
			i += 3
		default: // [A1-FE][A1-FE]
			if !isEUCJPByte(c) || !isEUCJPByte(b[i+1]) {
				return i, false
			}
			i += 2
		}
	}
	return i, true
}

func isASCII(c byte) bool {
	return c < 0x80
}

// isSJISSingle reports an ASCII or a half-width katakana byte.
func isSJISSingle(c byte) bool {
	return c < 0x80 || (c >= 0xA1 && c <= 0xDF)
}

func isSJISHead(c byte) bool {
	return (c >= 0x81 && c <= 0x9F) || (c >= 0xE0 && c <= 0xFC)
}

func isSJISTail(c byte) bool {
	return (c >= 0x40 && c <= 0x7E) || (c >= 0x80 && c <= 0xFC)
}

func isEUCJPByte(c byte) bool {
	return c >= 0xA1 && c <= 0xFE
}

func isEUCKRHead(c byte) bool {
	return c >= 0x81 && c <= 0xFE
}

func isEUCKRTail(c byte) bool {
	return (c >= 0x41 && c <= 0x5A) || (c >= 0x61 && c <= 0x7A) || (c >= 0x81 && c <= 0xFE)
}

func isGB2312Head(c byte) bool {
	return c >= 0xA1 && c <= 0xF7
}

func isGB2312Tail(c byte) bool {
	return c >= 0xA1 && c <= 0xFE
}
