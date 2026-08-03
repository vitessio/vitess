/*
Copyright 2021 The Vitess Authors.

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

package types

// Decoding is the result of decoding a rune. It mirrors the two predicates
// MySQL keeps separate for multibyte data: whether a byte sequence is a
// well-formed character (ismbchar, used for walking and counting) and whether
// it maps to Unicode (mb_wc, used for conversion).
type Decoding uint8

const (
	// DecodeOK means the input is well-formed and maps to Unicode. The rune is
	// the decoded codepoint and the width is the width of the character.
	DecodeOK Decoding = iota

	// DecodeUnmappable means the input is a well-formed character with no
	// Unicode mapping. The rune is RuneError and the width is the width of the character.
	DecodeUnmappable

	// DecodeInvalid means the input is malformed. The rune is RuneError and the
	// width is the number of bytes to skip to reach the next rune.
	DecodeInvalid
)

func (d Decoding) IsChar() bool { return d != DecodeInvalid }

func (d Decoding) IsMapped() bool { return d == DecodeOK }

type Charset interface {
	Name() string
	SupportsSupplementaryChars() bool
	IsSuperset(other Charset) bool
	MaxWidth() int

	EncodeRune([]byte, rune) int
	// DecodeRune decodes the first rune in the input and returns it along with
	// its width in bytes and the decoding result. The rune is only meaningful
	// when the result is DecodeOK. Unless the result is DecodeInvalid, the
	// first width bytes are exactly one character and can be copied through
	// verbatim; on DecodeInvalid the width is the number of bytes to skip to
	// reach the next rune, which is zero only for empty input.
	DecodeRune([]byte) (rune, int, Decoding)
}
