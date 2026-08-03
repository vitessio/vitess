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

type Decoding uint8

const (
	DecodeOK Decoding = iota

	DecodeUnmappable

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
	// its width in bytes and the decoding result. On malformed
	// input it returns RuneError, the number of bytes to skip to reach the
	// next rune (zero for empty input), and false.
	DecodeRune([]byte) (rune, int, Decoding)
}
