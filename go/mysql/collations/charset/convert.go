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

package charset

import (
	"fmt"
	"unicode/utf8"

	"vitess.io/vitess/go/hack"
)

func failedConversionError(from, to Charset, input []byte) error {
	return fmt.Errorf("Cannot convert string %q from %s to %s", input, from.Name(), to.Name())
}

func convertFastFromUTF8(dst []byte, dstCharset Charset, src []byte) ([]byte, error) {
	var failed, nDst int

	if dst == nil {
		dst = make([]byte, len(src)*3)
	} else {
		dst = dst[:cap(dst)]
	}

	for _, cp := range string(src) {
		if len(dst)-nDst < 4 {
			newDst := make([]byte, len(dst)*2)
			copy(newDst, dst[:nDst])
			dst = newDst
		}
		w := dstCharset.EncodeRune(dst[nDst:], cp)
		if w < 0 {
			failed++
			if w = dstCharset.EncodeRune(dst[nDst:], '?'); w < 0 {
				break
			}
		}
		nDst += w
	}

	if failed > 0 {
		return dst[:nDst], failedConversionError(&Charset_utf8mb4{}, dstCharset, src)
	}
	return dst[:nDst], nil
}

func convertSlow(dst []byte, dstCharset Charset, src []byte, srcCharset Charset) ([]byte, error) {
	var failed, nDst int
	var original = src

	if dst == nil {
		dst = make([]byte, len(src)*3)
	} else {
		dst = dst[:cap(dst)]
	}

	for len(src) > 0 {
		cp, width := srcCharset.DecodeRune(src)
		if cp == utf8.RuneError && width < 3 {
			failed++
			cp = '?'
		}
		src = src[width:]

		if len(dst)-nDst < 4 {
			newDst := make([]byte, len(dst)*2)
			copy(newDst, dst[:nDst])
			dst = newDst
		}

		w := dstCharset.EncodeRune(dst[nDst:], cp)
		if w < 0 {
			failed++
			w = dstCharset.EncodeRune(dst[nDst:], '?')
			if w < 0 {
				break
			}
		}
		nDst += w
	}

	if failed > 0 {
		return dst[:nDst], failedConversionError(srcCharset, dstCharset, original)
	}
	return dst[:nDst], nil
}

type Convertible interface {
	Charset
	Convert(dst, src []byte, from Charset) ([]byte, error)
}

// Convert transforms `src`, encoded with Charset `srcCharset`, and
// changes its encoding so that it becomes encoded with `dstCharset`.
// The result is appended to `dst` if `dst` is not nil; otherwise
// a new byte slice will be allocated to store the result.
func Convert(dst []byte, dstCharset Charset, src []byte, srcCharset Charset) ([]byte, error) {
	if dstCharset.IsSuperset(srcCharset) {
		if dst != nil {
			return append(dst, src...), nil
		}
		return src, nil
	}
	if trans, ok := dstCharset.(Convertible); ok {
		return trans.Convert(dst, src, srcCharset)
	}
	switch srcCharset.(type) {
	case Charset_binary:
		return ConvertFromBinary(dst, dstCharset, src)
	case Charset_utf8mb3, Charset_utf8mb4:
		return convertFastFromUTF8(dst, dstCharset, src)
	default:
		return convertSlow(dst, dstCharset, src, srcCharset)
	}
}

func Expand(dst []rune, src []byte, srcCharset Charset) []rune {
	switch srcCharset := srcCharset.(type) {
	case Charset_utf8mb3, Charset_utf8mb4:
		if dst == nil {
			return []rune(string(src))
		}
		dst = make([]rune, 0, len(src))
		for _, cp := range string(src) {
			dst = append(dst, cp)
		}
		return dst
	case Charset_binary:
		if dst == nil {
			dst = make([]rune, 0, len(src))
		}
		for _, c := range src {
			dst = append(dst, rune(c))
		}
		return dst
	default:
		if dst == nil {
			dst = make([]rune, 0, len(src))
		}
		for len(src) > 0 {
			cp, width := srcCharset.DecodeRune(src)
			src = src[width:]
			dst = append(dst, cp)
		}
		return dst
	}
}

func Collapse(dst []byte, src []rune, dstCharset Charset) []byte {
	switch dstCharset := dstCharset.(type) {
	case Charset_utf8mb3, Charset_utf8mb4:
		if dst == nil {
			return hack.StringBytes(string(src))
		}
		return append(dst, hack.StringBytes(string(src))...)
	case Charset_binary:
		if dst == nil {
			dst = make([]byte, 0, len(src))
		}
		for _, b := range src {
			dst = append(dst, byte(b))
		}
		return dst
	default:
		nDst := 0
		if dst == nil {
			dst = make([]byte, len(src)*dstCharset.MaxWidth())
		} else {
			dst = dst[:cap(dst)]
		}
		for _, c := range src {
			if len(dst)-nDst < 4 {
				newDst := make([]byte, len(dst)*2)
				copy(newDst, dst[:nDst])
				dst = newDst
			}
			w := dstCharset.EncodeRune(dst[nDst:], c)
			if w < 0 {
				if w = dstCharset.EncodeRune(dst[nDst:], '?'); w < 0 {
					break
				}
			}
			nDst += w
		}
		return dst[:nDst]
	}
}

func ConvertFromUTF8(dst []byte, dstCharset Charset, src []byte) ([]byte, error) {
	return Convert(dst, dstCharset, src, Charset_utf8mb4{})
}

func ConvertFromBinary(dst []byte, dstCharset Charset, src []byte) ([]byte, error) {
	switch dstCharset.(type) {
	case Charset_utf16, Charset_utf16le, Charset_ucs2:
		if len(src)%2 == 1 {
			dst = append(dst, 0)
		}
	case Charset_utf32:
		// TODO: it doesn't look like mysql pads binary for 4-byte encodings
	}
	if dst == nil {
		dst = src
	} else {
		dst = append(dst, src...)
	}
	if !Validate(dstCharset, dst) {
		return nil, failedConversionError(&Charset_binary{}, dstCharset, src)
	}
	return dst, nil
}
