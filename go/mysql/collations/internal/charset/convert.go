package charset

import (
	"fmt"
	"unicode/utf8"
)

type ErrFailedConversion int

func (e ErrFailedConversion) Error() string {
	return fmt.Sprintf("failed to convert %d codepoints", e)
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
		return dst[:nDst], ErrFailedConversion(failed)
	}
	return dst[:nDst], nil
}

func convertSlow(dst []byte, dstCharset Charset, src []byte, srcCharset Charset) ([]byte, error) {
	var failed, nDst int

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
		return dst[:nDst], ErrFailedConversion(failed)
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
		return src, nil
	}
	if trans, ok := dstCharset.(Convertible); ok {
		return trans.Convert(dst, src, srcCharset)
	}
	switch srcCharset.(type) {
	case Charset_utf8, Charset_utf8mb4:
		return convertFastFromUTF8(dst, dstCharset, src)
	default:
		return convertSlow(dst, dstCharset, src, srcCharset)
	}
}

func ConvertFromUTF8(dst []byte, dstCharset Charset, src []byte) ([]byte, error) {
	return Convert(dst, dstCharset, src, Charset_utf8mb4{})
}
