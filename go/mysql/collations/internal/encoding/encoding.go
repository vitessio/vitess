package encoding

import (
	"vitess.io/vitess/go/mysql/collations/internal/encoding/simplifiedchinese"
	"vitess.io/vitess/go/mysql/collations/internal/encoding/unicode"
)

type Encoding interface {
	Name() string
	SupportsSupplementaryChars() bool
	DecodeRune([]byte) (rune, int)
	EncodeFromUTF8(in []byte) ([]byte, error)
}

type Encoding_utf8 = unicode.Encoding_utf8
type Encoding_utf8mb4 = unicode.Encoding_utf8mb4
type Encoding_utf16 = unicode.Encoding_utf16
type Encoding_ucs2 = unicode.Encoding_ucs2
type Encoding_utf32 = unicode.Encoding_utf32
type Encoding_gb18030 = simplifiedchinese.Encoding_gb18030

const RuneError = unicode.RuneError
