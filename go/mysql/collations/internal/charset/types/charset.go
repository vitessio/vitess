package types

type Charset interface {
	Name() string
	SupportsSupplementaryChars() bool
	IsSuperset(other Charset) bool

	EncodeRune([]byte, rune) int
	DecodeRune([]byte) (rune, int)
}
