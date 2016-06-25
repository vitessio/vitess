package vindexes

import (
	"bytes"
	"fmt"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

// UnicodeLooseMD5 is a vindex that normalizes and hashes unicode strings
// to a keyspace id. It conservatively converts the string to its base
// characters before hashing. This is also known as UCA level 1.
// Ref: http://www.unicode.org/reports/tr10/#Multi_Level_Comparison.
// This is compatible with MySQL's utf8_unicode_ci collation.
type UnicodeLooseMD5 struct {
	name string
}

// MewUnicodeLooseMD5 creates a new UnicodeLooseMD5.
func MewUnicodeLooseMD5(name string, _ map[string]string) (Vindex, error) {
	return &UnicodeLooseMD5{name: name}, nil
}

// String returns the name of the vindex.
func (vind *UnicodeLooseMD5) String() string {
	return vind.name
}

// Cost returns the cost as 1.
func (vind *UnicodeLooseMD5) Cost() int {
	return 1
}

// Verify returns true if id maps to ksid.
func (vind *UnicodeLooseMD5) Verify(_ VCursor, id interface{}, ksid []byte) (bool, error) {
	data, err := unicodeHash(id)
	if err != nil {
		return false, fmt.Errorf("UnicodeLooseMD5.Verify: %v", err)
	}
	return bytes.Compare(data, ksid) == 0, nil
}

// Map returns the corresponding keyspace id values for the given ids.
func (vind *UnicodeLooseMD5) Map(_ VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		data, err := unicodeHash(id)
		if err != nil {
			return nil, fmt.Errorf("UnicodeLooseMD5.Map :%v", err)
		}
		out = append(out, data)
	}
	return out, nil
}

func unicodeHash(key interface{}) ([]byte, error) {
	source, err := getBytes(key)
	if err != nil {
		return nil, err
	}
	return binHash(normalize(source)), nil
}

func normalize(in []byte) []byte {
	// Ref: http://dev.mysql.com/doc/refman/5.6/en/char.html.
	// Trailing spaces are ignored by MySQL.
	in = bytes.TrimRight(in, " ")

	// Ref: http://www.unicode.org/reports/tr10/#Introduction.
	// Unicode seems to define a universal (or default) order.
	// But various locales have conflicting order,
	// which they have the right to override.
	// Unfortunately, the Go library requires you to specify a locale.
	// So, I chose English assuming that it won't override
	// the Unicode universal order. But I couldn't find an easy
	// way to verify this.
	// Also, the locale differences are not an issue for level 1,
	// because the conservative comparison makes them all equal.
	normalizer := collate.New(language.English, collate.Loose)

	// We use the collation key which can be used to
	// perform lexical comparisons.
	return normalizer.Key(new(collate.Buffer), in)
}

func init() {
	Register("unicode_loose_md5", MewUnicodeLooseMD5)
}
