package vindexes

import (
	"bytes"
	"fmt"
	"sync"
	"unicode/utf8"

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

// NewUnicodeLooseMD5 creates a new UnicodeLooseMD5.
func NewUnicodeLooseMD5(name string, _ map[string]string) (Vindex, error) {
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

// Verify returns true if ids maps to ksids.
func (vind *UnicodeLooseMD5) Verify(_ VCursor, ids []interface{}, ksids [][]byte) (bool, error) {
	if len(ids) != len(ksids) {
		return false, fmt.Errorf("UnicodeLooseMD5.Verify: length of ids %v doesn't match length of ksids %v", len(ids), len(ksids))
	}
	for rowNum := range ids {
		data, err := unicodeHash(ids[rowNum])
		if err != nil {
			return false, fmt.Errorf("UnicodeLooseMD5.Verify: %v", err)
		}
		if bytes.Compare(data, ksids[rowNum]) != 0 {
			return false, nil
		}
	}
	return true, nil
}

// Map returns the corresponding keyspace id values for the given ids.
func (vind *UnicodeLooseMD5) Map(_ VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		data, err := unicodeHash(id)
		if err != nil {
			return nil, fmt.Errorf("UnicodeLooseMD5.Map: %v", err)
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

	collator := collatorPool.Get().(pooledCollator)
	defer collatorPool.Put(collator)

	norm, err := normalize(collator.col, collator.buf, source)
	if err != nil {
		return nil, err
	}
	return binHash(norm), nil
}

func normalize(col *collate.Collator, buf *collate.Buffer, in []byte) ([]byte, error) {
	// We cannot pass invalid UTF-8 to the collator.
	if !utf8.Valid(in) {
		return nil, fmt.Errorf("cannot normalize string containing invalid UTF-8: %q", string(in))
	}

	// Ref: http://dev.mysql.com/doc/refman/5.6/en/char.html.
	// Trailing spaces are ignored by MySQL.
	in = bytes.TrimRight(in, " ")

	// We use the collation key which can be used to
	// perform lexical comparisons.
	return col.Key(buf, in), nil
}

// pooledCollator pairs a Collator and a Buffer.
// These pairs are pooled to avoid reallocating for every request,
// which would otherwise be required because they can't be used concurrently.
//
// Note that you must ensure no active references into the buffer remain
// before you return this pair back to the pool.
// That is, either do your processing on the result first, or make a copy.
type pooledCollator struct {
	col *collate.Collator
	buf *collate.Buffer
}

var collatorPool = sync.Pool{New: newPooledCollator}

func newPooledCollator() interface{} {
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
	return pooledCollator{
		col: collate.New(language.English, collate.Loose),
		buf: new(collate.Buffer),
	}
}

func init() {
	Register("unicode_loose_md5", NewUnicodeLooseMD5)
}
