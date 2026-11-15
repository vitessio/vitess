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

package colldata

import (
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/vt/vthash"
)

// textCollationFlag is the bit of a collation ID that every ID of ForText
// has. MySQL and MariaDB have no collation ID with it.
const textCollationFlag collations.ID = 0x8000

// ForText returns the ID of a collation that has the rules of the collation
// id for text that is in the character set of the collation from. Lookup
// returns that collation.
//
// MySQL compares, sorts and groups a column in the collation of the column,
// whatever character set it sends the text in. vtgate has to do with that
// text what MySQL did with the column. For a latin1_german2_ci column whose
// rows arrived as utf8mb4, 'Müller' (4D C3 BC 6C 6C 65 72) has to equal
// 'Mueller', as it does in latin1_german2_ci, and it does not when
// latin1_german2_ci takes those bytes for latin1 text. The collation that
// ForText gives converts each text that it gets to the character set of id,
// and then does what id does with it. The text itself stays as it is, so the
// wrong collation for a column gives a wrong order, and never other text.
//
// It returns id itself when the text is in the character set of id already,
// when id or from is the binary collation, and when Vitess does not have one
// of the two. An ID that ForText returned is valid in this process only.
func ForText(id, from collations.ID) collations.ID {
	to, src, ok := TextConversion(id, from)
	if !ok {
		return id
	}
	return textCollations.id(Lookup(id), to, src)
}

// TextConversion returns the character set of the collation id and the one
// of the collation from, and whether text of from has to be converted before
// the collation id is given it. It does not when the text is in the character
// set of id already, when id or from is the binary collation, and when Vitess
// does not have one of the two.
func TextConversion(id, from collations.ID) (to, src Charset, ok bool) {
	if id == from || id&textCollationFlag != 0 || from&textCollationFlag != 0 {
		return nil, nil, false
	}
	coll, fromColl := Lookup(id), Lookup(from)
	if coll == nil || fromColl == nil {
		return nil, nil, false
	}
	to, src = coll.Charset(), fromColl.Charset()
	if _, binary := to.(charset.Charset_binary); binary {
		return nil, nil, false
	}
	if _, binary := src.(charset.Charset_binary); binary {
		return nil, nil, false
	}
	// utf8mb3 text is utf8mb4 text, and ASCII text is text of most
	// character sets. The other way a character that id does not have
	// becomes "?", as it does in MySQL.
	if to.Name() == src.Name() || to.IsSuperset(src) {
		return nil, nil, false
	}
	return to, src, true
}

// textCollationKey names one collation of ForText.
type textCollationKey struct {
	collation collations.ID
	charset   string
}

// textCollationRegistry has the collations that ForText returned an ID for.
type textCollationRegistry struct {
	mu  sync.Mutex
	ids map[textCollationKey]collations.ID
	// byID has the collation of the ID textCollationFlag|i at i. Lookup
	// reads it without the lock, so it is replaced and never changed.
	byID atomic.Pointer[[]Collation]
}

var textCollations textCollationRegistry

func (r *textCollationRegistry) id(coll Collation, to, src Charset) collations.ID {
	key := textCollationKey{collation: coll.ID(), charset: src.Name()}

	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.ids[key]; ok {
		return id
	}
	var all []Collation
	if p := r.byID.Load(); p != nil {
		all = *p
	}
	if len(all) >= int(textCollationFlag) {
		// There are fewer pairs of a collation and a character set than IDs.
		return coll.ID()
	}
	id := textCollationFlag | collations.ID(len(all))
	text := &textCollation{
		Collation: coll,
		id:        id,
		from:      src,
		ascii:     asciiIdentical(src) && asciiIdentical(to),
	}
	var wrapped Collation = text
	if tiny, ok := coll.(TinyWeightCollation); ok {
		wrapped = &tinyTextCollation{textCollation: text, tiny: tiny}
	}
	next := make([]Collation, len(all)+1)
	copy(next, all)
	next[len(all)] = wrapped
	r.byID.Store(&next)
	if r.ids == nil {
		r.ids = make(map[textCollationKey]collations.ID)
	}
	r.ids[key] = id
	return id
}

func (r *textCollationRegistry) lookup(id collations.ID) Collation {
	p := r.byID.Load()
	if p == nil {
		return nil
	}
	i := int(id &^ textCollationFlag)
	if i >= len(*p) {
		return nil
	}
	return (*p)[i]
}

// asciiIdentical reports whether ASCII text has the bytes of ASCII in cs. It
// does not in ucs2, utf16, utf16le and utf32, and in swe7, which has Ä at the
// byte of "[" and eight more letters in place of ASCII punctuation.
func asciiIdentical(cs Charset) bool {
	switch cs.(type) {
	case charset.Charset_ucs2, charset.Charset_utf16, charset.Charset_utf16le, charset.Charset_utf32:
		return false
	}
	return cs.Name() != "swe7"
}

// textCollation is a collation of ForText: the embedded collation for text
// in the character set from.
type textCollation struct {
	Collation
	id   collations.ID
	from Charset
	// ascii is true when ASCII text has the same bytes in both character
	// sets.
	ascii bool
}

// ID implements Collation.
func (c *textCollation) ID() collations.ID {
	return c.id
}

// Charset implements Collation. It is the character set of the text that the
// collation is given.
func (c *textCollation) Charset() Charset {
	return c.from
}

// text returns src in the character set of the embedded collation. It appends
// the converted text to dst, which is nil or has capacity, and returns src
// itself when its bytes are the same in both character sets. A character that
// the character set does not have becomes "?", as in a conversion in MySQL.
func (c *textCollation) text(dst, src []byte) []byte {
	if c.ascii {
		ascii := true
		for _, b := range src {
			if b >= 0x80 {
				ascii = false
				break
			}
		}
		if ascii {
			return src
		}
	}
	// The error says that a character became "?".
	converted, _ := charset.Convert(dst, c.Collation.Charset(), src, c.from)
	return converted
}

// collateBuffers are the buffers that Collate converts its two texts into. A
// sort compares the same values many times, so they are used again.
type collateBuffers struct {
	left, right []byte
}

var collateBufferPool = sync.Pool{New: func() any {
	return &collateBuffers{left: make([]byte, 0, 64), right: make([]byte, 0, 64)}
}}

// maxCollateBuffer is the largest buffer that goes back to collateBufferPool.
const maxCollateBuffer = 64 * 1024

// Collate implements Collation.
func (c *textCollation) Collate(left, right []byte, isPrefix bool) int {
	bufs := collateBufferPool.Get().(*collateBuffers)
	l := c.text(bufs.left[:0], left)
	r := c.text(bufs.right[:0], right)
	result := c.Collation.Collate(l, r, isPrefix)
	// A text that was not converted is not a buffer, and a buffer that grew
	// large is not kept.
	if converted(l, left) && cap(l) <= maxCollateBuffer {
		bufs.left = l
	}
	if converted(r, right) && cap(r) <= maxCollateBuffer {
		bufs.right = r
	}
	collateBufferPool.Put(bufs)
	return result
}

// converted reports whether text is a conversion of src, and not src itself.
func converted(text, src []byte) bool {
	return len(text) != 0 && (len(src) == 0 || &text[0] != &src[0])
}

// WeightString implements Collation.
func (c *textCollation) WeightString(dst, src []byte, numCodepoints int) []byte {
	return c.Collation.WeightString(dst, c.text(nil, src), numCodepoints)
}

// Hash implements Collation.
func (c *textCollation) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	c.Collation.Hash(hasher, c.text(nil, src), numCodepoints)
}

// Wildcard implements Collation.
func (c *textCollation) Wildcard(pat []byte, matchOne, matchMany, escape rune) WildcardPattern {
	return &textPattern{collation: c, pattern: c.Collation.Wildcard(c.text(nil, pat), matchOne, matchMany, escape)}
}

// textPattern is a pattern of a textCollation.
type textPattern struct {
	collation *textCollation
	pattern   WildcardPattern
}

// Match implements WildcardPattern.
func (p *textPattern) Match(in []byte) bool {
	return p.pattern.Match(p.collation.text(nil, in))
}

// tinyTextCollation is a textCollation of a collation that has tiny weight
// strings.
type tinyTextCollation struct {
	*textCollation
	tiny TinyWeightCollation
}

// TinyWeightString implements TinyWeightCollation.
func (c *tinyTextCollation) TinyWeightString(src []byte) uint32 {
	return c.tiny.TinyWeightString(c.text(nil, src))
}
