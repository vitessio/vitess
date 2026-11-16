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

// Every ID of MySQL fits below the flag.
var _ [int(textCollationFlag) - len(collationsById)]struct{}

// ForText returns the ID of a collation that has the rules of the collation
// id for text that is in the character set of the collation from, when that
// is utf8mb4 or utf8mb3, the character sets that a tablet sends text in.
// Lookup returns that collation.
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
// when the text is in another character set than utf8mb4 and utf8mb3, when
// id or from is the binary collation, and when Vitess does not have one of
// the two. An ID that ForText returned is id with one more bit, and is valid
// in this process only.
func ForText(id, from collations.ID) collations.ID {
	_, src, ok := TextConversion(id, from)
	if !ok {
		return id
	}
	if isUTF8(src) {
		return id | textCollationFlag
	}
	return id
}

// TextConversion returns the character set of the collation id and the one
// of the collation from, and whether text of from has to be converted before
// the collation id is given it. It does not when the text is in the character
// set of id already, when id or from is the binary collation, and when Vitess
// does not have one of the two. It does not between utf8mb3 and utf8mb4
// either: the text of a utf8mb3 column has the same bytes when a tablet sends
// it as utf8mb4.
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
	if to.Name() == src.Name() || to.IsSuperset(src) || (isUTF8(to) && isUTF8(src)) {
		return nil, nil, false
	}
	return to, src, true
}

// isUTF8 reports whether cs is utf8mb4 or utf8mb3.
func isUTF8(cs Charset) bool {
	switch cs.(type) {
	case charset.Charset_utf8mb4, charset.Charset_utf8mb3:
		return true
	}
	return false
}

// textCollations has the collation of ForText for the collation of each ID,
// made when it is first looked up.
var textCollations [len(collationsById)]atomic.Pointer[Collation]

// lookupTextCollation returns the collation of an ID of ForText.
func lookupTextCollation(id collations.ID) Collation {
	i := int(id &^ textCollationFlag)
	if i >= len(collationsById) || collationsById[i] == nil {
		return nil
	}
	if coll := textCollations[i].Load(); coll != nil {
		return *coll
	}
	inner := collationsById[i]
	text := &textCollation{
		Collation: inner,
		id:        id,
		to:        inner.Charset(),
		ascii:     charset.IsASCIIIdentical(inner.Charset()),
	}
	var coll Collation = text
	if tiny, ok := inner.(TinyWeightCollation); ok {
		coll = &tinyTextCollation{textCollation: text, tiny: tiny}
	}
	textCollations[i].CompareAndSwap(nil, &coll)
	return *textCollations[i].Load()
}

// textCollation is a collation of ForText: the embedded collation for text
// in utf8mb4.
type textCollation struct {
	Collation
	textConversion
	id collations.ID
}

// textConversion converts utf8mb4 text to the character set to.
type textConversion struct {
	to Charset
	// ascii is true when ASCII text has the same bytes in to.
	ascii bool
}

// ID implements Collation.
func (c *textCollation) ID() collations.ID {
	return c.id
}

// Charset implements Collation. It is the character set of the text that the
// collation is given.
func (c *textCollation) Charset() Charset {
	return charset.Charset_utf8mb4{}
}

// unchanged reports whether src has the same bytes in the character set to,
// which ASCII text has in most character sets.
func (c textConversion) unchanged(src []byte) bool {
	if !c.ascii {
		return false
	}
	for _, b := range src {
		if b >= 0x80 {
			return false
		}
	}
	return true
}

// convert appends src in the character set to to dst, which has capacity. A
// character that the character set does not have becomes "?", as in a
// conversion in MySQL.
func (c textConversion) convert(dst, src []byte) []byte {
	// The error says that a character became "?".
	converted, _ := charset.Convert(dst, c.to, src, charset.Charset_utf8mb4{})
	return converted
}

// textBuffers has the buffers that the texts are converted into. A sort, a
// DISTINCT and a join convert every value, many of them more than once, so
// the buffers are used again. A buffer has capacity: charset.Convert does not
// append to one that has none.
var textBuffers = sync.Pool{New: func() any {
	buf := make([]byte, 0, 64)
	return &buf
}}

// maxTextBuffer is the largest buffer that goes back to textBuffers.
const maxTextBuffer = 64 * 1024

// buffered returns src in the character set to, in a buffer of textBuffers
// that release takes back. The text is not kept after release.
func (c textConversion) buffered(src []byte) (buf *[]byte, text []byte) {
	buf = textBuffers.Get().(*[]byte)
	return buf, c.convert((*buf)[:0], src)
}

// release gives the buffer of buffered back, with the capacity that the text
// grew it to. src is the text that was converted: a text that is src itself
// is not a buffer.
func release(buf *[]byte, text, src []byte) {
	if cap(text) != 0 && cap(text) <= maxTextBuffer && (len(src) == 0 || &text[:1][0] != &src[0]) {
		*buf = text[:0]
	}
	textBuffers.Put(buf)
}

// Collate implements Collation.
func (c *textCollation) Collate(left, right []byte, isPrefix bool) int {
	var leftBuf, rightBuf *[]byte
	l, r := left, right
	if !c.unchanged(left) {
		leftBuf, l = c.buffered(left)
	}
	if !c.unchanged(right) {
		rightBuf, r = c.buffered(right)
	}
	result := c.Collation.Collate(l, r, isPrefix)
	if leftBuf != nil {
		release(leftBuf, l, left)
	}
	if rightBuf != nil {
		release(rightBuf, r, right)
	}
	return result
}

// WeightString implements Collation.
func (c *textCollation) WeightString(dst, src []byte, numCodepoints int) []byte {
	if c.unchanged(src) {
		return c.Collation.WeightString(dst, src, numCodepoints)
	}
	buf, text := c.buffered(src)
	dst = c.Collation.WeightString(dst, text, numCodepoints)
	release(buf, text, src)
	return dst
}

// Hash implements Collation.
func (c *textCollation) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	if c.unchanged(src) {
		c.Collation.Hash(hasher, src, numCodepoints)
		return
	}
	buf, text := c.buffered(src)
	c.Collation.Hash(hasher, text, numCodepoints)
	release(buf, text, src)
}

// Wildcard implements Collation. The pattern keeps its text, which is not in
// a buffer.
func (c *textCollation) Wildcard(pat []byte, matchOne, matchMany, escape rune) WildcardPattern {
	if !c.unchanged(pat) {
		pat = c.convert(make([]byte, 0, len(pat)+1), pat)
	}
	return &textPattern{text: c.textConversion, pattern: c.Collation.Wildcard(pat, matchOne, matchMany, escape)}
}

// textPattern is a pattern of a textCollation.
type textPattern struct {
	text    textConversion
	pattern WildcardPattern
}

// Match implements WildcardPattern.
func (p *textPattern) Match(in []byte) bool {
	if p.text.unchanged(in) {
		return p.pattern.Match(in)
	}
	buf, text := p.text.buffered(in)
	matched := p.pattern.Match(text)
	release(buf, text, in)
	return matched
}

// tinyTextCollation is a textCollation of a collation that has tiny weight
// strings.
type tinyTextCollation struct {
	*textCollation
	tiny TinyWeightCollation
}

// TinyWeightString implements TinyWeightCollation.
func (c *tinyTextCollation) TinyWeightString(src []byte) uint32 {
	if c.unchanged(src) {
		return c.tiny.TinyWeightString(src)
	}
	buf, text := c.buffered(src)
	weight := c.tiny.TinyWeightString(text)
	release(buf, text, src)
	return weight
}
