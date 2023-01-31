/*
Copyright 2023 The Vitess Authors.

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

package json

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/rangetable"

	"vitess.io/vitess/go/hack"
)

type jpKind uint32

const (
	jpDocumentRoot jpKind = iota
	jpMember
	jpMemberAny
	jpArrayLocation
	jpArrayLocationAny
	jpAny
)

type Path struct {
	kind    jpKind
	offset0 int32
	offset1 int32
	name    string
	next    *Path
}

func (j *Path) push(next *Path) *Path {
	j.next = next
	return next
}

func (j *Path) format(b *strings.Builder) {
	switch j.kind {
	case jpDocumentRoot:
		b.WriteByte('$')
	case jpMember:
		if jpIsIdentifier(j.name) {
			b.WriteByte('.')
			b.WriteString(j.name)
		} else {
			_, _ = fmt.Fprintf(b, ".%q", j.name)
		}
	case jpMemberAny:
		b.WriteString(".*")
	case jpArrayLocation:
		switch {
		case j.offset0 == -1:
			b.WriteString("[last")
		case j.offset0 >= 0:
			_, _ = fmt.Fprintf(b, "[%d", j.offset0)
		case j.offset0 < 0:
			_, _ = fmt.Fprintf(b, "[last%d", j.offset0+1)
		}
		switch {
		case j.offset1 == -1:
			b.WriteString(" to last")
		case j.offset1 > 0:
			_, _ = fmt.Fprintf(b, " to %d", j.offset1)
		case j.offset1 < 0:
			_, _ = fmt.Fprintf(b, " to last%d", j.offset1+1)
		}
		b.WriteByte(']')
	case jpArrayLocationAny:
		b.WriteString("[*]")
	case jpAny:
		b.WriteString("**")
	}
}

func (j *Path) String() string {
	var b strings.Builder
	for j != nil {
		j.format(&b)
		j = j.next
	}
	return b.String()
}

func (j *Path) Multi() bool {
	for j != nil {
		if j.kind == jpAny {
			return true
		}
		j = j.next
	}
	return false
}

func (j *Path) arrayOffsets(ary []*Value) (int, int) {
	from := int(j.offset0)
	to := int(j.offset1)
	if from < 0 {
		from = len(ary) + from
	}
	if to == 0 {
		return from, from
	}
	if to < 0 {
		to = len(ary) + to
	}
	return from, to
}

func (j *Path) matchAny(v *Value, matched map[*Value]struct{}, match func(v *Value)) {
	j.match(v, matched, match)

	if obj, ok := v.Object(); ok {
		obj.Visit(func(_ []byte, v *Value) {
			j.matchAny(v, matched, match)
		})
	}
	if ary, ok := v.Array(); ok {
		for _, v := range ary {
			j.matchAny(v, matched, match)
		}
	}
}

func (j *Path) match(v *Value, matched map[*Value]struct{}, match func(v *Value)) {
	if v == nil {
		return
	}
	if j == nil {
		if _, m := matched[v]; !m {
			matched[v] = struct{}{}
			match(v)
		}
		return
	}
	switch j.kind {
	case jpDocumentRoot:
		j.next.match(v, matched, match)
	case jpAny:
		j.next.matchAny(v, matched, match)
	case jpMember:
		if obj, ok := v.Object(); ok {
			j.next.match(obj.Get(j.name), matched, match)
		}
	case jpMemberAny:
		if obj, ok := v.Object(); ok {
			obj.Visit(func(_ []byte, v *Value) {
				j.next.match(v, matched, match)
			})
		}
	case jpArrayLocation:
		if ary, ok := v.Array(); ok {
			from, to := j.arrayOffsets(ary)
			if from >= 0 && from < len(ary) {
				if to >= len(ary) {
					to = len(ary) - 1
				}
				for n := from; n <= to; n++ {
					j.next.match(ary[n], matched, match)
				}
			}
		} else if j.offset0 == 0 || j.offset0 == -1 {
			j.next.match(v, matched, match)
		}
	case jpArrayLocationAny:
		if ary, ok := v.Array(); ok {
			for _, v := range ary {
				j.next.match(v, matched, match)
			}
		}
	}
}

func (jp *Path) Match(doc *Value, match func(value *Value)) {
	jp.match(doc, make(map[*Value]struct{}), match)
}

func (j *Path) transform(v *Value, t func(pp *Path, vv *Value) bool) bool {
	if v == nil {
		return true
	}
	if j.next == nil {
		return t(j, v)
	}
	switch j.kind {
	case jpDocumentRoot:
		return j.next.transform(v, t)
	case jpMember:
		if obj, ok := v.Object(); ok {
			return j.next.transform(obj.Get(j.name), t)
		}
	case jpArrayLocation:
		if ary, ok := v.Array(); ok {
			from, to := j.arrayOffsets(ary)
			if from != to {
				return false
			}
			if from >= 0 && from < len(ary) {
				return j.next.transform(ary[from], t)
			}
		} else if j.offset0 == 0 || j.offset0 == -1 {
			/*
				If the path is evaluated against a value that is not an array,
				the result of the evaluation is the same as if the value had been
				wrapped in a single-element array:
			*/
			return j.next.transform(v, t)
		}
	case jpMemberAny, jpArrayLocationAny, jpAny:
		return false
	}
	return true
}

type Transformation int

const (
	Set Transformation = iota
	Insert
	Replace
	Remove
)

var errInvalidPathForTransform = errors.New("In this situation, path expressions may not contain the * and ** tokens or an array range.")

func ApplyTransform(t Transformation, doc *Value, paths []*Path, values []*Value) error {
	if t != Remove && len(paths) != len(values) {
		panic("missing Values for transformation")
	}
	for i, p := range paths {
		transform := func(pp *Path, vv *Value) bool {
			switch pp.kind {
			case jpArrayLocation:
				if ary, ok := vv.Array(); ok {
					from, to := pp.arrayOffsets(ary)
					if from != to {
						return false
					}
					if t == Remove {
						vv.DelArrayItem(from)
					} else {
						vv.SetArrayItem(from, values[i], t)
					}
				}
				return true
			case jpMember:
				if obj, ok := vv.Object(); ok {
					if t == Remove {
						obj.Del(pp.name)
					} else {
						obj.Set(pp.name, values[i], t)
					}
				}
				return true
			default:
				return false
			}
		}
		if !p.transform(doc, transform) {
			return errInvalidPathForTransform
		}
	}
	return nil
}

func MatchPath(rawJson, rawPath []byte, match func(value *Value)) error {
	var p1 Parser
	doc, err := p1.ParseBytes(rawJson)
	if err != nil {
		return err
	}

	var p2 PathParser
	jp, err := p2.ParseBytes(rawPath)
	if err != nil {
		return err
	}

	jp.Match(doc, match)
	return nil
}

var (
	unicodeRangeIdNeg      = rangetable.Merge(unicode.Pattern_Syntax, unicode.Pattern_White_Space)
	unicodeRangeIdStartPos = rangetable.Merge(unicode.Letter, unicode.Nl, unicode.Other_ID_Start)
	unicodeRangeIdContPos  = rangetable.Merge(unicodeRangeIdStartPos, unicode.Mn, unicode.Mc, unicode.Nd, unicode.Pc, unicode.Other_ID_Continue)
)

func isIdStartUnicode(r rune) bool {
	return unicode.Is(unicodeRangeIdStartPos, r) && !unicode.Is(unicodeRangeIdNeg, r)
}

func isIdPartUnicode(r rune) bool {
	return unicode.Is(unicodeRangeIdContPos, r) && !unicode.Is(unicodeRangeIdNeg, r) || r == '\u200C' || r == '\u200D'
}

func isIdentifierStart(chr rune) bool {
	return chr == '$' || chr == '_' || chr == '\\' ||
		'a' <= chr && chr <= 'z' || 'A' <= chr && chr <= 'Z' ||
		chr >= utf8.RuneSelf && isIdStartUnicode(chr)
}

func isIdentifierPart(chr rune) bool {
	return chr == '$' || chr == '_' || chr == '\\' ||
		'a' <= chr && chr <= 'z' || 'A' <= chr && chr <= 'Z' ||
		'0' <= chr && chr <= '9' ||
		chr >= utf8.RuneSelf && isIdPartUnicode(chr)
}

func jpIsIdentifier(s string) bool {
	if s == "" {
		return false
	}
	r, size := utf8.DecodeRuneInString(s)
	if !isIdentifierStart(r) {
		return false
	}
	for _, r := range s[size:] {
		if !isIdentifierPart(r) {
			return false
		}
	}
	return true
}

type PathParser struct {
	step func(p *PathParser, in []byte) ([]byte, error)
	path *Path
}

var errInvalid = errors.New("Invalid JSON path expression")

func stepRoot(p *PathParser, in []byte) ([]byte, error) {
	if in[0] == '$' {
		p.step = stepPathLeg
		return in[1:], nil
	}
	return nil, errInvalid
}

func trim(in []byte) ([]byte, int) {
	for n := 0; n < len(in); n++ {
		switch in[n] {
		case ' ', '\t', '\n':
		default:
			return in[n:], n
		}
	}
	return nil, len(in)
}

func stepPathLeg(p *PathParser, in []byte) ([]byte, error) {
	if in, _ = trim(in); in == nil {
		return nil, io.EOF
	}

	switch in[0] {
	case '.':
		p.step = stepMember
		return in[1:], nil
	case '[':
		p.step = stepArrayLocation
		return in[1:], nil
	case '*':
		if len(in) > 1 && in[1] == '*' {
			p.path = p.path.push(&Path{kind: jpAny})
			return in[2:], nil
		}
		return nil, errInvalid
	default:
		return nil, errInvalid
	}
}

func stepMember(p *PathParser, in []byte) ([]byte, error) {
	if in, _ = trim(in); in == nil {
		return nil, errInvalid
	}

	p.step = stepPathLeg

	if in[0] == '*' {
		p.path = p.path.push(&Path{kind: jpMemberAny})
		return in[1:], nil
	}

	var identifier string
	var err error

	if in[0] == '"' {
		identifier, in, err = p.lexQuotedString(in)
		if err != nil {
			return nil, err
		}
		p.path = p.path.push(&Path{kind: jpMember, name: identifier})
		return in, nil
	}
	identifier, in, err = p.lexIdentifier(in)
	if err != nil {
		return nil, err
	}
	p.path = p.path.push(&Path{kind: jpMember, name: identifier})
	return in, nil
}

func stepArrayLocation(p *PathParser, in []byte) ([]byte, error) {
	if in, _ = trim(in); in == nil {
		return nil, errInvalid
	}
	if in[0] >= '0' && in[0] <= '9' {
		var err error
		p.step = stepArrayLocationAfterNumeric
		p.path = p.path.push(&Path{kind: jpArrayLocation})
		p.path.offset0, in, err = p.lexNumeric(in)
		return in, err
	}
	if in[0] == '*' {
		p.step = stepArrayLocationClose
		p.path = p.path.push(&Path{kind: jpArrayLocationAny})
		return in[1:], nil
	}
	if bytes.HasPrefix(in, []byte{'l', 'a', 's', 't'}) {
		p.step = stepArrayLocationLast0
		p.path = p.path.push(&Path{kind: jpArrayLocation, offset0: -1})
		return in[4:], nil
	}
	return nil, errInvalid
}

func stepArrayLocationLast0(p *PathParser, in []byte) ([]byte, error) {
	if in, _ = trim(in); in == nil {
		return nil, errInvalid
	}
	if in[0] == '-' {
		p.step = stepArrayLocationLast0Offset
		return in[1:], nil
	}
	p.step = stepArrayLocationAfterNumeric
	return in, nil
}

func stepArrayLocationLast0Offset(p *PathParser, in []byte) ([]byte, error) {
	if in, _ = trim(in); in == nil {
		return nil, errInvalid
	}
	if in[0] >= '0' && in[0] <= '9' {
		p.step = stepArrayLocationAfterNumeric
		offset, in, err := p.lexNumeric(in)
		p.path.offset0 -= offset
		return in, err
	}
	return nil, errInvalid
}

func stepArrayLocationLast1(p *PathParser, in []byte) ([]byte, error) {
	if in, _ = trim(in); in == nil {
		return nil, errInvalid
	}
	if in[0] == '-' {
		p.step = stepArrayLocationLast1Offset
		return in[1:], nil
	}
	p.step = stepArrayLocationAfterNumeric
	return in, nil
}

func stepArrayLocationLast1Offset(p *PathParser, in []byte) ([]byte, error) {
	if in, _ = trim(in); in == nil {
		return nil, errInvalid
	}
	if in[0] >= '0' && in[0] <= '9' {
		p.step = stepArrayLocationClose
		offset, in, err := p.lexNumeric(in)
		p.path.offset1 -= offset
		return in, err
	}
	return nil, errInvalid
}

func stepArrayLocationAfterNumeric(p *PathParser, in []byte) ([]byte, error) {
	if in, _ = trim(in); in == nil {
		return nil, errInvalid
	}
	if in[0] == ']' {
		p.step = stepPathLeg
		return in[1:], nil
	}
	if bytes.HasPrefix(in, []byte{'t', 'o'}) {
		p.step = stepArrayLocationTo
		return in[2:], nil
	}
	return nil, errInvalid
}

func stepArrayLocationTo(p *PathParser, in []byte) ([]byte, error) {
	var skip int
	in, skip = trim(in)
	if in == nil || skip == 0 {
		return nil, errInvalid
	}
	if in[0] >= '0' && in[0] <= '9' {
		p.step = stepArrayLocationClose
		offset, in2, err := p.lexNumeric(in)
		if err != nil {
			return nil, err
		}
		if offset <= p.path.offset0 {
			return nil, fmt.Errorf("range %d should be >= %d", offset, p.path.offset0)
		}
		p.path.offset1 = offset
		return in2, nil
	}
	if bytes.HasPrefix(in, []byte{'l', 'a', 's', 't'}) {
		p.step = stepArrayLocationLast1
		p.path.offset1 = -1
		return in[4:], nil
	}
	return nil, errInvalid
}

func stepArrayLocationClose(p *PathParser, in []byte) ([]byte, error) {
	if in, _ = trim(in); in == nil {
		return nil, errInvalid
	}
	if in[0] == ']' {
		p.step = stepPathLeg
		return in[1:], nil
	}
	return nil, errInvalid
}

func (p *PathParser) lexIdentifier(in []byte) (string, []byte, error) {
	var n int
	for n < len(in) {
		var r rune
		var sz int

		r, sz = utf8.DecodeRune(in[n:])
		if r == utf8.RuneError {
			return "", nil, errInvalid
		} else if n == 0 {
			if !isIdentifierStart(r) {
				return "", nil, errInvalid
			}
		} else if !isIdentifierPart(r) {
			break
		}

		n += sz
	}
	return string(in[:n]), in[n:], nil
}

func (p *PathParser) lexQuotedString(in []byte) (string, []byte, error) {
	for n := 1; n < len(in); n++ {
		if in[n] == '\\' {
			break
		}
		if in[n] == '"' {
			return string(in[1:n]), in[n+1:], nil
		}
	}

	var buf strings.Builder
	var n = 1
	for n < len(in) {
		if in[n] == '"' {
			return buf.String(), in[n+1:], nil
		}

		c, multibyte, tail, err := strconv.UnquoteChar(hack.String(in[n:]), '"')
		if err != nil {
			return "", nil, err
		}
		if !multibyte {
			buf.WriteByte(byte(c))
		} else {
			buf.WriteRune(c)
		}
		n = len(in) - len(tail)
	}

	return "", nil, fmt.Errorf("unexpected EOF: missing closing '\"'")
}

func (p *PathParser) lexNumeric(in []byte) (int32, []byte, error) {
	const cutoff = math.MaxUint64/10 + 1
	const maxVal = math.MaxInt32

	var n uint64
	var pos int
	for pos < len(in) && in[pos] >= '0' && in[pos] <= '9' {
		if n >= cutoff {
			return maxVal, nil, strconv.ErrRange
		}
		n *= 10

		n1 := n + uint64(in[pos]-'0')
		if n1 < n || n1 > maxVal {
			return maxVal, nil, strconv.ErrRange
		}

		n = n1
		pos++
	}
	return int32(n), in[pos:], nil
}

func (p *PathParser) ParseBytes(in []byte) (*Path, error) {
	root := &Path{kind: jpDocumentRoot}
	p.path = root
	p.step = stepRoot

	var err error
	var ptr = in
	for {
		ptr, err = p.step(p, ptr)
		if err != nil {
			if err == io.EOF {
				return root, nil
			}
			return nil, fmt.Errorf("%w. The error is around character position %d.", err, len(in)-len(ptr))
		}
	}
}
