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
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
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

func (jp *Path) push(next *Path) *Path {
	jp.next = next
	return next
}

func (jp *Path) format(b *strings.Builder) {
	switch jp.kind {
	case jpDocumentRoot:
		b.WriteByte('$')
	case jpMember:
		if jpIsIdentifier(jp.name) {
			b.WriteByte('.')
			b.WriteString(jp.name)
		} else {
			_, _ = fmt.Fprintf(b, ".%q", jp.name)
		}
	case jpMemberAny:
		b.WriteString(".*")
	case jpArrayLocation:
		switch {
		case jp.offset0 == -1:
			b.WriteString("[last")
		case jp.offset0 >= 0:
			_, _ = fmt.Fprintf(b, "[%d", jp.offset0)
		case jp.offset0 < 0:
			_, _ = fmt.Fprintf(b, "[last%d", jp.offset0+1)
		}
		switch {
		case jp.offset1 == -1:
			b.WriteString(" to last")
		case jp.offset1 > 0:
			_, _ = fmt.Fprintf(b, " to %d", jp.offset1)
		case jp.offset1 < 0:
			_, _ = fmt.Fprintf(b, " to last%d", jp.offset1+1)
		}
		b.WriteByte(']')
	case jpArrayLocationAny:
		b.WriteString("[*]")
	case jpAny:
		b.WriteString("**")
	}
}

func (jp *Path) String() string {
	var b strings.Builder
	for jp != nil {
		jp.format(&b)
		jp = jp.next
	}
	return b.String()
}

func (jp *Path) ContainsWildcards() bool {
	for jp != nil {
		switch jp.kind {
		case jpAny, jpArrayLocationAny, jpMemberAny:
			return true
		case jpArrayLocation:
			if jp.offset1 != 0 {
				return true
			}
		}
		jp = jp.next
	}
	return false
}

func (jp *Path) arrayOffsets(ary []*Value) (int, int) {
	from := int(jp.offset0)
	to := int(jp.offset1)
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

type matcher struct {
	seen map[*Value]struct{}
	f    func(v *Value)
	wrap bool
}

func (m *matcher) match(v *Value) {
	if _, seen := m.seen[v]; !seen {
		m.seen[v] = struct{}{}
		m.f(v)
	}
}

func (m *matcher) any(p *Path, v *Value) {
	m.value(p, v)

	if obj, ok := v.Object(); ok {
		obj.Visit(func(_ string, v *Value) {
			m.any(p, v)
		})
	}
	if ary, ok := v.Array(); ok {
		for _, v := range ary {
			m.any(p, v)
		}
	}
}

func (m *matcher) value(p *Path, v *Value) {
	if v == nil {
		return
	}
	if p == nil {
		m.match(v)
		return
	}
	switch p.kind {
	case jpDocumentRoot:
		m.value(p.next, v)
	case jpAny:
		m.any(p.next, v)
	case jpMember:
		if obj, ok := v.Object(); ok {
			m.value(p.next, obj.Get(p.name))
		}
	case jpMemberAny:
		if obj, ok := v.Object(); ok {
			obj.Visit(func(_ string, v *Value) {
				m.value(p.next, v)
			})
		}
	case jpArrayLocation:
		if ary, ok := v.Array(); ok {
			from, to := p.arrayOffsets(ary)
			if from >= 0 && from < len(ary) {
				if to >= len(ary) {
					to = len(ary) - 1
				}
				for n := from; n <= to; n++ {
					m.value(p.next, ary[n])
				}
			}
		} else if m.wrap && (p.offset0 == 0 || p.offset0 == -1) {
			m.value(p.next, v)
		}
	case jpArrayLocationAny:
		if ary, ok := v.Array(); ok {
			for _, v := range ary {
				m.value(p.next, v)
			}
		}
	}
}

func (jp *Path) Match(doc *Value, wrap bool, match func(value *Value)) {
	m := matcher{
		seen: make(map[*Value]struct{}),
		f:    match,
		wrap: wrap,
	}
	m.value(jp, doc)
}

func (jp *Path) transform(v *Value, t func(pp *Path, vv *Value)) {
	if v == nil {
		return
	}
	if jp.next == nil {
		t(jp, v)
		return
	}
	switch jp.kind {
	case jpDocumentRoot:
		jp.next.transform(v, t)
	case jpMember:
		if obj, ok := v.Object(); ok {
			jp.next.transform(obj.Get(jp.name), t)
		}
	case jpArrayLocation:
		if ary, ok := v.Array(); ok {
			from, to := jp.arrayOffsets(ary)
			if from != to {
				panic("range in transformation path expression")
			}
			if from >= 0 && from < len(ary) {
				jp.next.transform(ary[from], t)
			}
		} else if jp.offset0 == 0 || jp.offset0 == -1 {
			/*
				If the path is evaluated against a value that is not an array,
				the result of the evaluation is the same as if the value had been
				wrapped in a single-element array:
			*/
			jp.next.transform(v, t)
		}
	case jpMemberAny, jpArrayLocationAny, jpAny:
		panic("wildcard in transformation path expression")
	}
}

type Transformation int

const (
	Set Transformation = iota
	Insert
	Replace
	Remove
)

func ApplyTransform(t Transformation, doc *Value, paths []*Path, values []*Value) error {
	if t != Remove && len(paths) != len(values) {
		panic("missing Values for transformation")
	}
	for i, p := range paths {
		transform := func(pp *Path, vv *Value) {
			switch pp.kind {
			case jpArrayLocation:
				if ary, ok := vv.Array(); ok {
					from, to := pp.arrayOffsets(ary)
					if from != to {
						return
					}
					if t == Remove {
						vv.DelArrayItem(from)
					} else {
						vv.SetArrayItem(from, values[i], t)
					}
				}
			case jpMember:
				if obj, ok := vv.Object(); ok {
					if t == Remove {
						obj.Del(pp.name)
					} else {
						obj.Set(pp.name, values[i], t)
					}
				}
			}
		}
		p.transform(doc, transform)
	}
	return nil
}

func MatchPath(rawJSON, rawPath []byte, match func(value *Value)) error {
	var p1 Parser
	doc, err := p1.ParseBytes(rawJSON)
	if err != nil {
		return err
	}

	var p2 PathParser
	jp, err := p2.ParseBytes(rawPath)
	if err != nil {
		return err
	}

	jp.Match(doc, true, match)
	return nil
}

var (
	unicodeRangeIDNeg      = rangetable.Merge(unicode.Pattern_Syntax, unicode.Pattern_White_Space)
	unicodeRangeIDStartPos = rangetable.Merge(unicode.Letter, unicode.Nl, unicode.Other_ID_Start)
	unicodeRangeIDContPos  = rangetable.Merge(unicodeRangeIDStartPos, unicode.Mn, unicode.Mc, unicode.Nd, unicode.Pc, unicode.Other_ID_Continue)
)

func isIDStartUnicode(r rune) bool {
	return unicode.Is(unicodeRangeIDStartPos, r) && !unicode.Is(unicodeRangeIDNeg, r)
}

func isIDPartUnicode(r rune) bool {
	return unicode.Is(unicodeRangeIDContPos, r) && !unicode.Is(unicodeRangeIDNeg, r) || r == '\u200C' || r == '\u200D'
}

func isIdentifierStart(chr rune) bool {
	return chr == '$' || chr == '_' || chr == '\\' ||
		'a' <= chr && chr <= 'z' || 'A' <= chr && chr <= 'Z' ||
		chr >= utf8.RuneSelf && isIDStartUnicode(chr)
}

func isIdentifierPart(chr rune) bool {
	return chr == '$' || chr == '_' || chr == '\\' ||
		'a' <= chr && chr <= 'z' || 'A' <= chr && chr <= 'Z' ||
		'0' <= chr && chr <= '9' ||
		chr >= utf8.RuneSelf && isIDPartUnicode(chr)
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
			return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "%v. The error is around character position %d.", err, len(in)-len(ptr))
		}
	}
}
