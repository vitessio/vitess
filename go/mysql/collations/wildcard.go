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

// The wildcard matching code in Vitess uses two different implementations for wildcard algorithms,
// as seen on https://en.wikipedia.org/wiki/Matching_wildcards
//
// The main implementation is based on the logic in INN (https://inn.eyrie.org/trac/browser/trunk/lib/uwildmat.c),
// and is originally MIT licensed. This is a recursive matching algorithm with important optimizations, as explained
// on the Wikipedia page: it is a traditional recursion algorithm with 3 return values for match, no match, and
// impossible match, which greatly stops the depth of the recursion tree. It also only tries to target the ending
// codepoint at the end of a 'star' match, which again cuts the recursion depth.
//
// In practice, this results in a very efficient algorithm which performs great in real world cases, however,
// as just explained, it DOES recurse, which may be an issue when the input pattern is complex enough to cause
// deep recursion.
//
// To prevent Vitess instances from crashing because of stack overflows, we've added a stack guard to the algorithm,
// controlled by the wildcardRecursionDepth constant. If the recursion limit is reached, the match will fail --
// potentially leading to wrong results for the algorithm.
//
// If accuracy is of upmost importance, the wildcardRecursionDepth constant can be set to 0, in which case Vitess
// will use an alternative iterative algorithm, based on a public domain algorithm by Alessandro Cantatore
// (seen in http://xoomer.virgilio.it/acantato/dev/wildcard/wildmatch.html). This algorithm is much simpler and does
// not recurse, however it is significantly slower than our recursive implementation (~25% slower in our benchmarks).
//
// Because of this, we intend to enable the recursive algorithm by default.

package collations

import (
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/charset"
)

type match byte

const (
	matchOK match = iota
	matchFail
	matchOver
)

// wildcardRecursionDepth is the maximum amount of recursive calls that can be performed when
// matching a wildcard. If set to 0, the default wildcard matcher will use an alternative algorithm
// that does not use recursion.
const wildcardRecursionDepth = 32

// patternMatchOne is a special value for compiled patterns which matches a single char (it usually replaces '_' or '?')
const patternMatchOne = -128

// patternMatchMany is a special value for compiled pattern that matches any amount of chars (it usually replaces '%' or '*')
const patternMatchMany = -256

// nopMatcher is an implementation of WildcardPattern that never matches anything.
// It is returned when we detect that a provided wildcard pattern cannot match anything
type nopMatcher struct{}

func (nopMatcher) Match(_ []byte) bool {
	return false
}

// emptyMatcher is an implementation of WildcardPattern that only matches the empty string
type emptyMatcher struct{}

func (emptyMatcher) Match(in []byte) bool {
	return len(in) == 0
}

// fastMatcher is an implementation of WildcardPattern that uses a collation's Collate method
// to perform wildcard matching.
// It is returned:
//   - when the wildcard pattern has no wildcard characters at all
//   - when the wildcard pattern has a single '%' (patternMatchMany) and it is the very last
//     character of the pattern (in this case, we set isPrefix to true to use prefix-match collation)
type fastMatcher struct {
	collate  func(left, right []byte, isPrefix bool) int
	pattern  []byte
	isPrefix bool
}

func (cm *fastMatcher) Match(in []byte) bool {
	return cm.collate(in, cm.pattern, cm.isPrefix) == 0
}

// unicodeWildcard is an implementation of WildcardPattern for multibyte charsets;
// it is used for all UCA collations, multibyte collations and all Unicode-based collations
type unicodeWildcard struct {
	equals  func(a, b rune) bool
	charset charset.Charset
	pattern []rune
}

func newUnicodeWildcardMatcher(
	cs charset.Charset,
	equals func(a rune, b rune) bool,
	collate func(left []byte, right []byte, isPrefix bool) int,
	pat []byte, chOne, chMany, chEsc rune,
) WildcardPattern {
	var escape bool
	var chOneCount, chManyCount, chEscCount int
	var parsedPattern = make([]rune, 0, len(pat))
	var patOriginal = pat

	if chOne == 0 {
		chOne = '_'
	}
	if chMany == 0 {
		chMany = '%'
	}
	if chEsc == 0 {
		chEsc = '\\'
	}

	for len(pat) > 0 {
		cp, width := cs.DecodeRune(pat)
		if cp == charset.RuneError && width < 3 {
			return nopMatcher{}
		}
		pat = pat[width:]

		if escape {
			parsedPattern = append(parsedPattern, cp)
			escape = false
			continue
		}

		switch cp {
		case chOne:
			chOneCount++
			parsedPattern = append(parsedPattern, patternMatchOne)
		case chMany:
			if len(parsedPattern) > 0 && parsedPattern[len(parsedPattern)-1] == patternMatchMany {
				continue
			}
			chManyCount++
			parsedPattern = append(parsedPattern, patternMatchMany)
		case chEsc:
			chEscCount++
			escape = true
		default:
			parsedPattern = append(parsedPattern, cp)
		}
	}
	if escape {
		parsedPattern = append(parsedPattern, chEsc)
	}

	// if we have a collation callback, we can detect some common cases for patterns
	// here and optimize them away without having to return a full WildcardPattern
	if collate != nil {
		if len(parsedPattern) == 0 {
			return emptyMatcher{}
		}
		if chOneCount == 0 && chEscCount == 0 {
			if chManyCount == 0 {
				return &fastMatcher{
					collate:  collate,
					pattern:  patOriginal,
					isPrefix: false,
				}
			}
			if chManyCount == 1 && chMany < utf8.RuneSelf && parsedPattern[len(parsedPattern)-1] == chMany {
				return &fastMatcher{
					collate:  collate,
					pattern:  patOriginal[:len(patOriginal)-1],
					isPrefix: true,
				}
			}
		}
	}

	return &unicodeWildcard{
		equals:  equals,
		charset: cs,
		pattern: parsedPattern,
	}
}

func (wc *unicodeWildcard) matchIter(str []byte, pat []rune) bool {
	var s []byte
	var p []rune
	var star = false
	var cs = wc.charset

retry:
	s = str
	p = pat
	for len(s) > 0 {
		var p0 rune
		if len(p) > 0 {
			p0 = p[0]
		}

		switch p0 {
		case patternMatchOne:
			c0, width := cs.DecodeRune(s)
			if c0 == charset.RuneError && width < 3 {
				return false
			}
			s = s[width:]
		case patternMatchMany:
			star = true
			str = s
			pat = p[1:]
			if len(pat) == 0 {
				return true
			}
			goto retry
		default:
			c0, width := cs.DecodeRune(s)
			if c0 == charset.RuneError && width < 3 {
				return false
			}
			if !wc.equals(c0, p0) {
				goto starCheck
			}
			s = s[width:]
		}
		p = p[1:]
	}
	return len(p) == 0 || (len(p) == 1 && p[0] == patternMatchMany)

starCheck:
	if !star {
		return false
	}
	if len(str) > 0 {
		c0, width := cs.DecodeRune(str)
		if c0 == charset.RuneError && width < 3 {
			return false
		}
		str = str[width:]
	}
	goto retry
}

func (wc *unicodeWildcard) Match(in []byte) bool {
	if wildcardRecursionDepth == 0 {
		return wc.matchIter(in, wc.pattern)
	}
	return wc.matchRecursive(in, wc.pattern, 0) == matchOK
}

func (wc *unicodeWildcard) matchMany(in []byte, pat []rune, depth int) match {
	var cs = wc.charset
	var p0 rune

many:
	if len(pat) == 0 {
		return matchOK
	}
	p0 = pat[0]
	pat = pat[1:]

	switch p0 {
	case patternMatchMany:
		goto many
	case patternMatchOne:
		cpIn, width := cs.DecodeRune(in)
		if cpIn == charset.RuneError && width < 3 {
			return matchFail
		}
		in = in[width:]
		goto many
	}

	if len(in) == 0 {
		return matchOver
	}

retry:
	var width int
	for len(in) > 0 {
		var cpIn rune
		cpIn, width = cs.DecodeRune(in)
		if cpIn == charset.RuneError && width < 3 {
			return matchFail
		}
		if wc.equals(cpIn, p0) {
			break
		}
		in = in[width:]
	}

	if len(in) == 0 {
		return matchOver
	}
	in = in[width:]

	m := wc.matchRecursive(in, pat, depth+1)
	if m == matchFail {
		goto retry
	}
	return m
}

func (wc *unicodeWildcard) matchRecursive(in []byte, pat []rune, depth int) match {
	if depth >= wildcardRecursionDepth {
		return matchFail
	}

	var cs = wc.charset
	for len(pat) > 0 {
		if pat[0] == patternMatchMany {
			return wc.matchMany(in, pat[1:], depth)
		}

		cpIn, width := cs.DecodeRune(in)
		if cpIn == charset.RuneError && width < 3 {
			return matchFail
		}

		switch {
		case pat[0] == patternMatchOne:
		case wc.equals(pat[0], cpIn):
		default:
			return matchFail
		}

		in = in[width:]
		pat = pat[1:]
	}

	if len(in) == 0 {
		return matchOK
	}
	return matchFail
}

// eightbitWildcard is an implementation of WildcardPattern used for 8-bit charsets.
// It is used for all 8-bit encodings.
type eightbitWildcard struct {
	sort    *[256]byte
	pattern []int16
}

func newEightbitWildcardMatcher(
	sort *[256]byte,
	collate func(left []byte, right []byte, isPrefix bool) int,
	pat []byte, chOneRune, chManyRune, chEscRune rune,
) WildcardPattern {
	var escape bool
	var parsedPattern = make([]int16, 0, len(pat))
	var chOne, chMany, chEsc byte = '_', '%', '\\'
	var chOneCount, chManyCount, chEscCount int

	if chOneRune > 255 || chManyRune > 255 || chEscRune > 255 {
		return nopMatcher{}
	}
	if chOneRune != 0 {
		chOne = byte(chOneRune)
	}
	if chManyRune != 0 {
		chMany = byte(chManyRune)
	}
	if chEscRune != 0 {
		chEsc = byte(chEscRune)
	}

	for _, ch := range pat {
		if escape {
			parsedPattern = append(parsedPattern, int16(ch))
			escape = false
			continue
		}

		switch ch {
		case chOne:
			chOneCount++
			parsedPattern = append(parsedPattern, patternMatchOne)
		case chMany:
			if len(parsedPattern) > 0 && parsedPattern[len(parsedPattern)-1] == patternMatchMany {
				continue
			}
			chManyCount++
			parsedPattern = append(parsedPattern, patternMatchMany)
		case chEsc:
			chEscCount++
			escape = true
		default:
			parsedPattern = append(parsedPattern, int16(ch))
		}
	}
	if escape {
		parsedPattern = append(parsedPattern, int16(chEsc))
	}

	// if we have a collation callback, we can detect some common cases for patterns
	// here and optimize them away without having to return a full WildcardPattern
	if collate != nil {
		if len(parsedPattern) == 0 {
			return emptyMatcher{}
		}
		if chOneCount == 0 && chEscCount == 0 {
			if chManyCount == 0 {
				return &fastMatcher{
					collate:  collate,
					pattern:  pat,
					isPrefix: false,
				}
			}
			if chManyCount == 1 && pat[len(pat)-1] == chMany {
				return &fastMatcher{
					collate:  collate,
					pattern:  pat[:len(pat)-1],
					isPrefix: true,
				}
			}
		}
	}

	return &eightbitWildcard{
		sort:    sort,
		pattern: parsedPattern,
	}
}

func (wc *eightbitWildcard) Match(in []byte) bool {
	if wildcardRecursionDepth == 0 {
		return wc.matchIter(in, wc.pattern)
	}
	return wc.matchRecursive(in, wc.pattern, 0) == matchOK
}

func (wc *eightbitWildcard) matchMany(in []byte, pat []int16, depth int) match {
	var p0 int16

many:
	if len(pat) == 0 {
		return matchOK
	}

	p0 = pat[0]
	pat = pat[1:]

	switch p0 {
	case patternMatchMany:
		goto many
	case patternMatchOne:
		if len(in) == 0 {
			return matchFail
		}
		in = in[1:]
		goto many
	}

	if len(in) == 0 {
		return matchOver
	}

retry:
	for len(in) > 0 {
		if wc.sort[in[0]] == wc.sort[byte(p0)] {
			break
		}
		in = in[1:]
	}
	if len(in) == 0 {
		return matchOver
	}
	in = in[1:]

	m := wc.matchRecursive(in, pat, depth+1)
	if m == matchFail {
		goto retry
	}
	return m
}

func (wc *eightbitWildcard) matchRecursive(in []byte, pat []int16, depth int) match {
	if depth >= wildcardRecursionDepth {
		return matchFail
	}
	for len(pat) > 0 {
		if pat[0] == patternMatchMany {
			return wc.matchMany(in, pat[1:], depth)
		}

		if len(in) == 0 {
			return matchFail
		}

		switch {
		case pat[0] == patternMatchOne:
		case wc.sort[byte(pat[0])] == wc.sort[in[0]]:
		default:
			return matchFail
		}

		in = in[1:]
		pat = pat[1:]
	}

	if len(in) == 0 {
		return matchOK
	}
	return matchFail
}

func (wc *eightbitWildcard) matchIter(str []byte, pat []int16) bool {
	var s []byte
	var p []int16
	var star = false

retry:
	s = str
	p = pat
	for len(s) > 0 {
		var p0 int16
		if len(p) > 0 {
			p0 = p[0]
		}

		switch p0 {
		case patternMatchOne:
			break
		case patternMatchMany:
			star = true
			str = s
			pat = p[1:]
			if len(pat) == 0 {
				return true
			}
			goto retry
		default:
			if wc.sort[byte(p0)] != wc.sort[s[0]] {
				goto starCheck
			}
		}
		s = s[1:]
		p = p[1:]
	}
	return len(p) == 0 || (len(p) == 1 && p[0] == patternMatchMany)

starCheck:
	if !star {
		return false
	}
	str = str[1:]
	goto retry
}
