/*
© 2016 and later: Unicode, Inc. and others.
Copyright (C) 2004-2015, International Business Machines Corporation and others.
Copyright 2023 The Vitess Authors.

This file contains code derived from the Unicode Project's ICU library.
License & terms of use for the original code: http://www.unicode.org/copyright.html

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

package icuregex

import (
	"vitess.io/vitess/go/mysql/icuregex/internal/uset"
)

type Pattern struct {
	pattern string
	flags   RegexpFlag

	compiledPat []instruction
	literalText []rune

	sets []*uset.UnicodeSet

	minMatchLen int32
	frameSize   int
	dataSize    int

	groupMap []int32

	startType        startOfMatch
	initialStringIdx int
	initialStringLen int
	initialChars     *uset.UnicodeSet
	initialChar      rune
	needsAltInput    bool

	namedCaptureMap map[string]int
}

func NewPattern(flags RegexpFlag) *Pattern {
	return &Pattern{
		flags:        flags,
		initialChars: uset.New(),
		// Slot zero of the vector of sets is reserved.  Fill it here.
		sets: []*uset.UnicodeSet{nil},
	}
}

func MustCompileString(in string, flags RegexpFlag) *Pattern {
	pat, err := CompileString(in, flags)
	if err != nil {
		panic(err)
	}
	return pat
}

func Compile(in []rune, flags RegexpFlag) (*Pattern, error) {
	pat := NewPattern(flags)
	cmp := newCompiler(pat)
	if err := cmp.compile(in); err != nil {
		return nil, err
	}
	return pat, nil
}

func CompileString(in string, flags RegexpFlag) (*Pattern, error) {
	pat := NewPattern(flags)
	cmp := newCompiler(pat)
	if err := cmp.compile([]rune(in)); err != nil {
		return nil, err
	}
	return pat, nil
}

func (p *Pattern) Match(input string) *Matcher {
	m := NewMatcher(p)
	m.ResetString(input)
	return m
}

type RegexpFlag int32

const (
	/**  Enable case insensitive matching.  @stable ICU 2.4 */
	CaseInsensitive RegexpFlag = 2

	/**  Allow white space and comments within patterns  @stable ICU 2.4 */
	Comments RegexpFlag = 4

	/**  If set, '.' matches line terminators,  otherwise '.' matching stops at line end.
	 *  @stable ICU 2.4 */
	DotAll RegexpFlag = 32

	/**  If set, treat the entire pattern as a literal string.
	 *  Metacharacters or escape sequences in the input sequence will be given
	 *  no special meaning.
	 *
	 *  The flag UREGEX_CASE_INSENSITIVE retains its impact
	 *  on matching when used in conjunction with this flag.
	 *  The other flags become superfluous.
	 *
	 * @stable ICU 4.0
	 */
	Literal RegexpFlag = 16

	/**   Control behavior of "$" and "^"
	 *    If set, recognize line terminators within string,
	 *    otherwise, match only at start and end of input string.
	 *   @stable ICU 2.4 */
	Multiline RegexpFlag = 8

	/**   Unix-only line endings.
	 *   When this mode is enabled, only \\u000a is recognized as a line ending
	 *    in the behavior of ., ^, and $.
	 *   @stable ICU 4.0
	 */
	UnixLines RegexpFlag = 1

	/**  Unicode word boundaries.
	 *     If set, \b uses the Unicode TR 29 definition of word boundaries.
	 *     Warning: Unicode word boundaries are quite different from
	 *     traditional regular expression word boundaries.  See
	 *     http://unicode.org/reports/tr29/#Word_Boundaries
	 *     @stable ICU 2.8
	 */
	UWord RegexpFlag = 256

	/**  Error on Unrecognized backslash escapes.
	 *     If set, fail with an error on patterns that contain
	 *     backslash-escaped ASCII letters without a known special
	 *     meaning.  If this flag is not set, these
	 *     escaped letters represent themselves.
	 *     @stable ICU 4.0
	 */
	ErrorOnUnknownEscapes RegexpFlag = 512
)
