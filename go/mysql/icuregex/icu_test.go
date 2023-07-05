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

package icuregex_test

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/icuregex"
	"vitess.io/vitess/go/mysql/icuregex/internal/pattern"
)

var ErrSkip = errors.New("ignored test")

type Matcher int8

const (
	FuncFind Matcher = iota
	FuncMatches
	FuncLookingAt
)

type Expectation int8

const (
	Unknown Expectation = iota
	Expected
	NotExpected
)

type TestPattern struct {
	Line   string
	Lineno int

	Pattern string
	Flags   icuregex.RegexpFlag
	Options struct {
		MatchFunc  Matcher
		FindCount  int
		MatchOnly  bool
		MustError  bool
		Dump       bool
		HitEnd     Expectation
		RequireEnd Expectation
	}
	Input  string
	Groups []TestGroup
}

type TestGroup struct {
	Start, End int
}

var parsePattern = regexp.MustCompile(`<(/?)(r|[0-9]+)>`)

func (tp *TestPattern) parseFlags(line string) (string, error) {
	for len(line) > 0 {
		switch line[0] {
		case '"', '\'', '/':
			return line, nil
		case ' ', '\t':
		case 'i':
			tp.Flags |= icuregex.CaseInsensitive
		case 'x':
			tp.Flags |= icuregex.Comments
		case 's':
			tp.Flags |= icuregex.DotAll
		case 'm':
			tp.Flags |= icuregex.Multiline
		case 'e':
			tp.Flags |= icuregex.ErrorOnUnknownEscapes
		case 'D':
			tp.Flags |= icuregex.UnixLines
		case 'Q':
			tp.Flags |= icuregex.Literal
		case '2', '3', '4', '5', '6', '7', '8', '9':
			tp.Options.FindCount = int(line[0] - '0')
		case 'G':
			tp.Options.MatchOnly = true
		case 'E':
			tp.Options.MustError = true
		case 'd':
			tp.Options.Dump = true
		case 'L':
			tp.Options.MatchFunc = FuncLookingAt
		case 'M':
			tp.Options.MatchFunc = FuncMatches
		case 'v':
			tp.Options.MustError = !icuregex.BreakIteration
		case 'a', 'b':
			return "", ErrSkip
		case 'z':
			tp.Options.HitEnd = Expected
		case 'Z':
			tp.Options.HitEnd = NotExpected
		case 'y':
			tp.Options.RequireEnd = Expected
		case 'Y':
			tp.Options.RequireEnd = NotExpected
		default:
			return "", fmt.Errorf("unexpected modifier '%c'", line[0])
		}
		line = line[1:]
	}
	return "", io.ErrUnexpectedEOF
}

func (tp *TestPattern) parseMatch(orig string) error {
	input, ok := pattern.Unescape(orig)
	if !ok {
		return fmt.Errorf("failed to unquote input: %s", orig)
	}

	var detagged []rune
	var last int

	m := parsePattern.FindAllStringSubmatchIndex(input, -1)
	for _, g := range m {
		detagged = append(detagged, []rune(input[last:g[0]])...)
		last = g[1]

		closing := input[g[2]:g[3]] == "/"
		groupNum := input[g[4]:g[5]]
		if groupNum == "r" {
			return ErrSkip
		}
		num, err := strconv.Atoi(groupNum)
		if err != nil {
			return fmt.Errorf("bad group number %q: %w", groupNum, err)
		}

		if num >= len(tp.Groups) {
			grp := make([]TestGroup, num+1)
			for i := range grp {
				grp[i].Start = -1
				grp[i].End = -1
			}
			copy(grp, tp.Groups)
			tp.Groups = grp
		}

		if closing {
			tp.Groups[num].End = len(detagged)
		} else {
			tp.Groups[num].Start = len(detagged)
		}
	}

	detagged = append(detagged, []rune(input[last:])...)
	tp.Input = string(detagged)
	return nil
}

func ParseTestFile(t testing.TB, filename string) []TestPattern {
	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("failed to open test data: %v", err)
	}

	defer f.Close()
	scanner := bufio.NewScanner(f)
	var lineno int
	var patterns []TestPattern

	errFunc := func(err error) {
		if err == ErrSkip {
			return
		}
		t.Errorf("Parse error: %v\n%03d: %s", err, lineno, scanner.Text())
	}

	for scanner.Scan() {
		lineno++
		line := scanner.Text()
		line = strings.TrimSpace(line)

		if len(line) == 0 || line[0] == '#' {
			continue
		}

		var tp TestPattern
		tp.Line = line
		tp.Lineno = lineno

		idx := strings.IndexByte(line[1:], line[0])

		tp.Pattern = line[1 : idx+1]
		line, err = tp.parseFlags(line[idx+2:])
		if err != nil {
			errFunc(err)
			continue
		}

		idx = strings.IndexByte(line[1:], line[0])
		err = tp.parseMatch(line[1 : idx+1])
		if err != nil {
			errFunc(err)
			continue
		}

		patterns = append(patterns, tp)
	}

	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return patterns
}

func (tp *TestPattern) fail(t testing.TB, msg string, args ...any) bool {
	t.Helper()
	msg = fmt.Sprintf(msg, args...)
	t.Errorf("%s (in line %d)\nregexp: %s\ninput: %q\noriginal: %s", msg, tp.Lineno, tp.Pattern, tp.Input, tp.Line)
	return false
}

func (tp *TestPattern) Test(t testing.TB) bool {
	re, err := func() (re *icuregex.Pattern, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("PANIC: %v", r)
			}
		}()
		re, err = icuregex.CompileString(tp.Pattern, tp.Flags)
		return
	}()
	if err != nil {
		if tp.Options.MustError {
			return true
		}

		return tp.fail(t, "unexpected parser failure: %v", err)
	}
	if tp.Options.MustError {
		return tp.fail(t, "parse failure expected")
	}

	matcher := re.Match(tp.Input)
	var isMatch bool
	var findCount = tp.Options.FindCount
	if findCount == 0 {
		findCount = 1
	}

	for i := 0; i < findCount; i++ {
		isMatch, err = func() (bool, error) {
			defer func() {
				if r := recover(); r != nil {
					tp.fail(t, "unexpected match failure: %v", r)
				}
			}()
			switch tp.Options.MatchFunc {
			case FuncMatches:
				return matcher.Matches()
			case FuncLookingAt:
				return matcher.LookingAt()
			case FuncFind:
				return matcher.Find()
			default:
				panic("invalid MatchFunc")
			}
		}()
	}

	require.NoError(t, err)

	if !isMatch && len(tp.Groups) > 0 {
		return tp.fail(t, "Match expected, but none found.")
	}
	if isMatch && len(tp.Groups) == 0 {
		return tp.fail(t, "No match expected, but found one at position %d", matcher.Start())
	}
	if tp.Options.MatchOnly {
		return true
	}

	for i := 0; i < matcher.GroupCount(); i++ {
		expectedStart := -1
		expectedEnd := -1

		if i < len(tp.Groups) {
			expectedStart = tp.Groups[i].Start
			expectedEnd = tp.Groups[i].End
		}
		if gotStart := matcher.StartForGroup(i); gotStart != expectedStart {
			return tp.fail(t, "Incorrect start position for group %d. Expected %d, got %d", i, expectedStart, gotStart)
		}
		if gotEnd := matcher.EndForGroup(i); gotEnd != expectedEnd {
			return tp.fail(t, "Incorrect end position for group %d. Expected %d, got %d", i, expectedEnd, gotEnd)
		}
	}

	if matcher.GroupCount()+1 < len(tp.Groups) {
		return tp.fail(t, "Expected %d capture groups, found %d", len(tp.Groups)-1, matcher.GroupCount())
	}

	if tp.Options.HitEnd == Expected && !matcher.HitEnd() {
		return tp.fail(t, "HitEnd() returned false. Expected true")
	}
	if tp.Options.HitEnd == NotExpected && matcher.HitEnd() {
		return tp.fail(t, "HitEnd() returned true. Expected false")
	}

	if tp.Options.RequireEnd == Expected && !matcher.RequireEnd() {
		return tp.fail(t, "RequireEnd() returned false. Expected true")
	}
	if tp.Options.RequireEnd == NotExpected && matcher.RequireEnd() {
		return tp.fail(t, "RequireEnd() returned true. Expected false")
	}

	return true
}

func TestICU(t *testing.T) {
	pats := ParseTestFile(t, "testdata/regextst.txt")

	var valid int

	for _, p := range pats {
		if p.Test(t) {
			valid++
		}
	}

	t.Logf("%d/%d (%.02f)", valid, len(pats), float64(valid)/float64(len(pats)))
}

func TestICUExtended(t *testing.T) {
	// This tests additional cases that aren't covered in the
	// copied ICU test suite.
	pats := ParseTestFile(t, "testdata/regextst_extended.txt")

	var valid int

	for _, p := range pats {
		if p.Test(t) {
			valid++
		}
	}

	t.Logf("%d/%d (%.02f)", valid, len(pats), float64(valid)/float64(len(pats)))
}

func TestCornerCases(t *testing.T) {
	var cases = []struct {
		Pattern string
		Input   string
		Flags   icuregex.RegexpFlag
		Match   bool
	}{
		{`xyz$`, "xyz\n", 0, true},
		{`a*+`, "abbxx", 0, true},
		{`(ABC){1,2}+ABC`, "ABCABCABC", 0, true},
		{`(ABC){2,3}+ABC`, "ABCABCABC", 0, false},
		{`(abc)*+a`, "abcabcabc", 0, false},
		{`(abc)*+a`, "abcabcab", 0, true},
		{`a\N{LATIN SMALL LETTER B}c`, "abc", 0, true},
		{`a.b`, "a\rb", icuregex.UnixLines, true},
		{`a.b`, "a\rb", 0, false},
		{`(?d)abc$`, "abc\r", 0, false},
		{`[ \b]`, "b", 0, true},
		{`[abcd-\N{LATIN SMALL LETTER G}]+`, "xyz-abcdefghij-", 0, true},
		{`[[abcd]&&[ac]]+`, "bacacd", 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.Pattern, func(t *testing.T) {
			_, err := icuregex.CompileString(tc.Pattern, tc.Flags)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOne(t *testing.T) {
	const Pattern = `\p{CaseIgnorable}`
	const Input = "foo.bar"
	const Flags = 0

	re, err := icuregex.CompileString(Pattern, Flags)
	if err != nil {
		t.Fatalf("compilation failed: %v", err)
	}

	re.Dump(os.Stderr)

	m := icuregex.NewMatcher(re)
	m.Dumper(os.Stderr)
	m.ResetString(Input)
	found, err := m.Find()
	require.NoError(t, err)
	t.Logf("match = %v", found)
}
