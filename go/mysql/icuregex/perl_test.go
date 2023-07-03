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
	"bufio"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestPerl(t *testing.T) {
	f, err := os.Open("testdata/re_tests.txt")
	if err != nil {
		t.Fatalf("failed to open test data: %v", err)
	}
	defer f.Close()

	flagPat := MustCompileString(`('?)(.*)\1(.*)`, 0)
	flagMat := NewMatcher(flagPat)

	groupsPat := MustCompileString(`\$([+\-])\[(\d+)\]`, 0)
	groupsMat := NewMatcher(groupsPat)

	cgPat := MustCompileString(`\$(\d+)`, 0)
	cgMat := NewMatcher(cgPat)

	group := func(m *Matcher, idx int) string {
		g, _ := m.Group(idx)
		return g
	}

	lookingAt := func(m *Matcher) bool {
		ok, err := m.LookingAt()
		if err != nil {
			t.Fatalf("failed to match with LookingAt(): %v", err)
		}
		return ok
	}

	replacer := strings.NewReplacer(
		`${bang}`, "!",
		`${nulnul}`, "\x00\x00",
		`${ffff}`, "\uffff",
	)

	scanner := bufio.NewScanner(f)
	var lineno int

	for scanner.Scan() {
		lineno++
		fields := strings.Split(scanner.Text(), "\t")

		flagMat.ResetString(fields[0])
		ok, _ := flagMat.Matches()
		if !ok {
			t.Fatalf("could not match pattern+flags (line %d)", lineno)
		}

		pattern, _ := flagMat.Group(2)
		pattern = replacer.Replace(pattern)

		flagStr, _ := flagMat.Group(3)
		var flags RegexpFlag
		if strings.IndexByte(flagStr, 'i') >= 0 {
			flags |= CaseInsensitive
		}
		if strings.IndexByte(flagStr, 'm') >= 0 {
			flags |= Multiline
		}
		if strings.IndexByte(flagStr, 'x') >= 0 {
			flags |= Comments
		}

		testPat, err := CompileString(pattern, flags)
		if err != nil {
			if cerr, ok := err.(*CompileError); ok && cerr.Code == Unimplemented {
				continue
			}
			if strings.IndexByte(fields[2], 'c') == -1 && strings.IndexByte(fields[2], 'i') == -1 {
				t.Errorf("line %d: ICU error %q", lineno, err)
			}
			continue
		}

		if strings.IndexByte(fields[2], 'i') >= 0 {
			continue
		}
		if strings.IndexByte(fields[2], 'c') >= 0 {
			t.Errorf("line %d: expected error", lineno)
			continue
		}

		matchString := fields[1]
		matchString = replacer.Replace(matchString)
		matchString = strings.ReplaceAll(matchString, `\n`, "\n")

		testMat := testPat.Match(matchString)
		found, _ := testMat.Find()
		expected := strings.IndexByte(fields[2], 'y') >= 0

		if expected != found {
			t.Errorf("line %d: expected %v, found %v", lineno, expected, found)
			continue
		}

		if !found {
			continue
		}

		var result []byte
		var perlExpr = fields[3]

		for len(perlExpr) > 0 {
			groupsMat.ResetString(perlExpr)
			cgMat.ResetString(perlExpr)

			switch {
			case strings.HasPrefix(perlExpr, "$&"):
				result = append(result, group(testMat, 0)...)
				perlExpr = perlExpr[2:]

			case lookingAt(groupsMat):
				groupNum, err := strconv.ParseInt(group(groupsMat, 2), 10, 32)
				if err != nil {
					t.Fatalf("failed to parse Perl pattern: %v", err)
				}

				var matchPosition int
				if group(groupsMat, 1) == "+" {
					matchPosition = testMat.EndForGroup(int(groupNum))
				} else {
					matchPosition = testMat.StartForGroup(int(groupNum))
				}
				if matchPosition != -1 {
					result = strconv.AppendInt(result, int64(matchPosition), 10)
				}

				perlExpr = perlExpr[groupsMat.EndForGroup(0):]

			case lookingAt(cgMat):
				groupNum, err := strconv.ParseInt(group(cgMat, 1), 10, 32)
				if err != nil {
					t.Fatalf("failed to parse Perl pattern: %v", err)
				}
				result = append(result, group(testMat, int(groupNum))...)
				perlExpr = perlExpr[cgMat.EndForGroup(0):]

			case strings.HasPrefix(perlExpr, "@-"):
				for i := 0; i <= testMat.GroupCount(); i++ {
					if i > 0 {
						result = append(result, ' ')
					}
					result = strconv.AppendInt(result, int64(testMat.StartForGroup(i)), 10)
				}
				perlExpr = perlExpr[2:]

			case strings.HasPrefix(perlExpr, "@+"):
				for i := 0; i <= testMat.GroupCount(); i++ {
					if i > 0 {
						result = append(result, ' ')
					}
					result = strconv.AppendInt(result, int64(testMat.EndForGroup(i)), 10)
				}
				perlExpr = perlExpr[2:]

			case strings.HasPrefix(perlExpr, "\\"):
				if len(perlExpr) > 1 {
					perlExpr = perlExpr[1:]
				}
				c := perlExpr[0]
				switch c {
				case 'n':
					c = '\n'
				}
				result = append(result, c)
				perlExpr = perlExpr[1:]

			default:
				result = append(result, perlExpr[0])
				perlExpr = perlExpr[1:]
			}
		}

		var expectedS string
		if len(fields) > 4 {
			expectedS = fields[4]
			expectedS = replacer.Replace(expectedS)
			expectedS = strings.ReplaceAll(expectedS, `\n`, "\n")
		}

		if expectedS != string(result) {
			t.Errorf("line %d: Incorrect Perl expression results for %s\nwant: %q\ngot: %q", lineno, pattern, expectedS, result)
		}
	}
}
