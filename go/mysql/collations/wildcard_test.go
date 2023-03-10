/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License"},
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package collations

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/collations/charset"
)

type wildcardtest struct {
	in, pat string
	match   bool
}

func testWildcardMatches(t *testing.T, collName string, chOne, chMany, chEsc rune, cases []wildcardtest) {
	t.Run(collName, func(t *testing.T) {
		coll := testcollation(t, collName)
		for _, tc := range cases {
			pat := coll.Wildcard([]byte(tc.pat), chOne, chMany, chEsc)
			match := pat.Match([]byte(tc.in))
			assert.Equal(t, tc.match, match, "%q LIKE %q = %v (expected %v)", tc.in, tc.pat, match, tc.match)

		}
	})
}

func TestLikeMatches(t *testing.T) {
	testWildcardMatches(t, "utf8mb4_0900_ai_ci", 0, 0, 0, []wildcardtest{
		{"abc", "abc", true},
		{"Abc", "aBc", true},
		{"abc", "_bc", true},
		{"abc", "a_c", true},
		{"abc", "ab_", true},
		{"abc", "%c", true},
		{"abc", "a%c", true},
		{"abc", "a%", true},
		{"abcdef", "a%d_f", true},
		{"abcdefg", "a%d%g", true},
		{"a\\", "a\\", true},
		{"aa\\", "a%\\", true},
		{"Y", "\u00dd", true},
		{"abcd", "abcde", false},
		{"abcde", "abcd", false},
		{"abcde", "a%f", false},
		{"abcdef", "a%%f", true},
		{"abcd", "a__d", true},
		{"abcd", "a\\bcd", true},
		{"a\\bcd", "abcd", false},
		{"abdbcd", "a%cd", true},
		{"abecd", "a%bd", false},
	})

	testWildcardMatches(t, "utf8mb4_0900_as_cs", 0, 0, 0, []wildcardtest{
		{"abc", "abc", true},
		{"Abc", "aBc", false},
		{"abc", "_bc", true},
		{"abc", "a_c", true},
		{"abc", "ab_", true},
		{"abc", "%c", true},
		{"abc", "a%c", true},
		{"abc", "a%", true},
		{"abcdef", "a%d_f", true},
		{"abcdefg", "a%d%g", true},
		{"a\\", "a\\", true},
		{"aa\\", "a%\\", true},
		{"Y", "\u00dd", false},
		{"abcd", "abcde", false},
		{"abcde", "abcd", false},
		{"abcde", "a%f", false},
		{"abcdef", "a%%f", true},
		{"abcd", "a__d", true},
		{"abcd", "a\\bcd", true},
		{"a\\bcd", "abcd", false},
		{"abdbcd", "a%cd", true},
		{"abecd", "a%bd", false},
	})

	testWildcardMatches(t, "utf8mb4_0900_as_ci", 0, 0, 0, []wildcardtest{
		{"ǎḄÇ", "Ǎḅç", true},
		{"ÁḆĈ", "Ǎḅç", false},
		{"ǍBc", "_bc", true},
		{"Aḅc", "a_c", true},
		{"Abç", "ab_", true},
		{"Ǎḅç", "%ç", true},
		{"Ǎḅç", "ǎ%Ç", true},
		{"aḅç", "a%", true},
		{"Ǎḅçdef", "ǎ%d_f", true},
		{"Ǎḅçdefg", "ǎ%d%g", true},
		{"ǎ\\", "Ǎ\\", true},
		{"ǎa\\", "Ǎ%\\", true},
		{"Y", "\u00dd", false},
		{"abcd", "Ǎḅçde", false},
		{"abcde", "Ǎḅçd", false},
		{"Ǎḅçde", "a%f", false},
		{"Ǎḅçdef", "ǎ%%f", true},
		{"Ǎḅçd", "ǎ__d", true},
		{"Ǎḅçd", "ǎ\\ḄÇd", true},
		{"a\\bcd", "Ǎḅçd", false},
		{"Ǎḅdbçd", "ǎ%Çd", true},
		{"Ǎḅeçd", "a%bd", false},
	})
}

// from http://developforperformance.com/MatchingWildcards_AnImprovedAlgorithmForBigData.html
// Copyright 2018 IBM Corporation
// Licensed under the Apache License, Version 2.0
var wildcardTestCases = []wildcardtest{
	{"Hi", "Hi*", true},
	{"abc", "ab*d", false},
	{"abcccd", "*ccd", true},
	{"mississipissippi", "*issip*ss*", true},
	{"xxxx*zzzzzzzzy*f", "xxxx*zzy*fffff", false},
	{"xxxx*zzzzzzzzy*f", "xxx*zzy*f", true},
	{"xxxxzzzzzzzzyf", "xxxx*zzy*fffff", false},
	{"xxxxzzzzzzzzyf", "xxxx*zzy*f", true},
	{"xyxyxyzyxyz", "xy*z*xyz", true},
	{"mississippi", "*sip*", true},
	{"xyxyxyxyz", "xy*xyz", true},
	{"mississippi", "mi*sip*", true},
	{"ababac", "*abac*", true},
	{"ababac", "*abac*", true},
	{"aaazz", "a*zz*", true},
	{"a12b12", "*12*23", false},
	{"a12b12", "a12b", false},
	{"a12b12", "*12*12*", true},
	{"caaab", "*a?b", true},
	{"*", "*", true},
	{"a*abab", "a*b", true},
	{"a*r", "a*", true},
	{"a*ar", "a*aar", false},
	{"XYXYXYZYXYz", "XY*Z*XYz", true},
	{"missisSIPpi", "*SIP*", true},
	{"mississipPI", "*issip*PI", true},
	{"xyxyxyxyz", "xy*xyz", true},
	{"miSsissippi", "mi*sip*", true},
	{"miSsissippi", "mi*Sip*", false},
	{"abAbac", "*Abac*", true},
	{"abAbac", "*Abac*", true},
	{"aAazz", "a*zz*", true},
	{"A12b12", "*12*23", false},
	{"a12B12", "*12*12*", true},
	{"oWn", "*oWn*", true},
	{"bLah", "bLah", true},
	{"bLah", "bLaH", false},
	{"a", "*?", true},
	{"ab", "*?", true},
	{"abc", "*?", true},
	{"a", "??", false},
	{"ab", "?*?", true},
	{"ab", "*?*?*", true},
	{"abc", "?**?*?", true},
	{"abc", "?**?*&?", false},
	{"abcd", "?b*??", true},
	{"abcd", "?a*??", false},
	{"abcd", "?**?c?", true},
	{"abcd", "?**?d?", false},
	{"abcde", "?*b*?*d*?", true},
	{"bLah", "bL?h", true},
	{"bLaaa", "bLa?", false},
	{"bLah", "bLa?", true},
	{"bLaH", "?Lah", false},
	{"bLaH", "?LaH", true},
	{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab", "a*a*a*a*a*a*aa*aaa*a*a*b", true},
	{"abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", "*a*b*ba*ca*a*aa*aaa*fa*ga*b*", true},
	{"abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", "*a*b*ba*ca*a*x*aaa*fa*ga*b*", false},
	{"abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", "*a*b*ba*ca*aaaa*fa*ga*gggg*b*", false},
	{"abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", "*a*b*ba*ca*aaaa*fa*ga*ggg*b*", true},
	{"aaabbaabbaab", "*aabbaa*a*", true},
	{"a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*", "a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*", true},
	{"aaaaaaaaaaaaaaaaa", "*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*", true},
	{"aaaaaaaaaaaaaaaa", "*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*", false},
	{"abc*abcd*abcde*abcdef*abcdefg*abcdefgh*abcdefghi*abcdefghij*abcdefghijk*abcdefghijkl*abcdefghijklm*abcdefghijklmn", "abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*", false},
	{"abc*abcd*abcde*abcdef*abcdefg*abcdefgh*abcdefghi*abcdefghij*abcdefghijk*abcdefghijkl*abcdefghijklm*abcdefghijklmn", "abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*", true},
	{"abc*abcd*abcd*abc*abcd", "abc*abc*abc*abc*abc", false},
	{"abc*abcd*abcd*abc*abcd*abcd*abc*abcd*abc*abc*abcd", "abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abcd", true},
	{"abc", "********a********b********c********", true},
	{"********a********b********c********", "abc", false},
	{"abc", "********a********b********b********", false},
	{"*abc*", "***a*b*c***", true},
	{"", "?", false},
	{"", "*?", false},
	{"", "", true},
	{"a", "", false},

	{"abc", "abd", false},
	{"abcccd", "abcccd", true},
	{"mississipissippi", "mississipissippi", true},
	{"xxxxzzzzzzzzyf", "xxxxzzzzzzzzyfffff", false},
	{"xxxxzzzzzzzzyf", "xxxxzzzzzzzzyf", true},
	{"xxxxzzzzzzzzyf", "xxxxzzy.fffff", false},
	{"xxxxzzzzzzzzyf", "xxxxzzzzzzzzyf", true},
	{"xyxyxyzyxyz", "xyxyxyzyxyz", true},
	{"mississippi", "mississippi", true},
	{"xyxyxyxyz", "xyxyxyxyz", true},
	{"m ississippi", "m ississippi", true},
	{"ababac", "ababac?", false},
	{"dababac", "ababac", false},
	{"aaazz", "aaazz", true},
	{"a12b12", "1212", false},
	{"a12b12", "a12b", false},
	{"a12b12", "a12b12", true},
	{"n", "n", true},
	{"aabab", "aabab", true},
	{"ar", "ar", true},
	{"aar", "aaar", false},
	{"XYXYXYZYXYz", "XYXYXYZYXYz", true},
	{"missisSIPpi", "missisSIPpi", true},
	{"mississipPI", "mississipPI", true},
	{"xyxyxyxyz", "xyxyxyxyz", true},
	{"miSsissippi", "miSsissippi", true},
	{"miSsissippi", "miSsisSippi", false},
	{"abAbac", "abAbac", true},
	{"abAbac", "abAbac", true},
	{"aAazz", "aAazz", true},
	{"A12b12", "A12b123", false},
	{"a12B12", "a12B12", true},
	{"oWn", "oWn", true},
	{"bLah", "bLah", true},
	{"bLah", "bLaH", false},
	{"a", "a", true},
	{"ab", "a?", true},
	{"abc", "ab?", true},
	{"a", "??", false},
	{"ab", "??", true},
	{"abc", "???", true},
	{"abcd", "????", true},
	{"abc", "????", false},
	{"abcd", "?b??", true},
	{"abcd", "?a??", false},
	{"abcd", "??c?", true},
	{"abcd", "??d?", false},
	{"abcde", "?b?d*?", true},
	{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab", true},
	{"abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", true},
	{"abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", "abababababababababababababababababababaacacacacacacacadaeafagahaiajaxalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", false},
	{"abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaggggagaaaaaaaab", false},
	{"abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", true},
	{"aaabbaabbaab", "aaabbaabbaab", true},
	{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true},
	{"aaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaa", true},
	{"aaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaa", false},
	{"abcabcdabcdeabcdefabcdefgabcdefghabcdefghiabcdefghijabcdefghijkabcdefghijklabcdefghijklmabcdefghijklmn", "abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc", false},
	{"abcabcdabcdeabcdefabcdefgabcdefghabcdefghiabcdefghijabcdefghijkabcdefghijklabcdefghijklmabcdefghijklmn", "abcabcdabcdeabcdefabcdefgabcdefghabcdefghiabcdefghijabcdefghijkabcdefghijklabcdefghijklmabcdefghijklmn", true},
	{"abcabcdabcdabcabcd", "abcabc?abcabcabc", false},
	{"abcabcdabcdabcabcdabcdabcabcdabcabcabcd", "abcabc?abc?abcabc?abc?abc?bc?abc?bc?bcd", true},
	{"?abc?", "?abc?", true},

	{"", "abd", false},
	{"", "abcccd", false},
	{"", "mississipissippi", false},
	{"", "xxxxzzzzzzzzyfffff", false},
	{"", "xxxxzzzzzzzzyf", false},
	{"", "xxxxzzy.fffff", false},
	{"", "xxxxzzzzzzzzyf", false},
	{"", "xyxyxyzyxyz", false},
	{"", "mississippi", false},
	{"", "xyxyxyxyz", false},
	{"", "m ississippi", false},
	{"", "ababac*", false},
	{"", "ababac", false},
	{"", "aaazz", false},
	{"", "1212", false},
	{"", "a12b", false},
	{"", "a12b12", false},
	{"", "n", false},
	{"", "aabab", false},
	{"", "ar", false},
	{"", "aaar", false},
	{"", "XYXYXYZYXYz", false},
	{"", "missisSIPpi", false},
	{"", "mississipPI", false},
	{"", "xyxyxyxyz", false},
	{"", "miSsissippi", false},
	{"", "miSsisSippi", false},
	{"", "abAbac", false},
	{"", "abAbac", false},
	{"", "aAazz", false},
	{"", "A12b123", false},
	{"", "a12B12", false},
	{"", "oWn", false},
	{"", "bLah", false},
	{"", "bLaH", false},
	{"", "", true},
	{"abc", "", false},
	{"abcccd", "", false},
	{"mississipissippi", "", false},
	{"xxxxzzzzzzzzyf", "", false},
	{"xxxxzzzzzzzzyf", "", false},
	{"xxxxzzzzzzzzyf", "", false},
	{"xxxxzzzzzzzzyf", "", false},
	{"xyxyxyzyxyz", "", false},
	{"mississippi", "", false},
	{"xyxyxyxyz", "", false},
	{"m ississippi", "", false},
	{"ababac", "", false},
	{"dababac", "", false},
	{"aaazz", "", false},
	{"a12b12", "", false},
	{"a12b12", "", false},
	{"a12b12", "", false},
	{"n", "", false},
	{"aabab", "", false},
	{"ar", "", false},
	{"aar", "", false},
	{"XYXYXYZYXYz", "", false},
	{"missisSIPpi", "", false},
	{"mississipPI", "", false},
	{"xyxyxyxyz", "", false},
	{"miSsissippi", "", false},
	{"miSsissippi", "", false},
	{"abAbac", "", false},
	{"abAbac", "", false},
	{"aAazz", "", false},
	{"A12b12", "", false},
	{"a12B12", "", false},
	{"oWn", "", false},
	{"bLah", "", false},
	{"bLah", "", false},
}

func identity(a, b rune) bool {
	return a == b
}

func TestWildcardMatches(t *testing.T) {
	t.Run("UnicodeWildcardMatcher (no optimization)", func(t *testing.T) {
		for _, tc := range wildcardTestCases {
			wildcard := newUnicodeWildcardMatcher(charset.Charset_utf8mb4{}, identity, nil, []byte(tc.pat), '?', '*', '\\')
			match := wildcard.Match([]byte(tc.in))
			assert.Equal(t, tc.match, match, "wildcard(%q, %q) = %v (expected %v)", tc.in, tc.pat, match, tc.match)

		}
	})

	t.Run("EightbitWildcardMatcher (no optimization)", func(t *testing.T) {
		for _, tc := range wildcardTestCases {
			wildcard := newEightbitWildcardMatcher(&sortOrderIdentity, nil, []byte(tc.pat), '?', '*', '\\')
			match := wildcard.Match([]byte(tc.in))
			assert.Equal(t, tc.match, match, "wildcard(%q, %q) = %v (expected %v)", tc.in, tc.pat, match, tc.match)

		}
	})

	testWildcardMatches(t, "utf8mb4_0900_bin", '?', '*', '\\', wildcardTestCases)
	testWildcardMatches(t, "utf8mb4_0900_as_cs", '?', '*', '\\', wildcardTestCases)
}

func BenchmarkWildcardMatching(b *testing.B) {
	type bench struct {
		input []byte
		m1    WildcardPattern
		m2    WildcardPattern
	}

	var patterns []bench
	for _, tc := range wildcardTestCases {
		patterns = append(patterns, bench{
			input: []byte(tc.in),
			m1:    newUnicodeWildcardMatcher(charset.Charset_utf8mb4{}, identity, nil, []byte(tc.pat), '?', '*', '\\'),
			m2:    newEightbitWildcardMatcher(&sortOrderIdentity, nil, []byte(tc.pat), '?', '*', '\\'),
		})
	}

	b.Run("unicode", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			for _, bb := range patterns {
				_ = bb.m1.Match(bb.input)
			}
		}
	})

	b.Run("8bit", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			for _, bb := range patterns {
				_ = bb.m2.Match(bb.input)
			}
		}
	})
}
