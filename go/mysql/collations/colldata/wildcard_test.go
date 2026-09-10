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

package colldata

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestWildcardManyMetacharAsTrailByte(t *testing.T) {
	// The custom match-many metacharacter 0x5F is a valid sjis trail
	// byte, so the pattern 81 5F is one literal character and not a
	// prefix pattern. The prefix shortcut in the byte pre-scan must
	// leave it to the full parser.
	coll := testcollation(t, "sjis_japanese_ci")
	pat := coll.Wildcard([]byte("\x81\x5f"), '#', '_', 0)
	require.True(t, pat.Match([]byte("\x81\x5f")))
	require.False(t, pat.Match([]byte("\x81\x40")))
	require.False(t, pat.Match([]byte("\x81\x5f\x40")))

	// A negative match-many rune is not a character and can never mark a
	// wildcard; byte conversion would wrap -95 to the valid single-byte
	// character 0xA1, so the pattern is a two-character literal.
	pat = coll.Wildcard([]byte("\x61\xa1"), 0, -95, 0)
	require.True(t, pat.Match([]byte("\x61\xa1")))
	require.False(t, pat.Match([]byte("abc")))
	require.False(t, pat.Match([]byte("\x61")))
}

func TestWildcardTrailingManyFastPath(t *testing.T) {
	// A pattern with a single trailing match-many builds a prefix
	// fastMatcher. The strip must remove the full encoded width of the
	// metacharacter, which is two bytes for utf16.
	coll := testcollation(t, "utf8mb4_general_ci")
	m := coll.Wildcard([]byte("abc%"), 0, 0, 0)
	fm, ok := m.(*fastMatcher)
	require.True(t, ok)
	require.True(t, fm.isPrefix)
	require.Equal(t, []byte("abc"), fm.pattern)
	require.True(t, m.Match([]byte("abcdef")))
	require.False(t, m.Match([]byte("abd")))

	coll = testcollation(t, "utf16_bin")
	m = coll.Wildcard([]byte("\x00a\x00b\x00%"), 0, 0, 0)
	fm, ok = m.(*fastMatcher)
	require.True(t, ok)
	require.True(t, fm.isPrefix)
	require.Equal(t, []byte("\x00a\x00b"), fm.pattern)
	require.True(t, m.Match([]byte("\x00a\x00b\x00c")))
	require.False(t, m.Match([]byte("\x00a\x00c")))
}

func TestWildcardMatcherCachedSize(t *testing.T) {
	// A wildcard matcher is retained by cached plans. Its reported size
	// must cover only matcher-owned memory and never the shared
	// collation or charset data; sizegen walks non-empty interface
	// fields, so such a field can pull the collation singletons in.
	collations := []string{
		"utf8mb4_0900_ai_ci", "utf8mb4_0900_bin", "utf8mb4_general_ci",
		"utf8mb4_bin", "utf8mb4_swedish_ci", "latin1_swedish_ci",
		"sjis_japanese_ci", "utf16_unicode_ci",
	}
	for _, collName := range collations {
		coll := testcollation(t, collName)
		for _, pat := range []string{"hello", "he%o", "%hello%", "abc%"} {
			m := coll.Wildcard([]byte(pat), 0, 0, 0)
			sized, ok := m.(interface{ CachedSize(alloc bool) int64 })
			if !ok {
				continue
			}
			size := sized.CachedSize(true)
			require.LessOrEqualf(t, size, int64(1024), "%s wildcard %q reports %d bytes", collName, pat, size)
		}
	}
}

func TestWildcardPatternSpareCapacity(t *testing.T) {
	// The parsers reserve one token per pattern byte to keep the parse
	// a single pass. Multibyte characters then leave spare capacity,
	// and the matcher must not retain a large spare in the plan cache.
	t.Run("multibyte", func(t *testing.T) {
		coll := testcollation(t, "eucjpms_japanese_ci")
		chunk := bytes.Repeat([]byte("\x8f\xa2\xaf"), 1024)
		pat := append(append(append([]byte{}, chunk...), '%'), chunk...)
		m := coll.Wildcard(pat, 0, 0, 0)
		mb, ok := m.(*multibyteWildcard)
		require.True(t, ok)
		require.Less(t, cap(mb.pattern)-len(mb.pattern), 128)
	})
	t.Run("unicode", func(t *testing.T) {
		coll := testcollation(t, "utf8mb4_general_ci")
		chunk := bytes.Repeat([]byte("あ"), 1024)
		pat := append(append(append([]byte{}, chunk...), '%'), chunk...)
		m := coll.Wildcard(pat, 0, 0, 0)
		uw, ok := m.(*unicodeWildcard)
		require.True(t, ok)
		require.Less(t, cap(uw.pattern)-len(uw.pattern), 256)
	})
}

func TestWildcardEightbitFastPath(t *testing.T) {
	// The literal and the prefix fast paths return before the token
	// slice exists, so a construction allocates only the matcher and
	// the collate callback.
	coll := testcollation(t, "latin1_swedish_ci")
	for _, tc := range []struct {
		pat    string
		prefix bool
	}{
		{"hello", false},
		{"hello%", true},
	} {
		m := coll.Wildcard([]byte(tc.pat), 0, 0, 0)
		fm, ok := m.(*fastMatcher)
		require.True(t, ok)
		require.Equal(t, tc.prefix, fm.isPrefix)
		require.True(t, m.Match([]byte("hello")))
		require.False(t, m.Match([]byte("hellx")))
		pat := []byte(tc.pat)
		allocs := testing.AllocsPerRun(100, func() {
			_ = coll.Wildcard(pat, 0, 0, 0)
		})
		require.LessOrEqualf(t, allocs, 2.0, "pattern %q allocates %v times", tc.pat, allocs)
	}
}

func TestWildcardCollapsedRunAllocation(t *testing.T) {
	// A pattern that is mostly match-many characters collapses to a
	// handful of tokens, so the construction must not reserve one token
	// per pattern byte: a large bound LIKE pattern would then allocate a
	// multiple of its own size.
	pat := bytes.Repeat([]byte{'%'}, 1024*1024)
	for _, collName := range []string{"sjis_japanese_ci", "utf8mb4_0900_ai_ci", "latin1_swedish_ci"} {
		coll := testcollation(t, collName)
		var ms runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&ms)
		before := ms.TotalAlloc
		m := coll.Wildcard(pat, 0, 0, 0)
		runtime.ReadMemStats(&ms)
		require.NotNil(t, m)
		require.Lessf(t, ms.TotalAlloc-before, uint64(256*1024), "%s: construction allocated %d bytes", collName, ms.TotalAlloc-before)
	}

	// The reservation reduction must not fire when the match-many byte is
	// not its own marker, or the parser grows the token slice step by
	// step. A fixed-width charset can hold the byte inside an unrelated
	// character: the utf16 bytes 25 25 are the literal U+2525. A rune
	// shared with match-one classifies every occurrence as match-one.
	utf16Coll := testcollation(t, "utf16_unicode_ci")
	utf16Pat := bytes.Repeat([]byte{0x25, 0x25}, 512*1024)
	allocs := testing.AllocsPerRun(3, func() {
		_ = utf16Coll.Wildcard(utf16Pat, 0, 0, 0)
	})
	require.LessOrEqualf(t, allocs, 8.0, "utf16 literal pattern construction allocates %v times", allocs)

	aliasPat := bytes.Repeat([]byte{'_'}, 1024*1024)
	for _, collName := range []string{"sjis_japanese_ci", "utf8mb4_0900_ai_ci", "latin1_swedish_ci"} {
		coll := testcollation(t, collName)
		allocs := testing.AllocsPerRun(3, func() {
			_ = coll.Wildcard(aliasPat, '_', '_', 0)
		})
		require.LessOrEqualf(t, allocs, 8.0, "%s: aliased metacharacter construction allocates %v times", collName, allocs)
	}

	// A fixed-width charset decodes at most one character per MinWidth
	// bytes, so the token reservation must divide by the character width:
	// one token per pattern byte holds twice the needed capacity for
	// utf16 and four times for utf32, and the spare-capacity trim then
	// copies the tokens a second time.
	for _, tc := range []struct {
		collName string
		width    int
	}{
		{"utf16_unicode_ci", 2},
		{"utf32_unicode_ci", 4},
	} {
		coll := testcollation(t, tc.collName)
		encodeASCII := func(ch byte) []byte {
			c := make([]byte, tc.width)
			c[tc.width-1] = ch
			return c
		}
		widePat := bytes.Repeat(encodeASCII('a'), (512*1024)/tc.width)
		widePat = append(widePat, encodeASCII('%')...)
		widePat = append(widePat, bytes.Repeat(encodeASCII('b'), (512*1024)/tc.width)...)
		var ms runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&ms)
		before := ms.TotalAlloc
		m := coll.Wildcard(widePat, 0, 0, 0)
		runtime.ReadMemStats(&ms)
		require.NotNil(t, m)
		limit := uint64(4*len(widePat)/tc.width + 256*1024)
		require.Lessf(t, ms.TotalAlloc-before, limit, "%s: construction allocated %d bytes", tc.collName, ms.TotalAlloc-before)
	}

	// A negative match-many rune never marks a wildcard, but its byte
	// conversion wraps to 'a', so every pattern byte would count as a
	// collapsible wildcard while the parser appends every rune literally.
	negPat := bytes.Repeat([]byte{'a'}, 1024*1024)
	for _, collName := range []string{"sjis_japanese_ci", "utf8mb4_0900_ai_ci"} {
		coll := testcollation(t, collName)
		allocs := testing.AllocsPerRun(3, func() {
			_ = coll.Wildcard(negPat, 0, -159, 0)
		})
		require.LessOrEqualf(t, allocs, 8.0, "%s: negative metacharacter construction allocates %v times", collName, allocs)
	}
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
