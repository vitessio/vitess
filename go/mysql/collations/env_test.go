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

package collations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseConnectionCharset checks that only character sets whose multibyte
// characters never contain ASCII bytes can be used for a connection, whether
// they are named by character set or by collation, or asked for by collation ID
// in a client handshake.
func TestParseConnectionCharset(t *testing.T) {
	env := MySQL8()

	for _, name := range []string{"", "utf8mb4", "UTF8MB4", "utf8mb4_bin", "utf8", "utf8mb3", "latin1", "ascii", "binary", "cp1251", "ujis", "eucjpms", "euckr", "gb2312"} {
		_, err := env.ParseConnectionCharset(name)
		require.NoError(t, err, "connection charset %q", name)
	}

	for _, name := range []string{"sjis", "sjis_bin", "cp932", "cp932_japanese_ci", "gb18030_unicode_520_ci", "gbk", "big5", "ucs2", "utf16", "utf16le", "utf16_bin", "utf32"} {
		_, err := env.ParseConnectionCharset(name)
		require.ErrorContains(t, err, "unsupported connection charset", "connection charset %q", name)
	}

	// A client can ask for any collation in its handshake, including ones Vitess
	// does not implement, such as gbk_chinese_ci and tis620_thai_ci.
	for id, want := range map[ID]string{Unknown: "", CollationUtf8mb4ID: "utf8mb4", CollationBinaryID: "binary", 8 /* latin1_swedish_ci */ : "latin1", 18 /* tis620_thai_ci */ : "tis620"} {
		charset, ok := env.ConnectionCharset(id)
		require.True(t, ok, "collation %d", id)
		require.Equal(t, want, charset, "collation %d", id)
	}
	for id, want := range map[ID]string{13 /* sjis_japanese_ci */ : "sjis", 95 /* cp932_japanese_ci */ : "cp932", 28 /* gbk_chinese_ci */ : "gbk", 1 /* big5_chinese_ci */ : "big5", 248 /* gb18030_chinese_ci */ : "gb18030", 250 /* gb18030_unicode_520_ci */ : "gb18030", 35 /* ucs2_general_ci */ : "ucs2", 54 /* utf16_general_ci */ : "utf16"} {
		charset, ok := env.ConnectionCharset(id)
		require.False(t, ok, "collation %d", id)
		require.Equal(t, want, charset, "collation %d", id)
	}

	// An ID that MySQL does not define is refused, with no name to report.
	charset, ok := env.ConnectionCharset(4000)
	require.False(t, ok)
	require.Empty(t, charset)

	// A MySQL 8.0 client's default collation is accepted by an environment for
	// a MySQL version that does not have it.
	_, ok = NewEnvironment("5.7.31").ConnectionCharset(CollationUtf8mb4ID)
	require.True(t, ok)

	// A SET statement names a character set or collation, which need not be one
	// Vitess implements either.
	for _, name := range []string{"utf8mb4", "UTF8MB4", "utf8mb4_ja_0900_as_cs", "utf8", "utf8_general_ci", "binary", "latin1_swedish_ci", "tis620"} {
		require.True(t, IsConnectionCharsetName(name), "name %q", name)
	}
	for _, name := range []string{"sjis", "cp932_japanese_ci", "gbk", "gbk_chinese_ci", "big5", "gb18030", "ucs2", "utf16le", "no_such_charset"} {
		require.False(t, IsConnectionCharsetName(name), "name %q", name)
	}
}
