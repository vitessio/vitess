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
	for _, id := range []ID{Unknown, CollationUtf8mb4ID, CollationBinaryID, 8 /* latin1_swedish_ci */, 18 /* tis620_thai_ci */} {
		require.True(t, env.IsConnectionCharset(id), "collation %d", id)
	}
	for _, id := range []ID{13 /* sjis_japanese_ci */, 95 /* cp932_japanese_ci */, 28 /* gbk_chinese_ci */, 1 /* big5_chinese_ci */, 248 /* gb18030_chinese_ci */, 250 /* gb18030_unicode_520_ci */, 35 /* ucs2_general_ci */, 54 /* utf16_general_ci */} {
		require.False(t, env.IsConnectionCharset(id), "collation %d", id)
	}

	// A MySQL 8.0 client's default collation is accepted by an environment for
	// a MySQL version that does not have it.
	require.True(t, NewEnvironment("5.7.31").IsConnectionCharset(CollationUtf8mb4ID))
}
