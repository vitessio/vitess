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

	"github.com/stretchr/testify/assert"
)

// TestParseConnectionCharset checks that only character sets whose multibyte
// characters never contain ASCII bytes can be used for a connection, whether
// they are named by character set or by collation.
func TestParseConnectionCharset(t *testing.T) {
	env := MySQL8()

	for _, name := range []string{"", "utf8mb4", "UTF8MB4", "utf8mb4_bin", "utf8", "utf8mb3", "latin1", "ascii", "binary", "cp1251", "ujis", "eucjpms", "euckr", "gb2312"} {
		_, err := env.ParseConnectionCharset(name)
		assert.NoError(t, err, "connection charset %q", name)
	}

	for _, name := range []string{"sjis", "sjis_bin", "cp932", "cp932_japanese_ci", "gb18030_unicode_520_ci", "gbk", "big5", "ucs2", "utf16", "utf16le", "utf16_bin", "utf32"} {
		_, err := env.ParseConnectionCharset(name)
		assert.ErrorContains(t, err, "unsupported connection charset", "connection charset %q", name)
	}
}
