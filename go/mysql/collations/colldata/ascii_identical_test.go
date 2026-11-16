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

package colldata

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
)

// TestIsASCIIIdentical checks which character sets have ASCII text as the
// bytes of ASCII. MySQL 8.0.46 writes '\\' as 81 5F in sjis and as 5C in
// cp932, and 'Ä' as 5B in swe7.
func TestIsASCIIIdentical(t *testing.T) {
	tests := map[string]bool{
		"utf8mb4_0900_ai_ci": true,
		"utf8mb3_general_ci": true,
		"latin1_swedish_ci":  true,
		"gb2312_chinese_ci":  true,
		"cp932_japanese_ci":  true,
		"sjis_japanese_ci":   false,
		"swe7_swedish_ci":    false,
		"ucs2_general_ci":    false,
		"utf16_general_ci":   false,
		"utf16le_general_ci": false,
		"utf32_general_ci":   false,
	}
	for name, want := range tests {
		coll := Lookup(collations.MySQL8().LookupByName(name))
		require.NotNil(t, coll, name)
		assert.Equal(t, want, charset.IsASCIIIdentical(coll.Charset()), name)
	}
}
