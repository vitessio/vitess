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

package vtgate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func TestResultCharset(t *testing.T) {
	env := collations.MySQL8()
	for _, cs := range []string{"", "utf8mb3", "binary", "utf16", "abcd"} {
		assert.Nil(t, resultCharset(env, &vtgatepb.Session{CharacterSetResults: cs}), cs)
	}
	for _, cs := range []string{"latin1", "ascii", "cp1251"} {
		assert.NotNil(t, resultCharset(env, &vtgatepb.Session{CharacterSetResults: cs}), cs)
	}
}

// TestEncodeResultNames checks that result metadata is converted to
// character_set_results, as MySQL sends it, without changing the fields that
// vtgate shares, for example with cached plans.
func TestEncodeResultNames(t *testing.T) {
	env := collations.MySQL8()
	latin1 := resultCharset(env, &vtgatepb.Session{CharacterSetResults: "latin1"})
	ascii := resultCharset(env, &vtgatepb.Session{CharacterSetResults: "ascii"})

	fields := []*querypb.Field{
		{Name: "id", Table: "t", Database: "ks"},
		{Name: "é", OrgName: "ünï", Table: "wé", OrgTable: "wé", Database: "ks"},
		{Name: "😀"},
	}
	qr := &sqltypes.Result{Fields: fields, Rows: [][]sqltypes.Value{{sqltypes.NewInt64(1), sqltypes.NewInt64(2), sqltypes.NewInt64(3)}}}

	encoded := encodeResult(latin1, qr)
	require.NotSame(t, qr, encoded)
	assert.Equal(t, qr.Rows, encoded.Rows)
	assert.Same(t, fields[0], encoded.Fields[0])
	assert.Equal(t, "\xe9", encoded.Fields[1].Name)
	assert.Equal(t, "\xfcn\xef", encoded.Fields[1].OrgName)
	assert.Equal(t, "w\xe9", encoded.Fields[1].Table)
	assert.Equal(t, "w\xe9", encoded.Fields[1].OrgTable)
	assert.Equal(t, "ks", encoded.Fields[1].Database)
	assert.Equal(t, "?", encoded.Fields[2].Name)

	assert.Equal(t, "?", encodeResult(ascii, qr).Fields[1].Name)

	// The fields that vtgate passed in are unchanged.
	assert.Equal(t, "é", fields[1].Name)
	assert.Equal(t, "😀", fields[2].Name)

	// Nothing is copied when no name changes.
	ids := &sqltypes.Result{Fields: fields[:1]}
	assert.Same(t, ids, encodeResult(latin1, ids))
	assert.Same(t, qr, encodeResult(nil, qr))
}
