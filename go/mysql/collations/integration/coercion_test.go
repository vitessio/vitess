/*
Copyright 2021 The Vitess Authors.

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

package integration

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations/colldata"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/remote"
	"vitess.io/vitess/go/sqltypes"
)

type TextWithCollation struct {
	Text      []byte
	Collation collations.ID
}

type RemoteCoercionResult struct {
	Expr         sqltypes.Value
	Collation    collations.ID
	Coercibility collations.Coercibility
}

type RemoteCoercionTest interface {
	Expression() string
	Test(t *testing.T, remote *RemoteCoercionResult, local collations.TypedCollation, coerce1, coerce2 colldata.Coercion)
}

type testConcat struct {
	left, right *TextWithCollation
}

func (tc *testConcat) Expression() string {
	env := collations.MySQL8()
	return fmt.Sprintf("CONCAT((_%s X'%x' COLLATE %q), (_%s X'%x' COLLATE %q))",
		colldata.Lookup(tc.left.Collation).Charset().Name(), tc.left.Text, env.LookupName(tc.left.Collation),
		colldata.Lookup(tc.right.Collation).Charset().Name(), tc.right.Text, env.LookupName(tc.right.Collation),
	)
}

func (tc *testConcat) Test(t *testing.T, remote *RemoteCoercionResult, local collations.TypedCollation, coercion1, coercion2 colldata.Coercion) {
	localCollation := colldata.Lookup(local.Collation)
	remoteName := collations.MySQL8().LookupName(remote.Collation)
	assert.Equal(t, remoteName, localCollation.Name(), "bad collation resolved: local is %s, remote is %s", localCollation.Name(), remoteName)
	assert.Equal(t, remote.Coercibility, local.Coercibility, "bad coercibility resolved: local is %d, remote is %d", local.Coercibility, remote.Coercibility)

	leftText, err := coercion1(nil, tc.left.Text)
	if err != nil {
		t.Errorf("failed to transcode left: %v", err)
		return
	}

	rightText, err := coercion2(nil, tc.right.Text)
	if err != nil {
		t.Errorf("failed to transcode right: %v", err)
		return
	}

	var concat bytes.Buffer
	concat.Write(leftText)
	concat.Write(rightText)

	rEBytes, err := remote.Expr.ToBytes()
	require.NoError(t, err)
	assert.True(t, bytes.Equal(concat.Bytes(), rEBytes), "failed to concatenate text;\n\tCONCAT(%v COLLATE %s, %v COLLATE %s) = \n\tCONCAT(%v, %v) COLLATE %s = \n\t\t%v\n\n\texpected: %v", tc.left.Text, collations.MySQL8().LookupName(tc.left.Collation),
		tc.right.Text, collations.MySQL8().LookupName(tc.right.Collation), leftText, rightText, localCollation.Name(),
		concat.Bytes(), rEBytes)

}

type testComparison struct {
	left, right *TextWithCollation
}

func (tc *testComparison) Expression() string {
	env := collations.MySQL8()
	return fmt.Sprintf("(_%s X'%x' COLLATE %q) = (_%s X'%x' COLLATE %q)",
		env.LookupCharsetName(tc.left.Collation), tc.left.Text, env.LookupName(tc.left.Collation),
		env.LookupCharsetName(tc.right.Collation), tc.right.Text, env.LookupName(tc.right.Collation),
	)
}

func (tc *testComparison) Test(t *testing.T, remote *RemoteCoercionResult, local collations.TypedCollation, coerce1, coerce2 colldata.Coercion) {
	localCollation := colldata.Lookup(local.Collation)
	leftText, err := coerce1(nil, tc.left.Text)
	if err != nil {
		t.Errorf("failed to transcode left: %v", err)
		return
	}

	rightText, err := coerce2(nil, tc.right.Text)
	if err != nil {
		t.Errorf("failed to transcode right: %v", err)
		return
	}
	rEBytes, err := remote.Expr.ToBytes()
	require.NoError(t, err)
	remoteEquals := rEBytes[0] == '1'
	localEquals := localCollation.Collate(leftText, rightText, false) == 0
	assert.Equal(t, localEquals, remoteEquals, "failed to collate %#v = %#v with collation %s (expected %v, got %v)", leftText, rightText, localCollation.Name(), remoteEquals, localEquals)

}

func TestComparisonSemantics(t *testing.T) {
	const BaseString = "abcdABCD01234"
	var testInputs []*TextWithCollation

	conn := mysqlconn(t)
	defer conn.Close()

	if v, err := conn.ServerVersionAtLeast(8, 0, 31); err != nil || !v {
		t.Skipf("The behavior of Coercion Semantics is not correct before 8.0.31")
	}

	for _, coll := range colldata.All(collations.MySQL8()) {
		text := verifyTranscoding(t, coll, remote.NewCollation(conn, coll.Name()), []byte(BaseString))
		testInputs = append(testInputs, &TextWithCollation{Text: text, Collation: coll.ID()})
	}
	sort.Slice(testInputs, func(i, j int) bool {
		return testInputs[i].Collation < testInputs[j].Collation
	})

	var testCases = []struct {
		name string
		make func(left, right *TextWithCollation) RemoteCoercionTest
	}{
		{
			name: "equals",
			make: func(left, right *TextWithCollation) RemoteCoercionTest {
				return &testComparison{left, right}
			},
		},
		{
			name: "concat",
			make: func(left, right *TextWithCollation) RemoteCoercionTest {
				return &testConcat{left, right}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, collA := range testInputs {
				for _, collB := range testInputs {
					left := collations.TypedCollation{
						Collation:    collA.Collation,
						Coercibility: 0,
						Repertoire:   collations.RepertoireASCII,
					}
					right := collations.TypedCollation{
						Collation:    collB.Collation,
						Coercibility: 0,
						Repertoire:   collations.RepertoireASCII,
					}
					resultLocal, coercionLocal1, coercionLocal2, errLocal := colldata.Merge(collations.MySQL8(), left, right,
						colldata.CoercionOptions{
							ConvertToSuperset:   true,
							ConvertWithCoercion: true,
						})

					// for strings that do not coerce, replace with a no-op coercion function
					if coercionLocal1 == nil {
						coercionLocal1 = func(_, in []byte) ([]byte, error) { return in, nil }
					}
					if coercionLocal2 == nil {
						coercionLocal2 = func(_, in []byte) ([]byte, error) { return in, nil }
					}

					remoteTest := tc.make(collA, collB)
					expr := remoteTest.Expression()
					query := fmt.Sprintf("SELECT CAST((%s) AS BINARY), COLLATION(%s), COERCIBILITY(%s)", expr, expr, expr)

					resultRemote, errRemote := conn.ExecuteFetch(query, 1, false)
					env := collations.MySQL8()
					if errRemote != nil {
						require.True(t, strings.Contains(errRemote.Error(), "Illegal mix of collations"), "query %s failed: %v", query, errRemote)

						if errLocal == nil {
							t.Errorf("expected %s vs %s to fail coercion: %v", env.LookupName(collA.Collation), env.LookupName(collB.Collation), errRemote)
							continue
						}
						require.True(t, strings.HasPrefix(normalizeCollationInError(errRemote.Error()), normalizeCollationInError(errLocal.Error())), "bad error message: expected %q, got %q", errRemote, errLocal)

						continue
					}

					if errLocal != nil {
						t.Errorf("expected %s vs %s to coerce, but they failed: %v", env.LookupName(collA.Collation), env.LookupName(collB.Collation), errLocal)
						continue
					}

					remoteCollation := collations.MySQL8().LookupByName(resultRemote.Rows[0][1].ToString())
					remoteCI, _ := resultRemote.Rows[0][2].ToInt64()
					remoteTest.Test(t, &RemoteCoercionResult{
						Expr:         resultRemote.Rows[0][0],
						Collation:    remoteCollation,
						Coercibility: collations.Coercibility(remoteCI),
					}, resultLocal, coercionLocal1, coercionLocal2)
				}
			}
		})
	}
}

// normalizeCollationInError normalizes the collation name in the error output.
// Starting with mysql 8.0.30 collations prefixed with `utf8_` have been changed to use `utf8mb3_` instead
// This is inconsistent with older MySQL versions and causes the tests to fail against it.
// As a stop-gap solution, this functions normalizes the error messages so that the tests pass until we
// have a fix for it.
// TODO: Remove error normalization
func normalizeCollationInError(errMessage string) string {
	return strings.ReplaceAll(errMessage, "utf8_", "utf8mb3_")
}
