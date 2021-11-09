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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/remote"
	"vitess.io/vitess/go/sqltypes"
)

type TextWithCollation struct {
	Text      []byte
	Collation collations.Collation
}

type RemoteCoercionResult struct {
	Expr         sqltypes.Value
	Collation    collations.Collation
	Coercibility collations.Coercibility
}

type RemoteCoercionTest interface {
	Expression() string
	Test(t *testing.T, remote *RemoteCoercionResult, localCollation *collations.TypedCollation, localCoercion collations.Coercion)
}

type testConcat struct {
	left, right *TextWithCollation
}

func (tc *testConcat) Expression() string {
	return fmt.Sprintf("CONCAT((_%s X'%x' COLLATE %q), (_%s X'%x' COLLATE %q))",
		tc.left.Collation.Charset().Name(), tc.left.Text, tc.left.Collation.Name(),
		tc.right.Collation.Charset().Name(), tc.right.Text, tc.right.Collation.Name(),
	)
}

func (tc *testConcat) Test(t *testing.T, remote *RemoteCoercionResult, local *collations.TypedCollation, coercion collations.Coercion) {
	if local.Collation.Name() != remote.Collation.Name() {
		t.Errorf("bad collation resolved: local is %s, remote is %s", local.Collation.Name(), remote.Collation.Name())
	}
	if local.Coercibility != remote.Coercibility {
		t.Errorf("bad coercibility resolved: local is %d, remote is %d", local.Coercibility, remote.Coercibility)
	}

	leftText, rightText, err := coercion(nil, tc.left.Text, tc.right.Text)
	if err != nil {
		t.Errorf("failed to transcode left/right: %v", err)
		return
	}

	var concat bytes.Buffer
	concat.Write(leftText)
	concat.Write(rightText)

	if !bytes.Equal(concat.Bytes(), remote.Expr.ToBytes()) {
		t.Errorf("failed to concatenate text;\n\tCONCAT(%v COLLATE %s, %v COLLATE %s) = \n\tCONCAT(%v, %v) COLLATE %s = \n\t\t%v\n\n\texpected: %v",
			tc.left.Text, tc.left.Collation.Name(),
			tc.right.Text, tc.right.Collation.Name(),
			leftText, rightText, local.Collation.Name(),
			concat.Bytes(), remote.Expr.ToBytes(),
		)
	}
}

type testComparison struct {
	left, right *TextWithCollation
}

func (tc *testComparison) Expression() string {
	return fmt.Sprintf("(_%s X'%x' COLLATE %q) = (_%s X'%x' COLLATE %q)",
		tc.left.Collation.Charset().Name(), tc.left.Text, tc.left.Collation.Name(),
		tc.right.Collation.Charset().Name(), tc.right.Text, tc.right.Collation.Name(),
	)
}

func (tc *testComparison) Test(t *testing.T, remote *RemoteCoercionResult, localCollation *collations.TypedCollation, localCoercion collations.Coercion) {
	leftText, rightText, err := localCoercion(nil, tc.left.Text, tc.right.Text)
	if err != nil {
		t.Errorf("failed to transcode left/right: %v", err)
		return
	}

	remoteEquals := remote.Expr.ToBytes()[0] == '1'
	localEquals := localCollation.Collation.Collate(leftText, rightText, false) == 0
	if remoteEquals != localEquals {
		t.Errorf("failed to collate %#v = %#v with collation %s (expected %v, got %v)",
			leftText, rightText, localCollation.Collation.Name(), remoteEquals, localEquals)
	}
}

func TestComparisonSemantics(t *testing.T) {
	const BaseString = "abcdABCD01234"
	var testInputs []*TextWithCollation

	conn := mysqlconn(t)
	defer conn.Close()

	for _, coll := range defaultenv.AllCollations() {
		text := verifyTranscoding(t, coll, remote.NewCollation(conn, coll.Name()), []byte(BaseString))
		testInputs = append(testInputs, &TextWithCollation{Text: text, Collation: coll})
	}
	sort.Slice(testInputs, func(i, j int) bool {
		return testInputs[i].Collation.ID() < testInputs[j].Collation.ID()
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
					left := &collations.TypedCollation{
						Collation:    collA.Collation,
						Coercibility: 0,
						Repertoire:   collations.RepertoireASCII,
					}
					right := &collations.TypedCollation{
						Collation:    collB.Collation,
						Coercibility: 0,
						Repertoire:   collations.RepertoireASCII,
					}
					resultLocal, coercionLocal, errLocal := defaultenv.MergeCollations(left, right,
						collations.CoercionOptions{
							ConvertToSuperset:   true,
							ConvertWithCoercion: true,
						})

					remoteTest := tc.make(collA, collB)
					expr := remoteTest.Expression()
					query := fmt.Sprintf("SELECT CAST((%s) AS BINARY), COLLATION(%s), COERCIBILITY(%s)", expr, expr, expr)

					resultRemote, errRemote := conn.ExecuteFetch(query, 1, false)
					if errRemote != nil {
						if !strings.Contains(errRemote.Error(), "Illegal mix of collations") {
							t.Fatalf("query %s failed: %v", query, errRemote)
						}
						if errLocal == nil {
							t.Errorf("expected %s vs %s to fail coercion: %v", collA.Collation.Name(), collB.Collation.Name(), errRemote)
							continue
						}
						if !strings.HasPrefix(errRemote.Error(), errLocal.Error()) {
							t.Fatalf("bad error message: expected %q, got %q", errRemote, errLocal)
						}
						continue
					}

					if errLocal != nil {
						t.Errorf("expected %s vs %s to coerce, but they failed: %v", collA.Collation.Name(), collB.Collation.Name(), errLocal)
						continue
					}

					remoteCollation := defaultenv.LookupByName(resultRemote.Rows[0][1].ToString())
					remoteCI, _ := resultRemote.Rows[0][2].ToInt64()
					remoteTest.Test(t, &RemoteCoercionResult{
						Expr:         resultRemote.Rows[0][0],
						Collation:    remoteCollation,
						Coercibility: collations.Coercibility(remoteCI),
					}, resultLocal, coercionLocal)
				}
			}
		})
	}
}
