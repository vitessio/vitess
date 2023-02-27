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
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/internal/charset"
	"vitess.io/vitess/go/mysql/collations/remote"
	"vitess.io/vitess/go/sqltypes"
)

type testweight struct {
	collation string
	input     []byte
}

type testcmp struct {
	collation   string
	left, right []byte
}

func testRemoteWeights(t *testing.T, golden io.Writer, cases []testweight) {
	conn := mysqlconn(t)
	defer conn.Close()

	for _, tc := range cases {
		t.Run(tc.collation, func(t *testing.T) {
			local := collations.Local().LookupByName(tc.collation)
			remote := remote.NewCollation(conn, tc.collation)
			localResult := local.WeightString(nil, tc.input, 0)
			remoteResult := remote.WeightString(nil, tc.input, 0)

			if err := remote.LastError(); err != nil {
				t.Fatalf("remote collation failed: %v", err)
			}
			assert.True(t, bytes.Equal(localResult, remoteResult), "expected WEIGHT_STRING(%#v) = %#v (got %#v)", tc.input, remoteResult, localResult)

			if golden != nil {
				fmt.Fprintf(golden, "{\n\tcollation: %q,\n\texpected: %#v,\n},\n", tc.collation, remoteResult)
			}
		})
	}
}

func testRemoteComparison(t *testing.T, golden io.Writer, cases []testcmp) {
	normalizecmp := func(res int) int {
		if res < 0 {
			return -1
		}
		if res > 0 {
			return 1
		}
		return 0
	}

	conn := mysqlconn(t)
	defer conn.Close()

	for _, tc := range cases {
		t.Run(tc.collation, func(t *testing.T) {
			local := collations.Local().LookupByName(tc.collation)
			remote := remote.NewCollation(conn, tc.collation)
			localResult := normalizecmp(local.Collate(tc.left, tc.right, false))
			remoteResult := remote.Collate(tc.left, tc.right, false)

			if err := remote.LastError(); err != nil {
				t.Fatalf("remote collation failed: %v", err)
			}
			assert.Equal(t, remoteResult, localResult, "expected STRCMP(%q, %q) = %d (got %d)", string(tc.left), string(tc.right), remoteResult, localResult)

			if golden != nil {
				fmt.Fprintf(golden, "{\n\tcollation: %q,\n\tleft: %#v,\n\tright: %#v,\n\texpected: %d,\n},\n",
					tc.collation, tc.left, tc.right, remoteResult)
			}
		})
	}
}

func verifyTranscoding(t *testing.T, local collations.Collation, remote *remote.Collation, text []byte) []byte {
	transRemote, err := charset.ConvertFromUTF8(nil, remote.Charset(), text)
	require.NoError(t, err, "remote transcoding failed: %v", err)

	transLocal, _ := charset.ConvertFromUTF8(nil, local.Charset(), text)
	require.True(t, bytes.Equal(transLocal, transRemote), "transcoding mismatch with %s (%d, charset: %s)\ninput:\n%s\nremote:\n%s\nlocal:\n%s\n", local.Name(), local.ID(), local.Charset().Name(),
		hex.Dump(text), hex.Dump(transRemote), hex.Dump(transLocal))

	return transLocal
}

func verifyWeightString(t *testing.T, local collations.Collation, remote *remote.Collation, text []byte) {
	localResult := local.WeightString(nil, text, 0)
	remoteResult := remote.WeightString(nil, text, 0)

	if err := remote.LastError(); err != nil {
		t.Fatalf("remote collation failed: %v", err)
	}

	if len(remoteResult) == 0 {
		t.Logf("remote collation %s returned empty string", remote.Name())
		return
	}

	if !bytes.Equal(localResult, remoteResult) {
		printDebugData(t, []string{
			"strnxfrm",
			"--collation", local.Name(),
			"--input", hex.EncodeToString(text),
		})
		t.Fatalf("WEIGHT_STRING mismatch with collation %s (charset %s)\ninput:\n%s\nremote:\n%s\nlocal:\n%s\ngolden:\n%#v\n",
			local.Name(), local.Charset().Name(), hex.Dump(text), hex.Dump(remoteResult), hex.Dump(localResult), text)
	}
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	res, err := conn.ExecuteFetch(query, -1, true)
	require.NoError(t, err, "failed to execute %q: %v", query, err)

	return res
}

func GoldenWeightString(t *testing.T, conn *mysql.Conn, collation string, input []byte) []byte {
	coll := remote.NewCollation(conn, collation)
	weightString := coll.WeightString(nil, input, 0)
	if weightString == nil {
		t.Fatal(coll.LastError())
	}
	return weightString
}

var printDebugData = func(t *testing.T, args []string) {
	t.Logf("debug: colldump %s", strings.Join(args, " "))
}
