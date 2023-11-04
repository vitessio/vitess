/*
Copyright 2020 The Vitess Authors.

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

package vindexes

import (
	"context"
	"reflect"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var charVindexXXHash SingleColumn

func init() {
	vindex, err := CreateVindex("unicode_loose_xxhash", "utf8ch", nil)
	if err != nil {
		panic(err)
	}
	unknownParams := vindex.(ParamValidating).UnknownParams()
	if len(unknownParams) > 0 {
		panic("unicode_loose_xxhash test init: expected 0 unknown params")
	}
	charVindexXXHash = vindex.(SingleColumn)
}

func unicodeLooseXXHashCreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "unicode_loose_xxhash",
		vindexName:   "unicode_loose_xxhash",
		vindexParams: vindexParams,

		expectCost:          1,
		expectErr:           expectErr,
		expectIsUnique:      true,
		expectNeedsVCursor:  false,
		expectString:        "unicode_loose_xxhash",
		expectUnknownParams: expectUnknownParams,
	}
}

func TestUnicodeLooseXXHashCreateVindex(t *testing.T) {
	cases := []createVindexTestCase{
		unicodeLooseXXHashCreateVindexTestCase(
			"no params",
			nil,
			nil,
			nil,
		),
		unicodeLooseXXHashCreateVindexTestCase(
			"empty params",
			map[string]string{},
			nil,
			nil,
		),
		unicodeLooseXXHashCreateVindexTestCase(
			"unknown params",
			map[string]string{
				"hello": "world",
			},
			nil,
			[]string{"hello"},
		),
	}

	testCreateVindexes(t, cases)
}

func TestUnicodeLooseXXHashMap(t *testing.T) {
	tcases := []struct {
		in  sqltypes.Value
		out string
	}{{
		in:  sqltypes.NewVarBinary("Test"),
		out: "B\xd2\x13a\bzL\a",
	}, {
		in:  sqltypes.NewVarBinary("TEst"),
		out: "B\xd2\x13a\bzL\a",
	}, {
		in:  sqltypes.NewVarBinary("Te\u0301st"),
		out: "B\xd2\x13a\bzL\a",
	}, {
		in:  sqltypes.NewVarBinary("Tést"),
		out: "B\xd2\x13a\bzL\a",
	}, {
		in:  sqltypes.NewVarBinary("Bést"),
		out: "\x92iu\xb9\xce.\xc3\x16",
	}, {
		in:  sqltypes.NewVarBinary("Test "),
		out: "B\xd2\x13a\bzL\a",
	}, {
		in:  sqltypes.NewVarBinary(" Test"),
		out: "Oˋ\xe3N\xc0Wu",
	}, {
		in:  sqltypes.NewVarBinary("Test\t"),
		out: " \xaf\x87\xfc6\xe3\xfdQ",
	}, {
		in:  sqltypes.NewVarBinary("TéstLooong"),
		out: "\xd3\xea\x879B\xb4\x84\xa7",
	}, {
		in:  sqltypes.NewVarBinary("T"),
		out: "\xf8\x1c;\xe2\xd5\x01\xfe\x18",
	}, {
		in:  sqltypes.NULL,
		out: "\x99\xe9\xd8Q7\xdbF\xef",
	}}
	for _, tcase := range tcases {
		got, err := charVindexXXHash.Map(context.Background(), nil, []sqltypes.Value{tcase.in})
		if err != nil {
			t.Error(err)
		}
		out := string(got[0].(key.DestinationKeyspaceID))
		if out != tcase.out {
			t.Errorf("Map(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
	}
}

func TestUnicodeLooseXXHashVerify(t *testing.T) {
	ids := []sqltypes.Value{sqltypes.NewVarBinary("Test"), sqltypes.NewVarBinary("TEst"), sqltypes.NewVarBinary("different")}
	ksids := [][]byte{[]byte("B\xd2\x13a\bzL\a"), []byte("B\xd2\x13a\bzL\a"), []byte(" \xaf\x87\xfc6\xe3\xfdQ")}
	got, err := charVindexXXHash.Verify(context.Background(), nil, ids, ksids)
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{true, true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("UnicodeLooseXXHash.Verify: %v, want %v", got, want)
	}
}
