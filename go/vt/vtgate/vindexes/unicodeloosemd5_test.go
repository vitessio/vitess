/*
Copyright 2019 The Vitess Authors.

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

var charVindexMD5 SingleColumn

func init() {
	vindex, err := CreateVindex("unicode_loose_md5", "utf8ch", nil)
	if err != nil {
		panic(err)
	}
	charVindexMD5 = vindex.(SingleColumn)
}

func unicodeLooseMD5CreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "unicode_loose_md5",
		vindexName:   "unicode_loose_md5",
		vindexParams: vindexParams,

		expectCost:          1,
		expectErr:           expectErr,
		expectIsUnique:      true,
		expectNeedsVCursor:  false,
		expectString:        "unicode_loose_md5",
		expectUnknownParams: expectUnknownParams,
	}
}

func TestUnicodeLooseMD5CreateVindex(t *testing.T) {
	cases := []createVindexTestCase{
		unicodeLooseMD5CreateVindexTestCase(
			"no params",
			nil,
			nil,
			nil,
		),
		unicodeLooseMD5CreateVindexTestCase(
			"empty params",
			map[string]string{},
			nil,
			nil,
		),
		unicodeLooseMD5CreateVindexTestCase(
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

func TestUnicodeLooseMD5Map(t *testing.T) {
	tcases := []struct {
		in  sqltypes.Value
		out string
	}{{
		in:  sqltypes.NewVarBinary("Test"),
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  sqltypes.NewVarBinary("TEST"),
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  sqltypes.NewVarBinary("Te\u0301st"),
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  sqltypes.NewVarBinary("Tést"),
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  sqltypes.NewVarBinary("Bést"),
		out: "²3.Os\xd0\aA\x02bIpo/\xb6",
	}, {
		in:  sqltypes.NewVarBinary("Test "),
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  sqltypes.NewVarBinary(" Test"),
		out: "\xa2\xe3Q\\~\x8d\xf1\xff\xd2\xcc\xfc\x11Ʊ\x9d\xd1",
	}, {
		in:  sqltypes.NewVarBinary("Test\t"),
		out: "\x82Em\xd8z\x9cz\x02\xb1\xc2\x05kZ\xba\xa2r",
	}, {
		in:  sqltypes.NewVarBinary("TéstLooong"),
		out: "\x96\x83\xe1+\x80C\f\xd4S\xf5\xdfߺ\x81ɥ",
	}, {
		in:  sqltypes.NewVarBinary("T"),
		out: "\xac\x0f\x91y\xf5\x1d\xb8\u007f\xe8\xec\xc0\xcf@ʹz",
	}, {
		in:  sqltypes.NULL,
		out: "\xd4\x1d\x8cُ\x00\xb2\x04\xe9\x80\t\x98\xec\xf8B~",
	}}
	for _, tcase := range tcases {
		got, err := charVindexMD5.Map(context.Background(), nil, []sqltypes.Value{tcase.in})
		if err != nil {
			t.Error(err)
		}
		out := string(got[0].(key.DestinationKeyspaceID))
		if out != tcase.out {
			t.Errorf("Map(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
	}
}

func TestUnicodeLooseMD5Verify(t *testing.T) {
	ids := []sqltypes.Value{sqltypes.NewVarBinary("Test"), sqltypes.NewVarBinary("TEst"), sqltypes.NewVarBinary("different")}
	ksids := [][]byte{[]byte("\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5"), []byte("\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5"), []byte("\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5")}
	got, err := charVindexMD5.Verify(context.Background(), nil, ids, ksids)
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{true, true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("UnicodeLooseMD5.Verify: %v, want %v", got, want)
	}
}
