/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vindexes

import (
	"testing"

	"strings"

	"github.com/youtube/vitess/go/sqltypes"
)

var binVindex Vindex

func init() {
	binVindex, _ = CreateVindex("binary_md5", "binary_md5_varchar", nil)
}

func TestBinaryMD5Cost(t *testing.T) {
	if binVindex.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", binVindex.Cost())
	}
}

func TestBinaryMD5String(t *testing.T) {
	if strings.Compare("binary_md5_varchar", binVindex.String()) != 0 {
		t.Errorf("String(): %s, want binary_md5_varchar", binVindex.String())
	}
}

func TestBinaryMD5(t *testing.T) {
	tcases := []struct {
		in, out string
	}{{
		in:  "Test",
		out: "\f\xbcf\x11\xf5T\vЀ\x9a8\x8d\xc9Za[",
	}, {
		in:  "TEST",
		out: "\x03;\xd9K\x11h\xd7\xe4\xf0\xd6D\xc3\xc9^5\xbf",
	}, {
		in:  "Test",
		out: "\f\xbcf\x11\xf5T\vЀ\x9a8\x8d\xc9Za[",
	}}
	for _, tcase := range tcases {
		got, err := binVindex.(Unique).Map(nil, []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(tcase.in))})
		if err != nil {
			t.Error(err)
		}
		out := string(got[0])
		if out != tcase.out {
			t.Errorf("Map(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
		ok, err := binVindex.Verify(nil, []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(tcase.in))}, [][]byte{[]byte(tcase.out)})
		if err != nil {
			t.Error(err)
		}
		if !ok {
			t.Errorf("Verify(%#v): false, want true", tcase.in)
		}
	}
}

func TestBinaryMD5VerifyNeg(t *testing.T) {
	_, err := binVindex.Verify(nil, []sqltypes.Value{sqltypes.NewVarChar("test1"), sqltypes.NewVarChar("test2")}, [][]byte{[]byte("test1")})
	want := "BinaryMD5_hash.Verify: length of ids 2 doesn't match length of ksids 1"
	if err.Error() != want {
		t.Error(err.Error())
	}

	ok, err := binVindex.Verify(nil, []sqltypes.Value{sqltypes.NewVarChar("test2")}, [][]byte{[]byte("test1")})
	if err != nil {
		t.Error(err)
	}
	if ok {
		t.Errorf("Verify(%#v): true, want false", []byte("test2"))
	}
}

func TestSQLValue(t *testing.T) {
	val := sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("Test"))
	got, err := binVindex.(Unique).Map(nil, []sqltypes.Value{val})
	if err != nil {
		t.Error(err)
	}
	out := string(got[0])
	want := "\f\xbcf\x11\xf5T\vЀ\x9a8\x8d\xc9Za["
	if out != want {
		t.Errorf("Map(%#v): %#v, want %#v", val, out, want)
	}
}
