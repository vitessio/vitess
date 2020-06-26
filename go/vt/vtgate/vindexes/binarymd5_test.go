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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var binVindex SingleColumn

func init() {
	vindex, _ := CreateVindex("binary_md5", "binary_md5_varchar", nil)
	binVindex = vindex.(SingleColumn)
}

func TestBinaryMD5Info(t *testing.T) {
	assert.Equal(t, 1, binVindex.Cost())
	assert.Equal(t, "binary_md5_varchar", binVindex.String())
	assert.True(t, binVindex.IsUnique())
	assert.False(t, binVindex.NeedsVCursor())
}

func TestBinaryMD5Map(t *testing.T) {
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
		got, err := binVindex.Map(nil, []sqltypes.Value{sqltypes.NewVarBinary(tcase.in)})
		if err != nil {
			t.Error(err)
		}
		out := string(got[0].(key.DestinationKeyspaceID))
		if out != tcase.out {
			t.Errorf("Map(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
	}
}

func TestBinaryMD5Verify(t *testing.T) {
	ids := []sqltypes.Value{sqltypes.NewVarBinary("Test"), sqltypes.NewVarBinary("TEst")}
	ksids := [][]byte{[]byte("\f\xbcf\x11\xf5T\vЀ\x9a8\x8d\xc9Za["), []byte("\f\xbcf\x11\xf5T\vЀ\x9a8\x8d\xc9Za[")}
	got, err := binVindex.Verify(nil, ids, ksids)
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("binaryMD5.Verify: %v, want %v", got, want)
	}
}

func TestSQLValue(t *testing.T) {
	val := sqltypes.NewVarBinary("Test")
	got, err := binVindex.Map(nil, []sqltypes.Value{val})
	require.NoError(t, err)
	out := string(got[0].(key.DestinationKeyspaceID))
	want := "\f\xbcf\x11\xf5T\vЀ\x9a8\x8d\xc9Za["
	if out != want {
		t.Errorf("Map(%#v): %#v, want %#v", val, out, want)
	}
}
