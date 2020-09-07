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
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var xxHash SingleColumn

func init() {
	hv, err := CreateVindex("xxhash", "xxhash_name", map[string]string{"Table": "t", "Column": "c"})
	if err != nil {
		panic(err)
	}
	xxHash = hv.(SingleColumn)
}

func TestXXHashInfo(t *testing.T) {
	assert.Equal(t, 1, xxHash.Cost())
	assert.Equal(t, "xxhash_name", xxHash.String())
	assert.True(t, xxHash.IsUnique())
	assert.False(t, xxHash.NeedsVCursor())
}

func TestXXHashMap(t *testing.T) {
	tcases := []struct {
		in  sqltypes.Value
		out []byte
	}{{
		in:  sqltypes.NewVarChar("test1"),
		out: []byte{0xd0, 0x1a, 0xb7, 0xe4, 0xd6, 0x97, 0x8f, 0xb},
	}, {
		in:  sqltypes.NewVarChar("test2"),
		out: []byte{0x87, 0xeb, 0x11, 0x71, 0x4c, 0xa, 0xe, 0x89},
	}, {
		in:  sqltypes.NewVarChar("testaverylongvaluetomakesurethisworks"),
		out: []byte{0x81, 0xd8, 0xc3, 0x8e, 0xd, 0x85, 0xe, 0x6a},
	}, {
		in:  sqltypes.NewInt64(1),
		out: []byte{0xd4, 0x64, 0x5, 0x36, 0x76, 0x12, 0xb4, 0xb7},
	}, {
		in:  sqltypes.NULL,
		out: []byte{0x99, 0xe9, 0xd8, 0x51, 0x37, 0xdb, 0x46, 0xef},
	}, {
		in:  sqltypes.NewInt64(-1),
		out: []byte{0xd8, 0xe2, 0xa6, 0xa7, 0xc8, 0xc7, 0x62, 0x3d},
	}, {
		in:  sqltypes.NewUint64(18446744073709551615),
		out: []byte{0x47, 0x7c, 0xfa, 0x8d, 0x6d, 0x8f, 0x1f, 0x8d},
	}, {
		in:  sqltypes.NewInt64(9223372036854775807),
		out: []byte{0xb3, 0x7e, 0xb0, 0x1f, 0x7b, 0xff, 0xaf, 0xd8},
	}, {
		in:  sqltypes.NewUint64(9223372036854775807),
		out: []byte{0xb3, 0x7e, 0xb0, 0x1f, 0x7b, 0xff, 0xaf, 0xd8},
	}, {
		in:  sqltypes.NewInt64(-9223372036854775808),
		out: []byte{0x10, 0x2c, 0x27, 0xdd, 0xb2, 0x6a, 0x60, 0x9e},
	}}

	for _, tcase := range tcases {
		got, err := xxHash.Map(nil, []sqltypes.Value{tcase.in})
		if err != nil {
			t.Error(err)
		}
		out := []byte(got[0].(key.DestinationKeyspaceID))
		if !bytes.Equal(tcase.out, out) {
			t.Errorf("Map(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
	}
}

func TestXXHashVerify(t *testing.T) {
	ids := []sqltypes.Value{sqltypes.NewUint64(1), sqltypes.NewUint64(2)}
	ksids := [][]byte{{0xd4, 0x64, 0x5, 0x36, 0x76, 0x12, 0xb4, 0xb7}, {0xd4, 0x64, 0x5, 0x36, 0x76, 0x12, 0xb4, 0xb7}}
	got, err := xxHash.Verify(nil, ids, ksids)
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("xxHash.Verify: %v, want %v", got, want)
	}
}

func BenchmarkXXHash(b *testing.B) {
	for _, benchSize := range []struct {
		name string
		n    int
	}{
		{"8B", 8},
		{"32B", 32},
		{"64B", 64},
		{"512B", 512},
		{"1KB", 1e3},
		{"4KB", 4e3},
	} {
		input := make([]byte, benchSize.n)
		for i := range input {
			input[i] = byte(i)
		}

		name := fmt.Sprintf("xxHash,direct,bytes,n=%s", benchSize.name)
		b.Run(name, func(b *testing.B) {
			benchmarkXXHashBytes(b, input)
		})

	}
}

var sinkXXHash []byte

func benchmarkXXHashBytes(b *testing.B, input []byte) {
	b.SetBytes(int64(len(input)))
	for i := 0; i < b.N; i++ {
		sinkXXHash = vXXHash(input)
	}
}
