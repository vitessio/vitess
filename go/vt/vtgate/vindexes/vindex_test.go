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
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

func TestVindexMap(t *testing.T) {
	ge, err := createRegionVindex(t, "region_experimental", "f1,f2", 1)
	assert.NoError(t, err)

	got, err := Map(ge, nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}})
	assert.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x01\x16k@\xb4J\xbaK\xd6")),
	}
	assert.Equal(t, want, got)

	hash, err := CreateVindex("hash", "hash", nil)
	assert.NoError(t, err)
	got, err = Map(hash, nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1),
	}})
	assert.NoError(t, err)
	want = []key.Destination{
		key.DestinationKeyspaceID([]byte("\x16k@\xb4J\xbaK\xd6")),
	}
	assert.Equal(t, want, got)
}

func TestVindexVerify(t *testing.T) {
	ge, err := createRegionVindex(t, "region_experimental", "f1,f2", 1)
	assert.NoError(t, err)

	got, err := Verify(ge, nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}},
		[][]byte{
			[]byte("\x01\x16k@\xb4J\xbaK\xd6"),
		},
	)
	assert.NoError(t, err)

	want := []bool{true}
	assert.Equal(t, want, got)

	hash, err := CreateVindex("hash", "hash", nil)
	assert.NoError(t, err)
	got, err = Verify(hash, nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1),
	}},
		[][]byte{
			[]byte("\x16k@\xb4J\xbaK\xd6"),
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}
