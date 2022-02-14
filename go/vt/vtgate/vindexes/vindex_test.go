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
