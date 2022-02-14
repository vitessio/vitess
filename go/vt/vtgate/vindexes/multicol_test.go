package vindexes

import (
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiColMisc(t *testing.T) {
	vindex, err := CreateVindex("multicol", "multicol", map[string]string{
		"column_count": "3",
	})
	require.NoError(t, err)

	multiColVdx, isMultiColVdx := vindex.(*MultiCol)
	assert.True(t, isMultiColVdx)

	assert.Equal(t, 3, multiColVdx.Cost())
	assert.Equal(t, "multicol", multiColVdx.String())
	assert.True(t, multiColVdx.IsUnique())
	assert.False(t, multiColVdx.NeedsVCursor())
	assert.True(t, multiColVdx.PartialVindex())
}

func TestMultiColMap(t *testing.T) {
	vindex, err := CreateVindex("multicol", "multicol", map[string]string{
		"column_count": "3",
	})
	require.NoError(t, err)
	mutiCol := vindex.(MultiColumn)

	got, err := mutiCol.Map(nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(255), sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(256), sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		// only one column provided, partial column for key range mapping.
		sqltypes.NewInt64(1),
	}, {
		// only two column provided, partial column for key range mapping.
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		// Invalid column value type.
		sqltypes.NewVarBinary("abcd"), sqltypes.NewInt64(256), sqltypes.NewInt64(256),
	}, {
		// Invalid column value type.
		sqltypes.NewInt64(256), sqltypes.NewInt64(256), sqltypes.NewVarBinary("abcd"),
	}})
	assert.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID("\x16\x6b\x40\x16\x6b\x40\x16\x6b"),
		key.DestinationKeyspaceID("\x25\x4e\x88\x16\x6b\x40\x16\x6b"),
		key.DestinationKeyspaceID("\xdd\x7c\x0b\x16\x6b\x40\x16\x6b"),
		key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x16\x6b\x40"), End: []byte("\x16\x6b\x41")}},
		key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x16\x6b\x40\x16\x6b\x40"), End: []byte("\x16\x6b\x40\x16\x6b\x41")}},
		key.DestinationNone{},
		key.DestinationNone{},
	}
	assert.Equal(t, want, got)
}
