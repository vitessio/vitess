package inst

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewOracleGtidSetEntry(t *testing.T) {
	{
		uuidSet := "00020194-3333-3333-3333-333333333333:1-7"
		entry, err := NewOracleGtidSetEntry(uuidSet)
		require.NoError(t, err)
		require.Equal(t, entry.UUID, "00020194-3333-3333-3333-333333333333")
		require.Equal(t, entry.Ranges, "1-7")
	}
	{
		uuidSet := "00020194-3333-3333-3333-333333333333:1-7:10-20"
		entry, err := NewOracleGtidSetEntry(uuidSet)
		require.NoError(t, err)
		require.Equal(t, entry.UUID, "00020194-3333-3333-3333-333333333333")
		require.Equal(t, entry.Ranges, "1-7:10-20")
	}
	{
		uuidSet := "00020194-3333-3333-3333-333333333333"
		_, err := NewOracleGtidSetEntry(uuidSet)
		require.Error(t, err)
	}
}

func TestNewOracleGtidSet(t *testing.T) {
	{
		gtidSetVal := "00020192-1111-1111-1111-111111111111:20-30, 00020194-3333-3333-3333-333333333333:7-8"
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		require.NoError(t, err)

		require.Equal(t, len(gtidSet.GtidEntries), 2)
		require.Equal(t, gtidSet.GtidEntries[0].String(), "00020192-1111-1111-1111-111111111111:20-30")
		require.Equal(t, gtidSet.GtidEntries[1].String(), "00020194-3333-3333-3333-333333333333:7-8")
	}
	{
		gtidSetVal := "   ,,, , , 00020192-1111-1111-1111-111111111111:20-30,,,, 00020194-3333-3333-3333-333333333333:7-8,,  ,,"
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		require.NoError(t, err)

		require.Equal(t, len(gtidSet.GtidEntries), 2)
		require.Equal(t, gtidSet.GtidEntries[0].String(), "00020192-1111-1111-1111-111111111111:20-30")
		require.Equal(t, gtidSet.GtidEntries[1].String(), "00020194-3333-3333-3333-333333333333:7-8")
	}
	{
		gtidSetVal := "   ,,, , ,,  ,,"
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		require.NoError(t, err)

		require.Equal(t, len(gtidSet.GtidEntries), 0)
		require.True(t, gtidSet.IsEmpty())
	}
}

func TestRemoveUUID(t *testing.T) {
	gtidSetVal := "00020192-1111-1111-1111-111111111111:20-30, 00020194-3333-3333-3333-333333333333:7-8"
	{
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		require.NoError(t, err)

		require.Equal(t, len(gtidSet.GtidEntries), 2)
		gtidSet.RemoveUUID("00020194-3333-3333-3333-333333333333")
		require.Equal(t, len(gtidSet.GtidEntries), 1)
		require.Equal(t, gtidSet.GtidEntries[0].String(), "00020192-1111-1111-1111-111111111111:20-30")

		removed := gtidSet.RemoveUUID(`230ea8ea-81e3-11e4-972a-e25ec4bd140a`)
		require.False(t, removed)
		require.Equal(t, len(gtidSet.GtidEntries), 1)
		require.Equal(t, gtidSet.GtidEntries[0].String(), "00020192-1111-1111-1111-111111111111:20-30")
	}
	{
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		require.NoError(t, err)

		require.Equal(t, len(gtidSet.GtidEntries), 2)

		gtidSet.RemoveUUID("00020192-1111-1111-1111-111111111111")
		require.Equal(t, len(gtidSet.GtidEntries), 1)
		require.Equal(t, gtidSet.GtidEntries[0].String(), "00020194-3333-3333-3333-333333333333:7-8")

		gtidSet.RemoveUUID("00020194-3333-3333-3333-333333333333")
		require.True(t, gtidSet.IsEmpty())
	}
}
