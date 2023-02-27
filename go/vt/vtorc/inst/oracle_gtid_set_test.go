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

func TestExplode(t *testing.T) {
	{
		uuidSet := "00020194-3333-3333-3333-333333333333:7"
		entry, err := NewOracleGtidSetEntry(uuidSet)
		require.NoError(t, err)

		exploded := entry.Explode()
		require.Equal(t, len(exploded), 1)
		require.Equal(t, exploded[0].String(), "00020194-3333-3333-3333-333333333333:7")
	}
	{
		uuidSet := "00020194-3333-3333-3333-333333333333:1-3"
		entry, err := NewOracleGtidSetEntry(uuidSet)
		require.NoError(t, err)

		exploded := entry.Explode()
		require.Equal(t, len(exploded), 3)
		require.Equal(t, exploded[0].String(), "00020194-3333-3333-3333-333333333333:1")
		require.Equal(t, exploded[1].String(), "00020194-3333-3333-3333-333333333333:2")
		require.Equal(t, exploded[2].String(), "00020194-3333-3333-3333-333333333333:3")
	}
	{
		uuidSet := "00020194-3333-3333-3333-333333333333:1-3:6-7"
		entry, err := NewOracleGtidSetEntry(uuidSet)
		require.NoError(t, err)

		exploded := entry.Explode()
		require.Equal(t, len(exploded), 5)
		require.Equal(t, exploded[0].String(), "00020194-3333-3333-3333-333333333333:1")
		require.Equal(t, exploded[1].String(), "00020194-3333-3333-3333-333333333333:2")
		require.Equal(t, exploded[2].String(), "00020194-3333-3333-3333-333333333333:3")
		require.Equal(t, exploded[3].String(), "00020194-3333-3333-3333-333333333333:6")
		require.Equal(t, exploded[4].String(), "00020194-3333-3333-3333-333333333333:7")
	}
	{
		gtidSetVal := "00020192-1111-1111-1111-111111111111:29-30, 00020194-3333-3333-3333-333333333333:7-8"
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		require.NoError(t, err)

		exploded := gtidSet.Explode()
		require.Equal(t, len(exploded), 4)
		require.Equal(t, exploded[0].String(), "00020192-1111-1111-1111-111111111111:29")
		require.Equal(t, exploded[1].String(), "00020192-1111-1111-1111-111111111111:30")
		require.Equal(t, exploded[2].String(), "00020194-3333-3333-3333-333333333333:7")
		require.Equal(t, exploded[3].String(), "00020194-3333-3333-3333-333333333333:8")
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

func TestRetainUUID(t *testing.T) {
	gtidSetVal := "00020192-1111-1111-1111-111111111111:20-30, 00020194-3333-3333-3333-333333333333:7-8"
	{
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		require.NoError(t, err)

		require.Equal(t, len(gtidSet.GtidEntries), 2)
		removed := gtidSet.RetainUUID("00020194-3333-3333-3333-333333333333")
		require.True(t, removed)
		require.Equal(t, len(gtidSet.GtidEntries), 1)
		require.Equal(t, gtidSet.GtidEntries[0].String(), "00020194-3333-3333-3333-333333333333:7-8")

		removed = gtidSet.RetainUUID("00020194-3333-3333-3333-333333333333")
		require.False(t, removed)
		require.Equal(t, len(gtidSet.GtidEntries), 1)
		require.Equal(t, gtidSet.GtidEntries[0].String(), "00020194-3333-3333-3333-333333333333:7-8")

		removed = gtidSet.RetainUUID("230ea8ea-81e3-11e4-972a-e25ec4bd140a")
		require.True(t, removed)
		require.Equal(t, len(gtidSet.GtidEntries), 0)
	}
}

func TestRetainUUIDs(t *testing.T) {
	gtidSetVal := "00020192-1111-1111-1111-111111111111:20-30, 00020194-3333-3333-3333-333333333333:7-8"
	{
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		require.NoError(t, err)

		require.Equal(t, len(gtidSet.GtidEntries), 2)
		removed := gtidSet.RetainUUIDs([]string{"00020194-3333-3333-3333-333333333333", "00020194-5555-5555-5555-333333333333"})
		require.True(t, removed)
		require.Equal(t, len(gtidSet.GtidEntries), 1)
		require.Equal(t, gtidSet.GtidEntries[0].String(), "00020194-3333-3333-3333-333333333333:7-8")

		removed = gtidSet.RetainUUIDs([]string{"00020194-3333-3333-3333-333333333333", "00020194-5555-5555-5555-333333333333"})
		require.False(t, removed)
		require.Equal(t, len(gtidSet.GtidEntries), 1)
		require.Equal(t, gtidSet.GtidEntries[0].String(), "00020194-3333-3333-3333-333333333333:7-8")

		removed = gtidSet.RetainUUIDs([]string{"230ea8ea-81e3-11e4-972a-e25ec4bd140a"})
		require.True(t, removed)
		require.Equal(t, len(gtidSet.GtidEntries), 0)
	}
}

func TestSharedUUIDs(t *testing.T) {
	gtidSetVal := "00020192-1111-1111-1111-111111111111:20-30, 00020194-3333-3333-3333-333333333333:7-8"
	gtidSet, err := NewOracleGtidSet(gtidSetVal)
	require.NoError(t, err)
	{
		otherSet, err := NewOracleGtidSet("00020194-3333-3333-3333-333333333333:7-8,230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-2")
		require.NoError(t, err)
		{
			shared := gtidSet.SharedUUIDs(otherSet)
			require.Equal(t, len(shared), 1)
			require.Equal(t, shared[0], "00020194-3333-3333-3333-333333333333")
		}
		{
			shared := otherSet.SharedUUIDs(gtidSet)
			require.Equal(t, len(shared), 1)
			require.Equal(t, shared[0], "00020194-3333-3333-3333-333333333333")
		}
	}
	{
		otherSet, err := NewOracleGtidSet("00020194-4444-4444-4444-333333333333:7-8,230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-2")
		require.NoError(t, err)
		{
			shared := gtidSet.SharedUUIDs(otherSet)
			require.Equal(t, len(shared), 0)
		}
		{
			shared := otherSet.SharedUUIDs(gtidSet)
			require.Equal(t, len(shared), 0)
		}
	}
	{
		otherSet, err := NewOracleGtidSet("00020194-3333-3333-3333-333333333333:7-8,00020192-1111-1111-1111-111111111111:1-2")
		require.NoError(t, err)
		{
			shared := gtidSet.SharedUUIDs(otherSet)
			require.Equal(t, len(shared), 2)
		}
		{
			shared := otherSet.SharedUUIDs(gtidSet)
			require.Equal(t, len(shared), 2)
		}
	}
}
