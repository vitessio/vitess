package inst

import (
	"testing"

	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
)

func TestNewOracleGtidSetEntry(t *testing.T) {
	{
		uuidSet := "00020194-3333-3333-3333-333333333333:1-7"
		entry, err := NewOracleGtidSetEntry(uuidSet)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(entry.UUID, "00020194-3333-3333-3333-333333333333")
		test.S(t).ExpectEquals(entry.Ranges, "1-7")
	}
	{
		uuidSet := "00020194-3333-3333-3333-333333333333:1-7:10-20"
		entry, err := NewOracleGtidSetEntry(uuidSet)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(entry.UUID, "00020194-3333-3333-3333-333333333333")
		test.S(t).ExpectEquals(entry.Ranges, "1-7:10-20")
	}
	{
		uuidSet := "00020194-3333-3333-3333-333333333333"
		_, err := NewOracleGtidSetEntry(uuidSet)
		test.S(t).ExpectNotNil(err)
	}
}

func TestExplode(t *testing.T) {
	{
		uuidSet := "00020194-3333-3333-3333-333333333333:7"
		entry, err := NewOracleGtidSetEntry(uuidSet)
		test.S(t).ExpectNil(err)

		exploded := entry.Explode()
		test.S(t).ExpectEquals(len(exploded), 1)
		test.S(t).ExpectEquals(exploded[0].String(), "00020194-3333-3333-3333-333333333333:7")
	}
	{
		uuidSet := "00020194-3333-3333-3333-333333333333:1-3"
		entry, err := NewOracleGtidSetEntry(uuidSet)
		test.S(t).ExpectNil(err)

		exploded := entry.Explode()
		test.S(t).ExpectEquals(len(exploded), 3)
		test.S(t).ExpectEquals(exploded[0].String(), "00020194-3333-3333-3333-333333333333:1")
		test.S(t).ExpectEquals(exploded[1].String(), "00020194-3333-3333-3333-333333333333:2")
		test.S(t).ExpectEquals(exploded[2].String(), "00020194-3333-3333-3333-333333333333:3")
	}
	{
		uuidSet := "00020194-3333-3333-3333-333333333333:1-3:6-7"
		entry, err := NewOracleGtidSetEntry(uuidSet)
		test.S(t).ExpectNil(err)

		exploded := entry.Explode()
		test.S(t).ExpectEquals(len(exploded), 5)
		test.S(t).ExpectEquals(exploded[0].String(), "00020194-3333-3333-3333-333333333333:1")
		test.S(t).ExpectEquals(exploded[1].String(), "00020194-3333-3333-3333-333333333333:2")
		test.S(t).ExpectEquals(exploded[2].String(), "00020194-3333-3333-3333-333333333333:3")
		test.S(t).ExpectEquals(exploded[3].String(), "00020194-3333-3333-3333-333333333333:6")
		test.S(t).ExpectEquals(exploded[4].String(), "00020194-3333-3333-3333-333333333333:7")
	}
	{
		gtidSetVal := "00020192-1111-1111-1111-111111111111:29-30, 00020194-3333-3333-3333-333333333333:7-8"
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		test.S(t).ExpectNil(err)

		exploded := gtidSet.Explode()
		test.S(t).ExpectEquals(len(exploded), 4)
		test.S(t).ExpectEquals(exploded[0].String(), "00020192-1111-1111-1111-111111111111:29")
		test.S(t).ExpectEquals(exploded[1].String(), "00020192-1111-1111-1111-111111111111:30")
		test.S(t).ExpectEquals(exploded[2].String(), "00020194-3333-3333-3333-333333333333:7")
		test.S(t).ExpectEquals(exploded[3].String(), "00020194-3333-3333-3333-333333333333:8")
	}
}

func TestNewOracleGtidSet(t *testing.T) {
	{
		gtidSetVal := "00020192-1111-1111-1111-111111111111:20-30, 00020194-3333-3333-3333-333333333333:7-8"
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		test.S(t).ExpectNil(err)

		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 2)
		test.S(t).ExpectEquals(gtidSet.GtidEntries[0].String(), "00020192-1111-1111-1111-111111111111:20-30")
		test.S(t).ExpectEquals(gtidSet.GtidEntries[1].String(), "00020194-3333-3333-3333-333333333333:7-8")
	}
	{
		gtidSetVal := "   ,,, , , 00020192-1111-1111-1111-111111111111:20-30,,,, 00020194-3333-3333-3333-333333333333:7-8,,  ,,"
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		test.S(t).ExpectNil(err)

		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 2)
		test.S(t).ExpectEquals(gtidSet.GtidEntries[0].String(), "00020192-1111-1111-1111-111111111111:20-30")
		test.S(t).ExpectEquals(gtidSet.GtidEntries[1].String(), "00020194-3333-3333-3333-333333333333:7-8")
	}
	{
		gtidSetVal := "   ,,, , ,,  ,,"
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		test.S(t).ExpectNil(err)

		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 0)
		test.S(t).ExpectTrue(gtidSet.IsEmpty())
	}
}

func TestRemoveUUID(t *testing.T) {
	gtidSetVal := "00020192-1111-1111-1111-111111111111:20-30, 00020194-3333-3333-3333-333333333333:7-8"
	{
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		test.S(t).ExpectNil(err)

		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 2)
		gtidSet.RemoveUUID("00020194-3333-3333-3333-333333333333")
		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 1)
		test.S(t).ExpectEquals(gtidSet.GtidEntries[0].String(), "00020192-1111-1111-1111-111111111111:20-30")

		removed := gtidSet.RemoveUUID(`230ea8ea-81e3-11e4-972a-e25ec4bd140a`)
		test.S(t).ExpectFalse(removed)
		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 1)
		test.S(t).ExpectEquals(gtidSet.GtidEntries[0].String(), "00020192-1111-1111-1111-111111111111:20-30")
	}
	{
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		test.S(t).ExpectNil(err)

		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 2)

		gtidSet.RemoveUUID("00020192-1111-1111-1111-111111111111")
		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 1)
		test.S(t).ExpectEquals(gtidSet.GtidEntries[0].String(), "00020194-3333-3333-3333-333333333333:7-8")

		gtidSet.RemoveUUID("00020194-3333-3333-3333-333333333333")
		test.S(t).ExpectTrue(gtidSet.IsEmpty())
	}
}

func TestRetainUUID(t *testing.T) {
	gtidSetVal := "00020192-1111-1111-1111-111111111111:20-30, 00020194-3333-3333-3333-333333333333:7-8"
	{
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		test.S(t).ExpectNil(err)

		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 2)
		removed := gtidSet.RetainUUID("00020194-3333-3333-3333-333333333333")
		test.S(t).ExpectTrue(removed)
		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 1)
		test.S(t).ExpectEquals(gtidSet.GtidEntries[0].String(), "00020194-3333-3333-3333-333333333333:7-8")

		removed = gtidSet.RetainUUID("00020194-3333-3333-3333-333333333333")
		test.S(t).ExpectFalse(removed)
		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 1)
		test.S(t).ExpectEquals(gtidSet.GtidEntries[0].String(), "00020194-3333-3333-3333-333333333333:7-8")

		removed = gtidSet.RetainUUID("230ea8ea-81e3-11e4-972a-e25ec4bd140a")
		test.S(t).ExpectTrue(removed)
		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 0)
	}
}

func TestRetainUUIDs(t *testing.T) {
	gtidSetVal := "00020192-1111-1111-1111-111111111111:20-30, 00020194-3333-3333-3333-333333333333:7-8"
	{
		gtidSet, err := NewOracleGtidSet(gtidSetVal)
		test.S(t).ExpectNil(err)

		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 2)
		removed := gtidSet.RetainUUIDs([]string{"00020194-3333-3333-3333-333333333333", "00020194-5555-5555-5555-333333333333"})
		test.S(t).ExpectTrue(removed)
		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 1)
		test.S(t).ExpectEquals(gtidSet.GtidEntries[0].String(), "00020194-3333-3333-3333-333333333333:7-8")

		removed = gtidSet.RetainUUIDs([]string{"00020194-3333-3333-3333-333333333333", "00020194-5555-5555-5555-333333333333"})
		test.S(t).ExpectFalse(removed)
		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 1)
		test.S(t).ExpectEquals(gtidSet.GtidEntries[0].String(), "00020194-3333-3333-3333-333333333333:7-8")

		removed = gtidSet.RetainUUIDs([]string{"230ea8ea-81e3-11e4-972a-e25ec4bd140a"})
		test.S(t).ExpectTrue(removed)
		test.S(t).ExpectEquals(len(gtidSet.GtidEntries), 0)
	}
}

func TestSharedUUIDs(t *testing.T) {
	gtidSetVal := "00020192-1111-1111-1111-111111111111:20-30, 00020194-3333-3333-3333-333333333333:7-8"
	gtidSet, err := NewOracleGtidSet(gtidSetVal)
	test.S(t).ExpectNil(err)
	{
		otherSet, err := NewOracleGtidSet("00020194-3333-3333-3333-333333333333:7-8,230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-2")
		test.S(t).ExpectNil(err)
		{
			shared := gtidSet.SharedUUIDs(otherSet)
			test.S(t).ExpectEquals(len(shared), 1)
			test.S(t).ExpectEquals(shared[0], "00020194-3333-3333-3333-333333333333")
		}
		{
			shared := otherSet.SharedUUIDs(gtidSet)
			test.S(t).ExpectEquals(len(shared), 1)
			test.S(t).ExpectEquals(shared[0], "00020194-3333-3333-3333-333333333333")
		}
	}
	{
		otherSet, err := NewOracleGtidSet("00020194-4444-4444-4444-333333333333:7-8,230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-2")
		test.S(t).ExpectNil(err)
		{
			shared := gtidSet.SharedUUIDs(otherSet)
			test.S(t).ExpectEquals(len(shared), 0)
		}
		{
			shared := otherSet.SharedUUIDs(gtidSet)
			test.S(t).ExpectEquals(len(shared), 0)
		}
	}
	{
		otherSet, err := NewOracleGtidSet("00020194-3333-3333-3333-333333333333:7-8,00020192-1111-1111-1111-111111111111:1-2")
		test.S(t).ExpectNil(err)
		{
			shared := gtidSet.SharedUUIDs(otherSet)
			test.S(t).ExpectEquals(len(shared), 2)
		}
		{
			shared := otherSet.SharedUUIDs(gtidSet)
			test.S(t).ExpectEquals(len(shared), 2)
		}
	}
}
