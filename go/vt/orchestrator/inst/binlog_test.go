package inst

import (
	"testing"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
)

var testCoordinates = BinlogCoordinates{LogFile: "mysql-bin.000010", LogPos: 108}

func init() {
	config.Config.HostnameResolveMethod = "none"
	config.Config.KVClusterMasterPrefix = "test/master/"
	config.MarkConfigurationLoaded()
	log.SetLevel(log.ERROR)
}

func TestDetach(t *testing.T) {
	detachedCoordinates := testCoordinates.Detach()
	test.S(t).ExpectEquals(detachedCoordinates.LogFile, "//mysql-bin.000010:108")
	test.S(t).ExpectEquals(detachedCoordinates.LogPos, testCoordinates.LogPos)
}

func TestDetachedCoordinates(t *testing.T) {
	isDetached, detachedCoordinates := testCoordinates.ExtractDetachedCoordinates()
	test.S(t).ExpectFalse(isDetached)
	test.S(t).ExpectEquals(detachedCoordinates.LogFile, testCoordinates.LogFile)
	test.S(t).ExpectEquals(detachedCoordinates.LogPos, testCoordinates.LogPos)
}

func TestDetachedCoordinates2(t *testing.T) {
	detached := testCoordinates.Detach()
	isDetached, coordinates := detached.ExtractDetachedCoordinates()

	test.S(t).ExpectTrue(isDetached)
	test.S(t).ExpectEquals(coordinates.LogFile, testCoordinates.LogFile)
	test.S(t).ExpectEquals(coordinates.LogPos, testCoordinates.LogPos)
}

func TestPreviousFileCoordinates(t *testing.T) {
	previous, err := testCoordinates.PreviousFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(previous.LogFile, "mysql-bin.000009")
	test.S(t).ExpectEquals(previous.LogPos, int64(0))
}

func TestNextFileCoordinates(t *testing.T) {
	next, err := testCoordinates.NextFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(next.LogFile, "mysql-bin.000011")
	test.S(t).ExpectEquals(next.LogPos, int64(0))
}

func TestBinlogCoordinates(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c3 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 5000}
	c4 := BinlogCoordinates{LogFile: "mysql-bin.00112", LogPos: 104}

	test.S(t).ExpectTrue(c1.Equals(&c2))
	test.S(t).ExpectFalse(c1.Equals(&c3))
	test.S(t).ExpectFalse(c1.Equals(&c4))
	test.S(t).ExpectFalse(c1.SmallerThan(&c2))
	test.S(t).ExpectTrue(c1.SmallerThan(&c3))
	test.S(t).ExpectTrue(c1.SmallerThan(&c4))
	test.S(t).ExpectTrue(c3.SmallerThan(&c4))
	test.S(t).ExpectFalse(c3.SmallerThan(&c2))
	test.S(t).ExpectFalse(c4.SmallerThan(&c2))
	test.S(t).ExpectFalse(c4.SmallerThan(&c3))

	test.S(t).ExpectTrue(c1.SmallerThanOrEquals(&c2))
	test.S(t).ExpectTrue(c1.SmallerThanOrEquals(&c3))
}

func TestBinlogPrevious(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	cres, err := c1.PreviousFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql-bin.00016")

	c2 := BinlogCoordinates{LogFile: "mysql-bin.00100", LogPos: 104}
	cres, err = c2.PreviousFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql-bin.00099")

	c3 := BinlogCoordinates{LogFile: "mysql.00.prod.com.00100", LogPos: 104}
	cres, err = c3.PreviousFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql.00.prod.com.00099")

	c4 := BinlogCoordinates{LogFile: "mysql.00.prod.com.00000", LogPos: 104}
	_, err = c4.PreviousFileCoordinates()

	test.S(t).ExpectNotNil(err)
}

func TestBinlogCoordinatesAsKey(t *testing.T) {
	m := make(map[BinlogCoordinates]bool)

	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinates{LogFile: "mysql-bin.00022", LogPos: 104}
	c3 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c4 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 222}

	m[c1] = true
	m[c2] = true
	m[c3] = true
	m[c4] = true

	test.S(t).ExpectEquals(len(m), 3)
}

func TestFileNumberDistance(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinates{LogFile: "mysql-bin.00022", LogPos: 104}

	test.S(t).ExpectEquals(c1.FileNumberDistance(&c1), 0)
	test.S(t).ExpectEquals(c1.FileNumberDistance(&c2), 5)
	test.S(t).ExpectEquals(c2.FileNumberDistance(&c1), -5)
}

func TestFileNumber(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	fileNum, numLen := c1.FileNumber()

	test.S(t).ExpectEquals(fileNum, 17)
	test.S(t).ExpectEquals(numLen, 5)
}
