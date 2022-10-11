package inst

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtorc/config"
)

var testCoordinates = BinlogCoordinates{LogFile: "mysql-bin.000010", LogPos: 108}

func init() {
	config.MarkConfigurationLoaded()
}

func TestDetach(t *testing.T) {
	detachedCoordinates := testCoordinates.Detach()
	require.Equal(t, detachedCoordinates.LogFile, "//mysql-bin.000010:108")
	require.Equal(t, detachedCoordinates.LogPos, testCoordinates.LogPos)
}

func TestDetachedCoordinates(t *testing.T) {
	isDetached, detachedCoordinates := testCoordinates.ExtractDetachedCoordinates()
	require.False(t, isDetached)
	require.Equal(t, detachedCoordinates.LogFile, testCoordinates.LogFile)
	require.Equal(t, detachedCoordinates.LogPos, testCoordinates.LogPos)
}

func TestDetachedCoordinates2(t *testing.T) {
	detached := testCoordinates.Detach()
	isDetached, coordinates := detached.ExtractDetachedCoordinates()

	require.True(t, isDetached)
	require.Equal(t, coordinates.LogFile, testCoordinates.LogFile)
	require.Equal(t, coordinates.LogPos, testCoordinates.LogPos)
}

func TestPreviousFileCoordinates(t *testing.T) {
	previous, err := testCoordinates.PreviousFileCoordinates()

	require.NoError(t, err)
	require.Equal(t, previous.LogFile, "mysql-bin.000009")
	require.Equal(t, previous.LogPos, int64(0))
}

func TestNextFileCoordinates(t *testing.T) {
	next, err := testCoordinates.NextFileCoordinates()

	require.NoError(t, err)
	require.Equal(t, next.LogFile, "mysql-bin.000011")
	require.Equal(t, next.LogPos, int64(0))
}

func TestBinlogCoordinates(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c3 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 5000}
	c4 := BinlogCoordinates{LogFile: "mysql-bin.00112", LogPos: 104}

	require.True(t, c1.Equals(&c2))
	require.False(t, c1.Equals(&c3))
	require.False(t, c1.Equals(&c4))
	require.False(t, c1.SmallerThan(&c2))
	require.True(t, c1.SmallerThan(&c3))
	require.True(t, c1.SmallerThan(&c4))
	require.True(t, c3.SmallerThan(&c4))
	require.False(t, c3.SmallerThan(&c2))
	require.False(t, c4.SmallerThan(&c2))
	require.False(t, c4.SmallerThan(&c3))

	require.True(t, c1.SmallerThanOrEquals(&c2))
	require.True(t, c1.SmallerThanOrEquals(&c3))
}

func TestBinlogPrevious(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	cres, err := c1.PreviousFileCoordinates()

	require.NoError(t, err)
	require.Equal(t, c1.Type, cres.Type)
	require.Equal(t, cres.LogFile, "mysql-bin.00016")

	c2 := BinlogCoordinates{LogFile: "mysql-bin.00100", LogPos: 104}
	cres, err = c2.PreviousFileCoordinates()

	require.NoError(t, err)
	require.Equal(t, c1.Type, cres.Type)
	require.Equal(t, cres.LogFile, "mysql-bin.00099")

	c3 := BinlogCoordinates{LogFile: "mysql.00.prod.com.00100", LogPos: 104}
	cres, err = c3.PreviousFileCoordinates()

	require.NoError(t, err)
	require.Equal(t, c1.Type, cres.Type)
	require.Equal(t, cres.LogFile, "mysql.00.prod.com.00099")

	c4 := BinlogCoordinates{LogFile: "mysql.00.prod.com.00000", LogPos: 104}
	_, err = c4.PreviousFileCoordinates()

	require.Error(t, err)
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

	require.Equal(t, len(m), 3)
}

func TestFileNumberDistance(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinates{LogFile: "mysql-bin.00022", LogPos: 104}

	require.Equal(t, c1.FileNumberDistance(&c1), 0)
	require.Equal(t, c1.FileNumberDistance(&c2), 5)
	require.Equal(t, c2.FileNumberDistance(&c1), -5)
}

func TestFileNumber(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	fileNum, numLen := c1.FileNumber()

	require.Equal(t, fileNum, 17)
	require.Equal(t, numLen, 5)
}
