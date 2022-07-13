package misc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		tables := []string{"t1"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}

func TestBitVals(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values (0,0)")

	mcmp.AssertMatches(`select b'1001', 0x9`, `[[VARBINARY("\t") VARBINARY("\t")]]`)
	mcmp.AssertMatches(`select b'1001', 0x9 from t1`, `[[VARBINARY("\t") VARBINARY("\t")]]`)
	mcmp.AssertMatches(`select 1 + b'1001', 2 + 0x9`, `[[INT64(10) UINT64(11)]]`)
	mcmp.AssertMatches(`select 1 + b'1001', 2 + 0x9 from t1`, `[[INT64(10) UINT64(11)]]`)
}

func TestHexVals(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values (0,0)")

	mcmp.AssertMatches(`select x'09', 0x9`, `[[VARBINARY("\t") VARBINARY("\t")]]`)
	mcmp.AssertMatches(`select X'09', 0x9 from t1`, `[[VARBINARY("\t") VARBINARY("\t")]]`)
	mcmp.AssertMatches(`select 1 + x'09', 2 + 0x9`, `[[UINT64(10) UINT64(11)]]`)
	mcmp.AssertMatches(`select 1 + X'09', 2 + 0x9 from t1`, `[[UINT64(10) UINT64(11)]]`)
}
