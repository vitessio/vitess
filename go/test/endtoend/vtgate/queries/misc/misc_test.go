/*
Copyright 2022 The Vitess Authors.

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

	mcmp.AssertMatches(`select b'1001', 0x9, B'010011011010'`, `[[VARBINARY("\t") VARBINARY("\t") VARBINARY("\x04\xda")]]`)
	mcmp.AssertMatches(`select b'1001', 0x9, B'010011011010' from t1`, `[[VARBINARY("\t") VARBINARY("\t") VARBINARY("\x04\xda")]]`)
	mcmp.AssertMatchesNoCompare(`select 1 + b'1001', 2 + 0x9, 3 + B'010011011010'`, `[[INT64(10) UINT64(11) INT64(1245)]]`, `[[UINT64(10) UINT64(11) UINT64(1245)]]`)
	mcmp.AssertMatchesNoCompare(`select 1 + b'1001', 2 + 0x9, 3 + B'010011011010' from t1`, `[[INT64(10) UINT64(11) INT64(1245)]]`, `[[UINT64(10) UINT64(11) UINT64(1245)]]`)
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

func TestDateTimeTimestampVals(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.AssertMatches(`select date'2022-08-03'`, `[[DATE("2022-08-03")]]`)
	mcmp.AssertMatches(`select time'12:34:56'`, `[[TIME("12:34:56")]]`)
	mcmp.AssertMatches(`select timestamp'2012-12-31 11:30:45'`, `[[DATETIME("2012-12-31 11:30:45")]]`)
}

func TestInvalidDateTimeTimestampVals(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.ExecAllowAndCompareError(`select date'2022'`)
	mcmp.ExecAllowAndCompareError(`select time'12:34:56:78'`)
	mcmp.ExecAllowAndCompareError(`select timestamp'2022'`)
}
