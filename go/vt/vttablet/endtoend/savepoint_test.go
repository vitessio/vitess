/*
Copyright 2020 The Vitess Authors.

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

package endtoend

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

func TestSavepointInTransactionWithSRollback(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval = 5", nil)

	prequery := "savepoint a"
	query := "insert into vitess_test (intval, floatval, charval, binval) values (5, null, null, null)"

	_, err := client.BeginExecute(query, nil, []string{prequery})
	require.NoError(t, err)

	qr, err := client.Execute("select intval from vitess_test where intval = 5", nil)
	require.NoError(t, err)
	require.Equal(t, "[[INT32(5)]]", fmt.Sprintf("%v", qr.Rows))

	_, err = client.Execute("rollback to a", nil)
	require.NoError(t, err)

	err = client.Commit()
	require.NoError(t, err)

	qr, err = client.Execute("select intval from vitess_test where intval = 5", nil)
	require.NoError(t, err)
	require.Empty(t, qr.Rows)
}

func TestSavepointInTransactionWithRelease(t *testing.T) {
	client := framework.NewClient()

	vstart := framework.DebugVars()

	prequery := "savepoint a"
	query := "insert into vitess_test (intval, floatval, charval, binval) values (5, null, null, null)"

	_, err := client.BeginExecute(query, nil, []string{prequery})
	require.NoError(t, err)

	qr, err := client.Execute("select intval from vitess_test where intval in (5, 6)", nil)
	require.NoError(t, err)
	require.Equal(t, "[[INT32(5)]]", fmt.Sprintf("%v", qr.Rows))

	_, err = client.Execute("savepoint b", nil)
	require.NoError(t, err)

	_, err = client.Execute("insert into vitess_test (intval, floatval, charval, binval) values (6, null, null, null)", nil)
	require.NoError(t, err)

	qr, err = client.Execute("select intval from vitess_test where intval in (5, 6)", nil)
	require.NoError(t, err)
	require.Equal(t, "[[INT32(5)] [INT32(6)]]", fmt.Sprintf("%v", qr.Rows))

	_, err = client.Execute("release savepoint b", nil)
	require.NoError(t, err)

	// After release savepoint does not exists
	_, err = client.Execute("rollback to b", nil)
	require.Error(t, err)

	qr, err = client.Execute("select intval from vitess_test where intval in (5, 6)", nil)
	require.NoError(t, err)
	require.Equal(t, "[[INT32(5)] [INT32(6)]]", fmt.Sprintf("%v", qr.Rows))

	_, err = client.Execute("rollback to a", nil)
	require.NoError(t, err)

	err = client.Commit()
	require.NoError(t, err)

	qr, err = client.Execute("select intval from vitess_test where intval in (5, 6)", nil)
	require.NoError(t, err)
	require.Empty(t, qr.Rows)

	vend := framework.DebugVars()

	expectedDiffs := []struct {
		tag  string
		diff int
	}{{
		tag:  "Queries/Histograms/Savepoint/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/Release/Count",
		diff: 1,
	}, {
		tag:  "Queries/Histograms/RollbackSavepoint/Count",
		diff: 2,
	}}
	for _, expected := range expectedDiffs {
		t.Run(expected.tag, func(t *testing.T) {
			compareIntDiff(t, vend, expected.tag, vstart, expected.diff)
		})
	}
}

func TestSavepointWithoutTransaction(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval = 6", nil)

	_, err := client.Execute("savepoint a", nil)
	require.NoError(t, err)

	_, err = client.Execute("insert into vitess_test (intval, floatval, charval, binval) values (6, null, null, null)", nil)
	require.NoError(t, err)

	_, err = client.Execute("savepoint b", nil)
	require.NoError(t, err)

	qr, err := client.Execute("select intval from vitess_test where intval = 6", nil)
	require.NoError(t, err)
	require.Equal(t, "[[INT32(6)]]", fmt.Sprintf("%v", qr.Rows))

	// Without transaction there is no savepoint.
	_, err = client.Execute("release savepoint a", nil)
	require.Error(t, err)

	// Without transaction there is no savepoint.
	_, err = client.Execute("rollback to a", nil)
	require.Error(t, err)
}
