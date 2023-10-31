/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"fmt"
	osExec "os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

var ctx = context.Background()

func TestDatabaseFunc(t *testing.T) {
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	exec(t, conn, "use ks")
	qr := exec(t, conn, "select database()")
	require.Equal(t, `[[VARCHAR("ks")]]`, fmt.Sprintf("%v", qr.Rows))
}

func TestSysNumericPrecisionScale(t *testing.T) {
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := exec(t, conn, "select numeric_precision, numeric_scale from information_schema.columns where table_schema = 'ks' and table_name = 't1'")
	assert.True(t, qr.Fields[0].Type == qr.Rows[0][0].Type())
	assert.True(t, qr.Fields[1].Type == qr.Rows[0][1].Type())
}

func TestCreateAndDropDatabase(t *testing.T) {
	// note that this is testing vttest and not vtgate
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// cleanup the keyspace from the topology.
	defer func() {
		// the corresponding database needs to be created in advance.
		// a subsequent DeleteKeyspace command returns the error of 'node doesn't exist' without it.
		_ = exec(t, conn, "create database testitest")

		_, err := osExec.Command("vtctldclient", "--server", grpcAddress, "DeleteKeyspace", "--recursive", "--force", "testitest").CombinedOutput()
		require.NoError(t, err)
	}()

	// run it 3 times.
	for count := 0; count < 3; count++ {
		t.Run(fmt.Sprintf("exec:%d", count), func(t *testing.T) {
			_ = exec(t, conn, "create database testitest")
			_ = exec(t, conn, "use testitest")
			qr := exec(t, conn, "select round(1.58)")
			assert.Equal(t, `[[DECIMAL(2)]]`, fmt.Sprintf("%v", qr.Rows))

			_ = exec(t, conn, "drop database testitest")
			_, err = conn.ExecuteFetch("use testitest", 1000, true)
			require.Error(t, err)
		})
	}
}
