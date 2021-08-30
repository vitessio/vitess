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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestDatabaseFunc(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	exec(t, conn, "use ks")
	qr := exec(t, conn, "select database()")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARBINARY("ks")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}

func TestRenameFieldsOnOLAP(t *testing.T) {
	// note that this is testing vttest and not vtgate
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	_ = exec(t, conn, "set workload = olap")
	qr := exec(t, conn, "show tables")
	assert.Equal(t, `[[VARCHAR("aggr_test")] [VARCHAR("t1")] [VARCHAR("t1_id2_idx")] [VARCHAR("t1_last_insert_id")] [VARCHAR("t1_row_count")] [VARCHAR("t1_sharded")] [VARCHAR("t2")] [VARCHAR("t2_id4_idx")] [VARCHAR("vstream_test")]]`, fmt.Sprintf("%v", qr.Rows))
}
