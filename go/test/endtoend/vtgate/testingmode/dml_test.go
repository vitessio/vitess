/*
Copyright 2021 The Vitess Authors.

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

package testingmode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestInsertTable(t *testing.T) {

	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	err = conn.WriteComInitDB(KeyspaceName)
	require.NoError(t, err)
	data, err := conn.ReadPacket()
	require.Equal(t, mysql.OKPacket, int(data[0]))
	require.NoError(t, err)

	execute(t, conn, "insert into t1(id1, id2) values (1, 2), (2, 3)")
	assertMatches(t, conn, "select * from t1", "[[INT64(1) INT64(2)] [INT64(2) INT64(3)]]")
}
