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

package vtgate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql"
)

func TestSetSystemVariable(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	updateWithoutWhere := `update t1 set id2 = 42`
	assertMatches(t, conn, updateWithoutWhere, `[]`)

	exec(t, conn, "SET @@sql_safe_updates=1")
	assertMatches(t, conn, "select @@sql_safe_updates", `[[INT64(1)]]`)

	// with safe updates on, the next query should fail
	_, err = conn.ExecuteFetch(updateWithoutWhere, 1000, true)
	require.Error(t, err)

	exec(t, conn, "SET @@sql_safe_updates=0")
	_, err = conn.ExecuteFetch(updateWithoutWhere, 1000, true)
	require.NoError(t, err)
}
