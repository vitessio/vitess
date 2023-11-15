/*
Copyright 2023 The Vitess Authors.

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
	"context"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func start(t *testing.T) (*mysql.Conn, func()) {
	vtConn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)

	deleteAll := func() {
		tables := []string{"music", "user"}
		for _, table := range tables {
			utils.Exec(t, vtConn, "delete from "+table)
		}
	}

	deleteAll()

	return vtConn, func() {
		deleteAll()
		vtConn.Close()
		cluster.PanicHandler(t)
	}
}

func TestExtraColLength(t *testing.T) {
	vtconn, closer := start(t)
	defer closer()

	_, err := utils.ExecAllowError(t, vtconn, "insert into test (one, two, three, four) values (1, 2, 3)")

	require.Equal(t, err, vterrors.VT03006())
}

func TestExtraRowLength(t *testing.T) {
	vtconn, closer := start(t)
	defer closer()

	_, err := utils.ExecAllowError(t, vtconn, "insert into test (one, two, three) values (1, 2, 3, 4)")

	require.Equal(t, err, vterrors.VT03006())
}
