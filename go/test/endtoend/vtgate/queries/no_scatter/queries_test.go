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

package aggregation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

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

func TestFailsWhenForcedToScatter(t *testing.T) {
	vtconn, closer := start(t)
	defer closer()

	utils.Exec(t, vtconn, "insert into music(id, user_id) values(1,1), (2,5), (3,1), (4,2), (5,3), (6,4), (7,5)")
	utils.Exec(t, vtconn, "insert into user(id, name) values(1,'toto'), (2,'tata'), (3,'titi'), (4,'tete'), (5,'foo')")

	_, err := utils.ExecAllowError(t, vtconn, "select * from user") // fails since we have disallowed scatter
	require.ErrorContains(t, err, "plan includes scatter, which is disallowed")

	_ = utils.Exec(t, vtconn, "select /*vt+ ALLOW_SCATTER */ * from user")          // passes thanks to the comment directive
	_ = utils.Exec(t, vtconn, "vexplain select /*vt+ ALLOW_SCATTER */ * from user") // passes thanks to the comment directive
}
