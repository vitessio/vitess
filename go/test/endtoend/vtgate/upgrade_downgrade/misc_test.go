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

package upgradedowngrade

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/vtgate/utils"
)

func TestUpgradeDowngradeQueryServing(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer utils.Exec(t, conn, "delete from t1; delete from t2;")

	utils.Exec(t, conn, "insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
	utils.Exec(t, conn, "insert into t2(id3, id4) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")

	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", "[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)]]")
	utils.AssertMatches(t, conn, "select id3 from t2 order by id3", "[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)]]")
}
