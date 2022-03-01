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

package vtgate

import (
	"context"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestColumnNames(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "create table uks.t2(id bigint,phone bigint,msg varchar(100),primary key(id)) Engine=InnoDB")
	defer utils.Exec(t, conn, "drop table uks.t2")

	qr := utils.Exec(t, conn, "SELECT t1.id as t1id, t2.id as t2id, t2.phone as t2phn FROM ks.t1 cross join uks.t2 where t1.id = t2.id ORDER BY t2.phone")

	assert.Equal(t, 3, len(qr.Fields))
	assert.Equal(t, "t1id", qr.Fields[0].Name)
	assert.Equal(t, "t2id", qr.Fields[1].Name)
	assert.Equal(t, "t2phn", qr.Fields[2].Name)
}
