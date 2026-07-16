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

This adds sharded keyspace dynamically in this test only and test sql insert, select
*/

package clustertest

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	addKeyspaceName   = "kstest"
	addKeyspaceSchema = `create table vt_user (
id bigint,
name varchar(64),
primary key (id)
) Engine=InnoDB`
	addKeyspaceVSchema = `{
 "sharded": true,
 "vindexes": {
   "hash_index": {
     "type": "hash"
   }
 },
 "tables": {
   "vt_user": {
     "column_vindexes": [
       {
         "column": "id",
         "name": "hash_index"
       }
     ]
   }
 }
}`
)

func TestAddKeyspace(t *testing.T) {
	setup(t)
	ctx := t.Context()

	_, err := clusterInstance.AddKeyspace(
		ctx,
		vitesst.WithKeyspace(addKeyspaceName).
			WithShardNames("-80", "80-").
			WithSchema(addKeyspaceSchema).
			WithVSchema(addKeyspaceVSchema),
	)
	require.NoError(t, err)

	require.NoError(t, clusterInstance.VTGate().Restart(ctx))
	vtParams = clusterInstance.VTParams(ctx, "")

	require.Eventually(t, func() bool {
		vschema, err := clusterInstance.VTGate().ReadVSchema(ctx)
		if err != nil {
			return false
		}
		encoded, err := json.Marshal(vschema)
		if err != nil {
			return false
		}
		return strings.Contains(string(encoded), addKeyspaceName)
	}, 30*time.Second, 100*time.Millisecond)

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "insert into vt_user(id, name) values(1,'name1')")

	qr := vitesst.Exec(t, conn, "select id, name from vt_user")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("name1")]]`; got != want {
		assert.Equalf(t, want, got, "select:\n%v want\n%v", got, want)
	}
}
