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

package ambiguous

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/vtgate/utils"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks_union"
	Cell            = "test_union"
	SchemaSQL       = `create table managers (managerid int);
create table properties (propertyid int, name varchar(50));
create table supervisions (managerid int, propertyid int);`

	VSchema = `
{
    "tables": {
        "managers": {},
        "properties": {},
        "supervisions": {}
    }
}
`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		clusterInstance.VtGateExtraArgs = []string{"-mysql_server_version=8.0.23"}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"0"}, 1, true)
		if err != nil {
			return 1
		}

		clusterInstance.VtGatePlannerVersion = planbuilder.V3
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "-enable_system_settings=true")
		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestAmbiguousCol(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	utils.AssertMatches(t, conn, "select name from properties join supervisions using (propertyId) join managers using (managerId) where propertyId = 1 AND managerId = 1", `[]`)
}
