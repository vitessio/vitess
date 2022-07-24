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

package normalize

import (
	"flag"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks_normalize"
	cell            = "test_normalize"
	schemaSQL       = `
create table t1(
  id bigint unsigned not null,
  charcol char(10),
  vcharcol varchar(50),
  bincol binary(50),
  varbincol varbinary(50),
  floatcol float,
  deccol decimal(5,2),
  bitcol bit,
  datecol date,
  enumcol enum('small', 'medium', 'large'),
  setcol set('a', 'b', 'c'),
  jsoncol json,
  geocol geometry,
  binvalcol varbinary(50),
  binnumcol varbinary(50),
  primary key(id)
) Engine=InnoDB;
`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: schemaSQL,
		}
		clusterInstance.VtGateExtraArgs = []string{}
		clusterInstance.VtTabletExtraArgs = []string{}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-"}, 1, false)
		if err != nil {
			return 1
		}

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
