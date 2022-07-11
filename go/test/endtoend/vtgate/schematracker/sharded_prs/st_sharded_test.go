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

package shardedprs

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks"
	Cell            = "test"
	SchemaSQL       = `
create table t2(
	id3 bigint,
	id4 bigint,
	primary key(id3)
) Engine=InnoDB;

create table t2_id4_idx(
	id bigint not null auto_increment,
	id4 bigint,
	id3 bigint,
	primary key(id),
	key idx_id4(id4)
) Engine=InnoDB;

create table t8(
	id8 bigint,
	testId bigint,
	primary key(id8)
) Engine=InnoDB;
`

	VSchema = `
{
  "sharded": true,
  "vindexes": {
    "unicode_loose_xxhash" : {
	  "type": "unicode_loose_xxhash"
    },
    "unicode_loose_md5" : {
	  "type": "unicode_loose_md5"
    },
    "hash": {
      "type": "hash"
    },
    "xxhash": {
      "type": "xxhash"
    },
    "t2_id4_idx": {
      "type": "lookup_hash",
      "params": {
        "table": "t2_id4_idx",
        "from": "id4",
        "to": "id3",
        "autocommit": "true"
      },
      "owner": "t2"
    }
  },
  "tables": {
    "t2": {
      "column_vindexes": [
        {
          "column": "id3",
          "name": "hash"
        },
        {
          "column": "id4",
          "name": "t2_id4_idx"
        }
      ]
    },
    "t2_id4_idx": {
      "column_vindexes": [
        {
          "column": "id4",
          "name": "hash"
        }
      ]
    },
    "t8": {
      "column_vindexes": [
        {
          "column": "id8",
          "name": "hash"
        }
      ]
    }
  }
}`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal", "--schema_change_signal_user", "userData1"}
		clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal", "--queryserver-config-schema-change-signal-interval", "5", "--queryserver-config-strict-table-acl", "--queryserver-config-acl-exempt-acl", "userData1", "--table-acl-config", "dummy.json"}

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
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 2, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		err = waitForVTGateAndVTTablet()
		if err != nil {
			fmt.Println(err)
			return 1
		}

		// PRS on the second VTTablet of each shard
		// This is supposed to change the primary tablet in the shards, meaning that a different tablet
		// will be responsible for sending schema tracking updates.
		for _, shard := range clusterInstance.Keyspaces[0].Shards {
			err := clusterInstance.VtctlclientProcess.InitializeShard(KeyspaceName, shard.Name, Cell, shard.Vttablets[1].TabletUID)
			if err != nil {
				fmt.Println(err)
				return 1
			}
		}

		err = waitForVTGateAndVTTablet()
		if err != nil {
			fmt.Println(err)
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

func waitForVTGateAndVTTablet() error {
	timeout := time.After(5 * time.Minute)
	for {
		select {
		case <-timeout:
			return vterrors.New(vtrpcpb.Code_INTERNAL, "timeout")
		default:
			err := clusterInstance.WaitForTabletsToHealthyInVtgate()
			if err != nil {
				continue
			}
			return nil
		}
	}
}

func TestAddColumn(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SkipIfBinaryIsBelowVersion(t, 14, "vtgate")
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	_ = utils.Exec(t, conn, `alter table t2 add column aaa int`)
	time.Sleep(10 * time.Second)
	_ = utils.Exec(t, conn, "select aaa from t2")
}
