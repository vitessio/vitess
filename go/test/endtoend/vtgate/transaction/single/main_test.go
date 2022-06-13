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

package vtgate

import (
	"context"
	_ "embed"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks"
	Cell            = "test"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
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
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = []string{"--transaction_mode", "SINGLE"}
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

func TestSingleOneWay(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer func() {
		utils.Exec(t, conn, `delete from txn_unique_constraints where txn_id = 'txn1'`)
		utils.Exec(t, conn, `delete from txn_unique_constraints where txn_id = 'txn3'`)
	}()

	utils.Exec(t, conn, `begin`)
	utils.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn1', 'txn_info_txn_id_txn1')`)
	utils.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn1', 'txn_info_mti_mc___mti1_mc1')`)
	utils.Exec(t, conn, `commit`)

	utils.Exec(t, conn, `begin`)
	utils.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn3', 'txn_info_txn_id_txn3')`)
	// should fail with duplicate key error and not with multi-db transaction
	utils.AssertContainsError(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn3', 'txn_info_mti_mc___mti1_mc1')`, `Duplicate entry 'txn_info_mti_mc___mti1_mc1'`)
	utils.Exec(t, conn, `rollback`)
}

func TestSingleReverseWay(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer func() {
		utils.Exec(t, conn, `delete from txn_unique_constraints where txn_id = 'txn1'`)
		utils.Exec(t, conn, `delete from txn_unique_constraints where txn_id = 'txn3'`)
	}()

	utils.Exec(t, conn, `begin`)
	utils.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn3', 'txn_info_txn_id_txn3')`)
	utils.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn3', 'txn_info_mti_mc___mti1_mc1')`)
	utils.Exec(t, conn, `commit`)

	utils.Exec(t, conn, `begin`)
	utils.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn1', 'txn_info_txn_id_txn1')`)
	// should fail with duplicate key error and not with multi-db transaction
	utils.AssertContainsError(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn1', 'txn_info_mti_mc___mti1_mc1')`, `Duplicate entry 'txn_info_mti_mc___mti1_mc1'`)
	utils.Exec(t, conn, `rollback`)
}

func TestSingleLookupDangleRow(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer func() {
		utils.Exec(t, conn, `delete from txn_unique_constraints where txn_id = 'txn1'`)
		utils.Exec(t, conn, `delete from txn_unique_constraints where txn_id = 'txn3'`)
	}()

	// insert a dangling row in lookup table
	utils.Exec(t, conn, `INSERT INTO uniqueConstraint_vdx(unique_constraint, keyspace_id) VALUES ('txn_info_mti_mc___mti1_mc1', 'J\xda\xf0p\x0e\xcc(\x8fਁ\xa7P\x86\xa5=')`)

	utils.Exec(t, conn, `begin`)
	// should succeed by validating that the original row does not exist for the unique_constraint, so this should succeed.
	utils.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn3', 'txn_info_mti_mc___mti1_mc1')`)
	utils.Exec(t, conn, `commit`)

	utils.AssertMatches(t, conn, `select txn_id, unique_constraint from txn_unique_constraints where txn_id = 'txn3'`, `[[VARCHAR("txn3") VARCHAR("txn_info_mti_mc___mti1_mc1")]]`)
}
