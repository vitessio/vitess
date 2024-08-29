/*
Copyright 2024 The Vitess Authors.

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

package stress

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/transaction/twopc/utils"
)

var (
	clusterInstance   *cluster.LocalProcessCluster
	vtParams          mysql.ConnParams
	vtgateGrpcAddress string
	keyspaceName      = "ks"
	cell              = "zone1"
	hostname          = "localhost"
	sidecarDBName     = "vt_ks"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Reserve vtGate port in order to pass it to vtTablet
		clusterInstance.VtgateGrpcPort = clusterInstance.GetAndReservePort()

		// Set extra args for twopc
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
			"--transaction_mode", "TWOPC",
			"--grpc_use_effective_callerid",
		)
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs,
			"--twopc_enable",
			"--twopc_abandon_age", "1",
			"--migration_check_interval", "2s",
		)

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:             keyspaceName,
			SchemaSQL:        SchemaSQL,
			VSchema:          VSchema,
			SidecarDBName:    sidecarDBName,
			DurabilityPolicy: "semi_sync",
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-40", "40-80", "80-"}, 2, false); err != nil {
			return 1
		}

		// Start Vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}
		vtParams = clusterInstance.GetVTParams(keyspaceName)
		vtgateGrpcAddress = fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateGrpcPort)

		return m.Run()
	}()
	os.Exit(exitcode)
}

func start(t *testing.T) (*mysql.Conn, func()) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	cleanup(t)

	return conn, func() {
		conn.Close()
		cleanup(t)
	}
}

func cleanup(t *testing.T) {
	cluster.PanicHandler(t)

	utils.ClearOutTable(t, vtParams, "twopc_fuzzer_insert")
	utils.ClearOutTable(t, vtParams, "twopc_fuzzer_update")
	utils.ClearOutTable(t, vtParams, "twopc_t1")
}
