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

package fuzz

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/transaction/twopc/utils"
	"vitess.io/vitess/go/test/vitesst"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

var (
	clusterInstance       *vitesst.Cluster
	vtParams              mysql.ConnParams
	vtgateGrpcAddress     string
	keyspaceName          = "ks"
	unshardedKeyspaceName = "uks"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithVTGateArgs(
				"--transaction-mode", "TWOPC",
				"--grpc-use-effective-callerid",
				"--tablet-refresh-interval", "2s",
			),
			vitesst.WithVTTabletArgs(
				"--twopc-abandon-age", "1",
				"--migration-check-interval", "2s",
			),
			vitesst.WithKeyspace(keyspaceName).
				WithShardNames("-40", "40-80", "80-").
				WithReplicas(2).
				WithSchema(SchemaSQL).
				WithVSchema(VSchema).
				WithDurabilityPolicy(policy.DurabilitySemiSync),
			vitesst.WithKeyspace(unshardedKeyspaceName).
				WithReplicas(2).
				WithVSchema("{}").
				WithDurabilityPolicy(policy.DurabilitySemiSync),
		)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		cleanup, err := cluster.Start(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer func() {
			if err := cleanup(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "cluster teardown:", err)
			}
		}()

		clusterInstance = cluster
		vtParams = cluster.VTParams(ctx, "")

		grpcAddr, err := cluster.VTGate().GRPCAddr(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		vtgateGrpcAddress = grpcAddr

		return m.Run()
	}()
	os.Exit(exitcode)
}

func start(t *testing.T) (*mysql.Conn, func()) {
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	cleanup(t)

	return conn, func() {
		conn.Close()
		cleanup(t)
	}
}

func cleanup(t *testing.T) {
	utils.ClearOutTable(t, vtParams, "twopc_fuzzer_insert")
	utils.ClearOutTable(t, vtParams, "twopc_fuzzer_update")
	utils.ClearOutTable(t, vtParams, "twopc_fuzzer_multi")
	utils.ClearOutTable(t, vtParams, "twopc_t1")
}
