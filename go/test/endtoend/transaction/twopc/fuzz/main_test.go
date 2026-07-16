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
	"testing"
	"time"

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

func setup(t *testing.T) {
	t.Helper()

	clusterInstance = nil
	vtParams = mysql.ConnParams{}
	vtgateGrpcAddress = ""
	count = 0
	moveTablesCount = 0
	t.Cleanup(func() {
		clusterInstance = nil
		vtParams = mysql.ConnParams{}
		vtgateGrpcAddress = ""
		count = 0
		moveTablesCount = 0
	})

	ctx := t.Context()
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
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")

	grpcAddr, err := cluster.VTGate().GRPCAddr(ctx)
	require.NoError(t, err)
	vtgateGrpcAddress = grpcAddr
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
