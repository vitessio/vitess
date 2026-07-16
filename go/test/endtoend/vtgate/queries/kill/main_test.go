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

package kill

import (
	"context"
	_ "embed"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	ks              = "ks"

	//go:embed schema.sql
	schema string

	//go:embed vschema.json
	vschema string
)

func setup(t *testing.T) {
	ctx := t.Context()
	var maxGrpcSize int64 = 256 * 1024 * 1024
	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(ks).
			WithShardNames("-80", "80-").
			WithSchema(schema).
			WithVSchema(vschema),
		vitesst.WithVTTabletArgs(
			"--queryserver-config-max-result-size", "10000000",
			"--grpc-max-message-size", strconv.FormatInt(maxGrpcSize, 10),
		),
		vitesst.WithVTGateArgs(
			"--grpc-max-message-size", strconv.FormatInt(maxGrpcSize, 10),
			"--max-memory-rows", "999999",
			"--allow-kill-statement",
		),
	)
	require.NoError(t, err)
	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := cleanup(context.WithoutCancel(ctx)); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")
}

func setupData(t *testing.T, huge bool) {
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	initialRow := 9999
	multiplier := 32
	if !huge {
		initialRow = 4
		multiplier = 0
	}
	r1 := getRandomString(10)
	r2 := getRandomString(20)
	r3 := getRandomString(30)
	r4 := getRandomString(40)

	for i := 0; i < initialRow; i += 4 {
		vitesst.Exec(t, conn, fmt.Sprintf("insert into test(id, msg, extra) values (%d, '%s', '%s'),(%d, '%s', '%s'),(%d, '%s', '%s'),(%d, '%s', '%s')",
			i, r1, r2,
			i+1, r2, r3,
			i+2, r3, r4,
			i+3, r4, r1))
	}
	if !huge {
		vitesst.AssertMatches(t, conn, `select count(*), min(id), max(id) from test`, `[[INT64(4) INT64(0) INT64(3)]]`)
		return
	}

	vitesst.AssertMatches(t, conn, `select count(*), min(id), max(id) from test`, `[[INT64(10000) INT64(0) INT64(9999)]]`)
	for i := 1; i < multiplier; i = i << 1 {
		vitesst.Exec(t, conn, fmt.Sprintf("insert into test(id, msg, extra) select id+%d, msg, extra from test", (initialRow+1)*i))
	}
	vitesst.AssertMatches(t, conn, `select count(*), min(id), max(id) from test`, `[[INT64(320000) INT64(0) INT64(319999)]]`)
}

func dropData(t *testing.T) {
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "drop table if exists test")
	vitesst.Exec(t, conn, "drop table if exists test_idx")
	vitesst.ExecMulti(t, conn, schema)
}

func getRandomString(size int) string {
	var str strings.Builder

	for range size {
		str.WriteByte(byte((rand.Int() % 26) + 97))
	}

	return str.String()
}
