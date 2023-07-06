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
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	cell            = "zone1"
	hostname        = "localhost"
	ks              = "ks"

	//go:embed schema.sql
	schema string

	//go:embed vschema.json
	vschema string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      ks,
			SchemaSQL: schema,
			VSchema:   vschema,
		}
		var maxGrpcSize int64 = 256 * 1024 * 1024
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs,
			"--queryserver-config-max-result-size", "10000000",
			"--grpc_max_message_size", strconv.FormatInt(maxGrpcSize, 10))
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false); err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGatePlannerVersion = planbuilder.Gen4
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
			"--grpc_max_message_size", strconv.FormatInt(maxGrpcSize, 10),
			"--max_memory_rows", "999999",
			"--allow-kill-statement")
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}

		vtParams = clusterInstance.GetVTParams(ks)

		return m.Run()
	}()
	os.Exit(exitCode)
}

func setupData(t *testing.T, huge bool) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
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
		utils.Exec(t, conn, fmt.Sprintf("insert into test(id, msg, extra) values (%d, '%s', '%s'),(%d, '%s', '%s'),(%d, '%s', '%s'),(%d, '%s', '%s')",
			i, r1, r2,
			i+1, r2, r3,
			i+2, r3, r4,
			i+3, r4, r1))
	}
	if !huge {
		utils.AssertMatches(t, conn, `select count(*), min(id), max(id) from test`, `[[INT64(4) INT64(0) INT64(3)]]`)
		return
	}

	utils.AssertMatches(t, conn, `select count(*), min(id), max(id) from test`, `[[INT64(10000) INT64(0) INT64(9999)]]`)
	for i := 1; i < multiplier; i = i << 1 {
		utils.Exec(t, conn, fmt.Sprintf("insert into test(id, msg, extra) select id+%d, msg, extra from test", (initialRow+1)*i))
	}
	utils.AssertMatches(t, conn, `select count(*), min(id), max(id) from test`, `[[INT64(320000) INT64(0) INT64(319999)]]`)
}

func dropData(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "drop table if exists test")
	utils.Exec(t, conn, schema)
}

func getRandomString(size int) string {
	var str strings.Builder

	for i := 0; i < size; i++ {
		str.WriteByte(byte((rand.Int() % 26) + 97))
	}

	return str.String()
}
