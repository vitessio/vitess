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

package vtcombo

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vttest"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var (
	localCluster *vttest.LocalCluster
	grpcAddress  string
	vtctldAddr   string
	mysqlAddress string
	ks1          = "test_keyspace"
	jsonTopo     = `
{
	"keyspaces": [
		{
			"name": "test_keyspace",
			"shards": [{"name": "-80"}, {"name": "80-"}],
			"rdonlyCount": 1,
			"replicaCount": 2
		},
		{
			"name": "routed",
			"shards": [{"name": "0"}]
		}
	],
	"routing_rules": {
		"rules": [{
            "from_table": "routed.routed_table",
            "to_tables": [
                "routed.test_table"
            ]
        }]
	}
}`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		var topology vttestpb.VTTestTopology

		data := vttest.JSONTopoData(&topology)
		err := data.Set(jsonTopo)
		if err != nil {
			return 1, err
		}

		var cfg vttest.Config
		cfg.Topology = &topology
		cfg.SchemaDir = os.Getenv("VTROOT") + "/test/vttest_schema"
		cfg.DefaultSchemaDir = os.Getenv("VTROOT") + "/test/vttest_schema/default"
		cfg.PersistentMode = true

		localCluster = &vttest.LocalCluster{
			Config: cfg,
		}

		err = localCluster.Setup()
		defer localCluster.TearDown()
		if err != nil {
			return 1, err
		}

		grpcAddress = fmt.Sprintf("localhost:%d", localCluster.Env.PortForProtocol("vtcombo", "grpc"))
		mysqlAddress = fmt.Sprintf("localhost:%d", localCluster.Env.PortForProtocol("vtcombo_mysql_port", ""))
		vtctldAddr = fmt.Sprintf("localhost:%d", localCluster.Env.PortForProtocol("vtcombo", "port"))

		return m.Run(), nil
	}()
	if err != nil {
		log.Errorf("top level error: %v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

func TestStandalone(t *testing.T) {
	// validate debug vars
	resp, err := http.Get(fmt.Sprintf("http://%s/debug/vars", vtctldAddr))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	resultMap := make(map[string]any)
	respByte, _ := io.ReadAll(resp.Body)
	err = json.Unmarshal(respByte, &resultMap)
	require.NoError(t, err)
	cmd := resultMap["cmdline"]
	require.NotNil(t, cmd, "cmdline is not available in debug vars")
	tmp, _ := cmd.([]any)
	require.Contains(t, tmp[0], "vtcombo")

	ctx := context.Background()
	conn, err := vtgateconn.Dial(ctx, grpcAddress)
	require.NoError(t, err)
	defer conn.Close()

	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = mysqlAddress
	cfg.DBName = "routed@primary"
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer db.Close()

	idStart, rowCount := 1000, 500
	insertManyRows(ctx, t, conn, idStart, rowCount)
	assertInsertedRowsExist(ctx, t, conn, idStart, rowCount)
	assertRouting(ctx, t, db)
	assertCanInsertRow(ctx, t, conn)
	assertTabletsPresent(t)

	err = localCluster.TearDown()
	require.NoError(t, err)
	err = localCluster.Setup()
	require.NoError(t, err)

	assertInsertedRowsExist(ctx, t, conn, idStart, rowCount)
	assertTabletsPresent(t)
	assertTransactionalityAndRollbackObeyed(ctx, t, conn, idStart)
}

func assertInsertedRowsExist(ctx context.Context, t *testing.T, conn *vtgateconn.VTGateConn, idStart, rowCount int) {
	cur := conn.Session(ks1+":-80@rdonly", nil)
	bindVariables := map[string]*querypb.BindVariable{
		"id_start": {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(idStart), 10))},
	}
	res, err := cur.Execute(ctx, "select * from test_table where id >= :id_start", bindVariables)
	require.NoError(t, err)

	assert.Equal(t, rowCount, len(res.Rows))
}

func assertRouting(ctx context.Context, t *testing.T, db *sql.DB) {
	// insert into test table
	_, err := db.ExecContext(ctx, `insert into test_table (id, msg, keyspace_id) values (?, ?, ?)`, 1, "message", 1)
	require.NoError(t, err)

	// read from routed table
	row := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM routed_table")
	require.NoError(t, row.Err())
	var count uint64
	require.NoError(t, row.Scan(&count))
	require.NotZero(t, count)
}

func assertCanInsertRow(ctx context.Context, t *testing.T, conn *vtgateconn.VTGateConn) {
	cur := conn.Session(ks1+":80-@primary", nil)
	_, err := cur.Execute(ctx, "begin", nil)
	require.NoError(t, err)

	i := 0x810000000000000
	bindVariables := map[string]*querypb.BindVariable{
		"id":          {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(i), 10))},
		"msg":         {Type: querypb.Type_VARCHAR, Value: []byte("test" + strconv.FormatInt(int64(i), 10))},
		"keyspace_id": {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(i), 10))},
	}
	query := "insert into test_table (id, msg, keyspace_id) values (:id, :msg, :keyspace_id)"
	_, err = cur.Execute(ctx, query, bindVariables)
	require.NoError(t, err)

	_, err = cur.Execute(ctx, "commit", nil)
	require.NoError(t, err)
}

func insertManyRows(ctx context.Context, t *testing.T, conn *vtgateconn.VTGateConn, idStart, rowCount int) {
	cur := conn.Session(ks1+":-80@primary", nil)

	query := "insert into test_table (id, msg, keyspace_id) values (:id, :msg, :keyspace_id)"
	_, err := cur.Execute(ctx, "begin", nil)
	require.NoError(t, err)

	for i := idStart; i < idStart+rowCount; i++ {
		bindVariables := map[string]*querypb.BindVariable{
			"id":          {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(i), 10))},
			"msg":         {Type: querypb.Type_VARCHAR, Value: []byte("test" + strconv.FormatInt(int64(i), 10))},
			"keyspace_id": {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(i), 10))},
		}
		_, err = cur.Execute(ctx, query, bindVariables)
		require.NoError(t, err)
	}

	_, err = cur.Execute(ctx, "commit", nil)
	require.NoError(t, err)
}

func assertTabletsPresent(t *testing.T) {
	tmpCmd := exec.Command("vtctlclient", "--vtctl_client_protocol", "grpc", "--server", grpcAddress, "--stderrthreshold", "0", "ListAllTablets", "--", "test")

	log.Infof("Running vtctlclient with command: %v", tmpCmd.Args)

	output, err := tmpCmd.CombinedOutput()
	require.NoError(t, err)

	numPrimary, numReplica, numRdonly, numDash80, num80Dash, numRouted := 0, 0, 0, 0, 0, 0
	lines := strings.Split(string(output), "\n")

	for _, line := range lines {
		if !strings.HasPrefix(line, "test-") {
			continue
		}
		parts := strings.Split(line, " ")
		if parts[1] == "routed" {
			numRouted++
			continue
		}

		assert.Equal(t, "test_keyspace", parts[1])

		switch parts[3] {
		case "primary":
			numPrimary++
		case "replica":
			numReplica++
		case "rdonly":
			numRdonly++
		default:
			t.Logf("invalid tablet type %s", parts[3])
		}

		switch parts[2] {
		case "-80":
			numDash80++
		case "80-":
			num80Dash++
		default:
			t.Logf("invalid shard %s", parts[2])
		}

	}

	assert.Equal(t, 2, numPrimary)
	assert.Equal(t, 2, numReplica)
	assert.Equal(t, 2, numRdonly)
	assert.Equal(t, 3, numDash80)
	assert.Equal(t, 3, num80Dash)
	assert.NotZero(t, numRouted)
}

func assertTransactionalityAndRollbackObeyed(ctx context.Context, t *testing.T, conn *vtgateconn.VTGateConn, idStart int) {
	cur := conn.Session(ks1+":80-@primary", &querypb.ExecuteOptions{})

	i := idStart + 1
	msg := "test"
	bindVariables := map[string]*querypb.BindVariable{
		"id":          {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(i), 10))},
		"msg":         {Type: querypb.Type_VARCHAR, Value: []byte(msg)},
		"keyspace_id": {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(i), 10))},
	}
	query := "insert into test_table (id, msg, keyspace_id) values (:id, :msg, :keyspace_id)"
	_, err := cur.Execute(ctx, query, bindVariables)
	require.NoError(t, err)

	bindVariables = map[string]*querypb.BindVariable{
		"msg": {Type: querypb.Type_VARCHAR, Value: []byte(msg)},
	}
	res, err := cur.Execute(ctx, "select * from test_table where msg = :msg", bindVariables)
	require.NoError(t, err)
	require.Equal(t, 1, len(res.Rows))

	_, err = cur.Execute(ctx, "begin", nil)
	require.NoError(t, err)

	msg2 := msg + "2"
	bindVariables = map[string]*querypb.BindVariable{
		"id":  {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(i), 10))},
		"msg": {Type: querypb.Type_VARCHAR, Value: []byte(msg2)},
	}
	query = "update test_table set msg = :msg where id = :id"
	_, err = cur.Execute(ctx, query, bindVariables)
	require.NoError(t, err)

	_, err = cur.Execute(ctx, "rollback", nil)
	require.NoError(t, err)

	bindVariables = map[string]*querypb.BindVariable{
		"msg": {Type: querypb.Type_VARCHAR, Value: []byte(msg2)},
	}
	res, err = cur.Execute(ctx, "select * from test_table where msg = :msg", bindVariables)
	require.NoError(t, err)
	require.Equal(t, 0, len(res.Rows))
}
