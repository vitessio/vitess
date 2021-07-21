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
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

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
	ks1          = "test_keyspace"
	redirected   = "redirected"
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {

		topology := new(vttestpb.VTTestTopology)
		topology.Keyspaces = []*vttestpb.Keyspace{
			{
				Name: ks1,
				Shards: []*vttestpb.Shard{
					{Name: "-80"},
					{Name: "80-"},
				},
				RdonlyCount:  1,
				ReplicaCount: 2,
			},
			{
				Name:       redirected,
				ServedFrom: ks1,
			},
		}

		var cfg vttest.Config
		cfg.Topology = topology
		cfg.SchemaDir = os.Getenv("VTROOT") + "/test/vttest_schema"
		cfg.DefaultSchemaDir = os.Getenv("VTROOT") + "/test/vttest_schema/default"
		cfg.PersistentMode = true

		localCluster = &vttest.LocalCluster{
			Config: cfg,
		}

		err := localCluster.Setup()
		defer localCluster.TearDown()
		if err != nil {
			return 1, err
		}

		grpcAddress = fmt.Sprintf("localhost:%d", localCluster.Env.PortForProtocol("vtcombo", "grpc"))
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
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)
	resultMap := make(map[string]interface{})
	respByte, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(respByte, &resultMap)
	require.Nil(t, err)
	cmd := resultMap["cmdline"]
	require.NotNil(t, cmd, "cmdline is not available in debug vars")
	tmp, _ := cmd.([]interface{})
	require.Contains(t, tmp[0], "vtcombo")

	ctx := context.Background()
	conn, err := vtgateconn.Dial(ctx, grpcAddress)
	require.Nil(t, err)
	defer conn.Close()

	idStart, rowCount := 1000, 500
	insertManyRows(ctx, t, conn, idStart, rowCount)
	assertInsertedRowsExist(ctx, t, conn, idStart, rowCount)
	assertCanInsertRow(ctx, t, conn)
	assertTablesPresent(t)

	err = localCluster.TearDown()
	require.Nil(t, err)
	err = localCluster.Setup()
	require.Nil(t, err)

	assertInsertedRowsExist(ctx, t, conn, idStart, rowCount)
	assertTablesPresent(t)
}

func assertInsertedRowsExist(ctx context.Context, t *testing.T, conn *vtgateconn.VTGateConn, idStart, rowCount int) {
	cur := conn.Session(ks1+":-80@rdonly", nil)
	bindVariables := map[string]*querypb.BindVariable{
		"id_start": {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(idStart), 10))},
	}
	res, err := cur.Execute(ctx, "select * from test_table where id >= :id_start", bindVariables)
	require.Nil(t, err)

	assert.Equal(t, rowCount, len(res.Rows))

	cur = conn.Session(redirected+":-80@replica", nil)
	bindVariables = map[string]*querypb.BindVariable{
		"id_start": {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(idStart), 10))},
	}
	res, err = cur.Execute(ctx, "select * from test_table where id = :id_start", bindVariables)
	require.Nil(t, err)
	require.Equal(t, 1, len(res.Rows))
	assert.Equal(t, "VARCHAR(\"test1000\")", res.Rows[0][1].String())
}

func assertCanInsertRow(ctx context.Context, t *testing.T, conn *vtgateconn.VTGateConn) {
	cur := conn.Session(ks1+":80-@master", nil)
	_, err := cur.Execute(ctx, "begin", nil)
	require.Nil(t, err)

	i := 0x810000000000000
	bindVariables := map[string]*querypb.BindVariable{
		"id":          {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(i), 10))},
		"msg":         {Type: querypb.Type_VARCHAR, Value: []byte("test" + strconv.FormatInt(int64(i), 10))},
		"keyspace_id": {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(i), 10))},
	}
	query := "insert into test_table (id, msg, keyspace_id) values (:id, :msg, :keyspace_id)"
	_, err = cur.Execute(ctx, query, bindVariables)
	require.Nil(t, err)

	_, err = cur.Execute(ctx, "commit", nil)
	require.Nil(t, err)
}

func insertManyRows(ctx context.Context, t *testing.T, conn *vtgateconn.VTGateConn, idStart, rowCount int) {
	cur := conn.Session(ks1+":-80@master", nil)

	query := "insert into test_table (id, msg, keyspace_id) values (:id, :msg, :keyspace_id)"
	_, err := cur.Execute(ctx, "begin", nil)
	require.Nil(t, err)

	for i := idStart; i < idStart+rowCount; i++ {
		bindVariables := map[string]*querypb.BindVariable{
			"id":          {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(i), 10))},
			"msg":         {Type: querypb.Type_VARCHAR, Value: []byte("test" + strconv.FormatInt(int64(i), 10))},
			"keyspace_id": {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(int64(i), 10))},
		}
		_, err = cur.Execute(ctx, query, bindVariables)
		require.Nil(t, err)
	}

	_, err = cur.Execute(ctx, "commit", nil)
	require.Nil(t, err)
}

func assertTablesPresent(t *testing.T) {
	tmpCmd := exec.Command("vtctlclient", "-vtctl_client_protocol", "grpc", "-server", grpcAddress, "-stderrthreshold", "0", "ListAllTablets", "test")

	log.Infof("Running vtctlclient with command: %v", tmpCmd.Args)

	output, err := tmpCmd.CombinedOutput()
	require.Nil(t, err)

	numMaster, numReplica, numRdonly, numDash80, num80Dash := 0, 0, 0, 0, 0
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if !strings.HasPrefix(line, "test-") {
			continue
		}
		parts := strings.Split(line, " ")
		assert.Equal(t, "test_keyspace", parts[1])

		switch parts[3] {
		case "master":
			numMaster++
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

	assert.Equal(t, 2, numMaster)
	assert.Equal(t, 2, numReplica)
	assert.Equal(t, 2, numRdonly)
	assert.Equal(t, 3, numDash80)
	assert.Equal(t, 3, num80Dash)
}
