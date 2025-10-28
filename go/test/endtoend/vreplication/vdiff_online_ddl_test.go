package vreplication

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/utils"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// TestOnlineDDLVDiff is to run a vdiff on a table that is part of an OnlineDDL workflow.
func TestOnlineDDLVDiff(t *testing.T) {
	setSidecarDBName("_vt")
	originalRdonly := defaultRdonly
	originalReplicas := defaultReplicas
	defer func() {
		defaultRdonly = originalRdonly
		defaultReplicas = originalReplicas
	}()
	defaultRdonly = 0
	defaultReplicas = 0
	vc = setupMinimalCluster(t)
	defer vc.TearDown()
	keyspace := defaultSourceKs
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createQuery := "create table temp (id int, name varchar(100), blb blob, primary key (id))"
	dropQuery := "drop table temp"
	alterQuery := "alter table temp add column extra1 int not null default 0"
	insertTemplate := "insert into temp (id, name, blb) values (%d, 'name%d', 'blb%d')"
	updateTemplate := "update temp set name = 'name_%d' where id = %d"
	execOnlineDDL(t, "direct", keyspace, createQuery)
	defer execOnlineDDL(t, "direct", keyspace, dropQuery)

	var output string

	t.Run("OnlineDDL VDiff", func(t *testing.T) {
		var done = make(chan bool)
		go populate(ctx, t, done, insertTemplate, updateTemplate)

		waitForAdditionalRows(t, keyspace, "temp", 100)
		output = execOnlineDDL(t, "vitess --postpone-completion", keyspace, alterQuery)
		uuid := strings.TrimSpace(output)
		waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", keyspace, uuid), binlogdatapb.VReplicationWorkflowState_Running.String())
		waitForAdditionalRows(t, keyspace, "temp", 200)

		require.NoError(t, waitForCondition("online ddl migration to be ready to complete", func() bool {
			response := onlineDDLShow(t, keyspace, uuid)
			if len(response.Migrations) > 0 &&
				response.Migrations[0].ReadyToComplete == true {
				return true
			}
			return false
		}, defaultTimeout))

		want := &expectedVDiff2Result{
			state:               "completed",
			minimumRowsCompared: 200,
			hasMismatch:         false,
			shards:              []string{"0"},
		}
		doVtctldclientVDiff(t, keyspace, uuid, "zone1", want)

		cancel()
		<-done
	})
}

func onlineDDLShow(t *testing.T, keyspace, uuid string) *vtctldata.GetSchemaMigrationsResponse {
	var response vtctldata.GetSchemaMigrationsResponse
	output, err := vc.VtctldClient.OnlineDDLShow(keyspace, uuid)
	require.NoError(t, err, output)
	err = protojson.Unmarshal([]byte(output), &response)
	require.NoErrorf(t, err, "error unmarshalling OnlineDDL showresponse")
	return &response
}

func execOnlineDDL(t *testing.T, strategy, keyspace, query string) string {
	output, err := vc.VtctldClient.ExecuteCommandWithOutput("ApplySchema", utils.GetFlagVariantForTests("--ddl-strategy"), strategy, "--sql", query, keyspace)
	require.NoError(t, err, output)
	output = strings.TrimSpace(output)
	if strategy != "direct" {
		// We expect a UUID as the only output, but when using --ddl_strategy we get a warning mixed into the output:
		//   Flag --ddl_strategy has been deprecated, use --ddl-strategy instead
		// In order to prevent this and other similar future issues, lets hunt for the UUID (which should be on its own line)
		// in the returned output.
		uuid := ""
		lines := strings.Split(output, "\n")
		for i := range lines {
			line := strings.TrimSpace(lines[i])
			if schema.IsOnlineDDLUUID(line) {
				uuid = line
				break
			}
		}
		require.NotEmpty(t, uuid, "UUID not returned in ApplySchema command output: %v", output)
		output = uuid // return the UUID instead of the original output
		err = waitForCondition("online ddl to start", func() bool {
			response := onlineDDLShow(t, keyspace, uuid)
			if len(response.Migrations) > 0 &&
				(response.Migrations[0].Status == vtctldata.SchemaMigration_RUNNING ||
					response.Migrations[0].Status == vtctldata.SchemaMigration_COMPLETE) {
				return true
			}
			return false
		}, workflowStateTimeout)
		require.NoError(t, err)
		// The online ddl migration is set to SchemaMigration_RUNNING before it creates the
		// _vt.vreplication records. Hence wait for the vreplication workflow to be created as well.
		waitForWorkflowToBeCreated(t, vc, fmt.Sprintf("%s.%s", keyspace, uuid))
	}
	return output
}

func waitForAdditionalRows(t *testing.T, keyspace, table string, count int) {
	vtgateConn, cancel := getVTGateConn()
	defer cancel()

	numRowsStart := getNumRows(t, vtgateConn, keyspace, table)
	numRows := 0
	shortCtx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	for {
		switch {
		case shortCtx.Err() != nil:
			require.FailNowf(t, "Timed out waiting for additional rows", "wanted %d rows, got %d rows", count, numRows)
		default:
			numRows = getNumRows(t, vtgateConn, keyspace, table)
			if numRows >= numRowsStart+count {
				return
			}
			time.Sleep(defaultTick)
		}
	}
}

func getNumRows(t *testing.T, vtgateConn *mysql.Conn, keyspace, table string) int {
	qr := execVtgateQuery(t, vtgateConn, keyspace, "SELECT COUNT(*) FROM "+table)
	require.NotNil(t, qr)
	numRows, err := strconv.Atoi(qr.Rows[0][0].ToString())
	require.NoError(t, err)
	return numRows
}

func populate(ctx context.Context, t *testing.T, done chan bool, insertTemplate, updateTemplate string) {
	defer close(done)
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	id := 1
	for {
		select {
		case <-ctx.Done():
			log.Infof("load cancelled")
			return
		default:
			query := fmt.Sprintf(insertTemplate, id, id, id)
			_, err := vtgateConn.ExecuteFetch(query, 1, false)
			require.NoErrorf(t, err, "error in insert")
			query = fmt.Sprintf(updateTemplate, id, id)
			_, err = vtgateConn.ExecuteFetch(query, 1, false)
			require.NoErrorf(t, err, "error in update")
			id++
			time.Sleep(10 * time.Millisecond)
		}
	}
}
