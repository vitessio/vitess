package worker

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/wrangler"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// This file contains common test helper.

func runCommand(t *testing.T, wi *Instance, wr *wrangler.Wrangler, args []string) error {
	worker, done, err := wi.RunCommand(args, wr, false /* runFromCli */)
	if err != nil {
		return fmt.Errorf("Worker creation failed: %v", err)
	}
	if err := wi.WaitForCommand(worker, done); err != nil {
		return fmt.Errorf("Worker failed: %v", err)
	}

	t.Logf("Got status: %v", worker.StatusAsText())
	if worker.State() != WorkerStateDone {
		return fmt.Errorf("Worker finished but not successfully: %v", err)
	}
	return nil
}

// expectBlpCheckpointCreationQueries fakes out the queries which vtworker
// sends out to create the Binlog Player (BLP) checkpoint.
func expectBlpCheckpointCreationQueries(f *FakePoolConnection) {
	f.addExpectedQuery("CREATE DATABASE IF NOT EXISTS _vt", nil)
	f.addExpectedQuery("CREATE TABLE IF NOT EXISTS _vt.blp_checkpoint (\n"+
		"  source_shard_uid INT(10) UNSIGNED NOT NULL,\n"+
		"  pos VARCHAR(250) DEFAULT NULL,\n"+
		"  max_tps BIGINT(20) NOT NULL,\n"+
		"  max_replication_lag BIGINT(20) NOT NULL,\n"+
		"  time_updated BIGINT(20) UNSIGNED NOT NULL,\n"+
		"  transaction_timestamp BIGINT(20) UNSIGNED NOT NULL,\n"+
		"  flags VARCHAR(250) DEFAULT NULL,\n"+
		"  PRIMARY KEY (source_shard_uid)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8", nil)
	f.addExpectedQuery("INSERT INTO _vt.blp_checkpoint (source_shard_uid, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, flags) VALUES (0, 'MariaDB/12-34-5678', *", nil)
}

// sourceRdonlyFactory fakes out the MIN, MAX query on the primary key.
// (This query is used to calculate the split points for reading a table
// using multiple threads.)
func sourceRdonlyFactory(t *testing.T, dbAndTableName string, min, max int) func() (dbconnpool.PoolConnection, error) {
	f := NewFakePoolConnectionQuery(t, "sourceRdonly")
	f.addExpectedExecuteFetch(ExpectedExecuteFetch{
		Query: fmt.Sprintf("SELECT MIN(id), MAX(id) FROM %s", dbAndTableName),
		QueryResult: &sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name: "min",
					Type: sqltypes.Int64,
				},
				{
					Name: "max",
					Type: sqltypes.Int64,
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte(strconv.Itoa(min))),
					sqltypes.MakeString([]byte(strconv.Itoa(max))),
				},
			},
		},
	})
	return f.getFactory()
}
