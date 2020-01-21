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

 This file contains integration tests for the go/vt/binlog package.
 It sets up filtered replication between two shards and checks how data flows
 through binlog streamer.
*/

package binlog

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/proto/query"
)

var (
	localCluster *cluster.LocalProcessCluster
	cell         = "zone1"
	hostname     = "localhost"
	keyspaceName = "ks"
	tableName    = "test_table"
	sqlSchema    = `
					create table %s(
					id bigint(20) unsigned auto_increment,
					msg varchar(64),
					primary key (id),
					index by_msg (msg)
					) Engine=InnoDB
`
	insertSql       = `insert into %s(id,msg) values(%d, "%s")`
	commonTabletArg = []string{
		"-vreplication_healthcheck_topology_refresh", "1s",
		"-vreplication_healthcheck_retry_delay", "1s",
		"-vreplication_retry_delay", "1s",
		"-degraded_threshold", "5s",
		"-lock_tables_timeout", "5s",
		"-watch_replication_stream",
		"-enable_replication_reporter",
		"-serving_state_grace_period", "1s",
		"-binlog_player_protocol", "grpc",
		"-enable-autocommit",
	}
	vSchema = `
		{
		  "sharded": false,
		  "vindexes": {
			"hash_index": {
			  "type": "hash"
			}
		  },
		  "tables": {
			"%s": {
			   "column_vindexes": [
				{
				  "column": "id",
				  "name": "hash_index"
				}
			  ] 
			}
		  }
		}
`
	srcMaster  *cluster.Vttablet
	srcReplica *cluster.Vttablet
	srcRdonly  *cluster.Vttablet

	destMaster  *cluster.Vttablet
	destReplica *cluster.Vttablet
	destRdonly  *cluster.Vttablet
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		localCluster = cluster.NewCluster(cell, hostname)
		defer localCluster.Teardown()

		localCluster.Keyspaces = append(localCluster.Keyspaces, cluster.Keyspace{
			Name: keyspaceName,
		})

		// Start topo server
		if err := localCluster.StartTopo(); err != nil {
			return 1, err
		}

		srcMaster = localCluster.GetVttabletInstance("master", 0, "")
		srcReplica = localCluster.GetVttabletInstance("replica", 0, "")
		srcRdonly = localCluster.GetVttabletInstance("rdonly", 0, "")

		destMaster = localCluster.GetVttabletInstance("master", 0, "")
		destReplica = localCluster.GetVttabletInstance("replica", 0, "")
		destRdonly = localCluster.GetVttabletInstance("rdonly", 0, "")

		var mysqlProcs []*exec.Cmd
		for _, tablet := range []*cluster.Vttablet{srcMaster, srcReplica, srcRdonly, destMaster, destReplica, destRdonly} {
			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
			tablet.VttabletProcess = cluster.VttabletProcessInstance(tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				cell,
				"",
				keyspaceName,
				localCluster.VtctldProcess.Port,
				tablet.Type,
				localCluster.TopoPort,
				hostname,
				localCluster.TmpDirectory,
				commonTabletArg,
				true,
			)
			tablet.VttabletProcess.SupportsBackup = true
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				return 1, err
			}
			mysqlProcs = append(mysqlProcs, proc)
		}
		for _, proc := range mysqlProcs {
			if err := proc.Wait(); err != nil {
				return 1, err
			}
		}

		if err := localCluster.VtctlProcess.CreateKeyspace(keyspaceName); err != nil {
			return 1, err
		}

		shard1 := cluster.Shard{
			Name:      "0",
			Vttablets: []*cluster.Vttablet{srcMaster, srcReplica, srcRdonly},
		}
		for idx := range shard1.Vttablets {
			shard1.Vttablets[idx].VttabletProcess.Shard = shard1.Name
		}
		localCluster.Keyspaces[0].Shards = append(localCluster.Keyspaces[0].Shards, shard1)

		shard2 := cluster.Shard{
			Name:      "-",
			Vttablets: []*cluster.Vttablet{destMaster, destReplica, destRdonly},
		}
		for idx := range shard2.Vttablets {
			shard2.Vttablets[idx].VttabletProcess.Shard = shard2.Name
		}
		localCluster.Keyspaces[0].Shards = append(localCluster.Keyspaces[0].Shards, shard2)

		for _, tablet := range shard1.Vttablets {
			if err := localCluster.VtctlclientProcess.InitTablet(tablet, cell, keyspaceName, hostname, shard1.Name); err != nil {
				return 1, err
			}
			if err := tablet.VttabletProcess.Setup(); err != nil {
				return 1, err
			}
		}
		if err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shard1.Name, cell, srcMaster.TabletUID); err != nil {
			return 1, err
		}

		if err := localCluster.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(sqlSchema, tableName)); err != nil {
			return 1, err
		}
		if err := localCluster.VtctlclientProcess.ApplyVSchema(keyspaceName, fmt.Sprintf(vSchema, tableName)); err != nil {
			return 1, err
		}

		// run a health check on source replica so it responds to discovery
		// (for binlog players) and on the source rdonlys (for workers)
		for _, tablet := range []string{srcReplica.Alias, srcRdonly.Alias} {
			if err := localCluster.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet); err != nil {
				return 1, err
			}
		}

		// Create destination shard (won't be serving as there is no DB)
		for _, tablet := range shard2.Vttablets {
			if err := localCluster.VtctlclientProcess.InitTablet(tablet, cell, keyspaceName, hostname, shard2.Name); err != nil {
				return 1, err
			}
			if err := tablet.VttabletProcess.Setup(); err != nil {
				return 1, err
			}
		}

		if err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shard2.Name, cell, destMaster.TabletUID); err != nil {
			return 1, err
		}
		_ = localCluster.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
		// Copy schema
		if err := localCluster.VtctlclientProcess.ExecuteCommand("CopySchemaShard", srcReplica.Alias, fmt.Sprintf("%s/%s", keyspaceName, shard2.Name)); err != nil {
			return 1, err
		}

		// run the clone worker (this is a degenerate case, source and destination
		// both have the full keyrange. Happens to work correctly).
		localCluster.VtworkerProcess = *cluster.VtworkerProcessInstance(localCluster.GetAndReservePort(),
			localCluster.GetAndReservePort(),
			localCluster.TopoPort,
			localCluster.Hostname,
			localCluster.TmpDirectory)
		localCluster.VtworkerProcess.Cell = cell
		if err := localCluster.VtworkerProcess.ExecuteVtworkerCommand(localCluster.VtworkerProcess.Port,
			localCluster.VtworkerProcess.GrpcPort, "--use_v3_resharding_mode=true",
			"SplitClone",
			"--chunk_count", "10",
			"--min_rows_per_chunk", "1",
			"--exclude_tables", "unrelated",
			"--min_healthy_rdonly_tablets", "1",
			fmt.Sprintf("%s/%s", keyspaceName, shard1.Name)); err != nil {
			return 1, err
		}
		if err := destMaster.VttabletProcess.WaitForBinLogPlayerCount(1); err != nil {
			return 1, err
		}
		// Wait for dst_replica to be ready.
		if err := destReplica.VttabletProcess.WaitForBinlogServerState("Enabled"); err != nil {
			return 1, err
		}
		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

//  Insert something that will replicate incorrectly if the charset is not
//  propagated through binlog streamer to the destination.

//  Vitess tablets default to using utf8, so we insert something crazy and
//  pretend it's latin1. If the binlog player doesn't also pretend it's
//  latin1, it will be inserted as utf8, which will change its value.
func TestCharset(t *testing.T) {
	position, _ := cluster.GetMasterPosition(t, *destReplica, hostname)

	_, err := queryTablet(t, *srcMaster, fmt.Sprintf(insertSql, tableName, 1, "Šṛ́rỏé"), "latin1")
	assert.Nil(t, err)
	println("Waiting to get rows in dest master tablet")
	waitForReplicaEvent(t, position, "1", *destReplica)

	verifyData(t, 1, "latin1", `[UINT64(1) VARCHAR("Šṛ́rỏé")]`)
}

// Enable binlog_checksum, which will also force a log rotation that should
// cause binlog streamer to notice the new checksum setting.
func TestChecksumEnabled(t *testing.T) {
	position, _ := cluster.GetMasterPosition(t, *destReplica, hostname)
	_, err := queryTablet(t, *destReplica, "SET @@global.binlog_checksum=1", "")
	assert.Nil(t, err)

	// Insert something and make sure it comes through intact.
	_, err = queryTablet(t, *srcMaster, fmt.Sprintf(insertSql, tableName, 2, "value - 2"), "")
	assert.Nil(t, err)

	//  Look for it using update stream to see if binlog streamer can talk to
	//  dest_replica, which now has binlog_checksum enabled.
	waitForReplicaEvent(t, position, "2", *destReplica)

	verifyData(t, 2, "", `[UINT64(2) VARCHAR("value - 2")]`)
}

// Disable binlog_checksum to make sure we can also talk to a server without
// checksums enabled, in case they are enabled by default
func TestChecksumDisabled(t *testing.T) {
	position, _ := cluster.GetMasterPosition(t, *destReplica, hostname)

	_, err := queryTablet(t, *destReplica, "SET @@global.binlog_checksum=0", "")
	assert.Nil(t, err)

	// Insert something and make sure it comes through intact.
	_, err = queryTablet(t, *srcMaster, fmt.Sprintf(insertSql, tableName, 3, "value - 3"), "")
	assert.Nil(t, err)

	// Look for it using update stream to see if binlog streamer can talk to
	// dest_replica, which now has binlog_checksum disabled.
	waitForReplicaEvent(t, position, "3", *destReplica)

	verifyData(t, 3, "", `[UINT64(3) VARCHAR("value - 3")]`)
}

// Wait for a replica event with the given SQL string.
func waitForReplicaEvent(t *testing.T, position string, pkKey string, vttablet cluster.Vttablet) {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		println("fetching with position " + position)
		output, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletUpdateStream", "-position", position, "-count", "1", vttablet.Alias)
		assert.Nil(t, err)

		var binlogStreamEvent query.StreamEvent
		err = json.Unmarshal([]byte(output), &binlogStreamEvent)
		assert.Nil(t, err)
		for _, statement := range binlogStreamEvent.Statements {
			if isCurrentRowPresent(*statement, pkKey) {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
		position = binlogStreamEvent.EventToken.Position
	}
}

func isCurrentRowPresent(statement query.StreamEvent_Statement, pkKey string) bool {
	if statement.TableName == tableName &&
		statement.PrimaryKeyFields[0].Name == "id" &&
		fmt.Sprintf("%s", statement.PrimaryKeyValues[0].Values) == pkKey {
		return true
	}
	return false
}

func verifyData(t *testing.T, id uint64, charset string, expectedOutput string) {
	data, err := queryTablet(t, *destMaster, fmt.Sprintf("select id, msg from %s where id = %d", tableName, id), charset)
	assert.Nil(t, err)
	assert.NotNil(t, data.Rows)
	rowFound := assert.Equal(t, len(data.Rows), 1)
	assert.Equal(t, len(data.Fields), 2)
	if rowFound {
		assert.Equal(t, fmt.Sprintf("%v", data.Rows[0]), expectedOutput)
	}
}

func queryTablet(t *testing.T, vttablet cluster.Vttablet, query string, charset string) (*sqltypes.Result, error) {
	dbParams := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(vttablet.VttabletProcess.Directory, "mysql.sock"),

		DbName: fmt.Sprintf("vt_%s", keyspaceName),
	}
	if charset != "" {
		dbParams.Charset = charset
	}
	ctx := context.Background()
	dbConn, err := mysql.Connect(ctx, &dbParams)
	assert.Nil(t, err)
	defer dbConn.Close()
	return dbConn.ExecuteFetch(query, 1000, true)
}
