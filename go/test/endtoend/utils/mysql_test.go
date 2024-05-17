/*
Copyright 2022 The Vitess Authors.

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

package utils

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/mysqlctl"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	mysqlParams     mysql.ConnParams
	mysqld          *mysqlctl.Mysqld
	mycnf           *mysqlctl.Mycnf
	keyspaceName    = "ks"
	cell            = "test"
	schemaSQL       = `create table t1(
		id1 bigint,
		id2 bigint,
		id3 bigint,
		primary key(id1)
	) Engine=InnoDB;`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		var closer func()
		var err error
		mysqlParams, mysqld, mycnf, closer, err = NewMySQLWithMysqld(clusterInstance.GetAndReservePort(), clusterInstance.Hostname, keyspaceName, schemaSQL)
		if err != nil {
			fmt.Println(err)
			return 1
		}
		defer closer()
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestCheckFields(t *testing.T) {
	createField := func(typ querypb.Type) *querypb.Field {
		return &querypb.Field{
			Type: typ,
		}
	}

	cases := []struct {
		fail    bool
		vtField querypb.Type
		myField querypb.Type
	}{
		{
			vtField: querypb.Type_INT32,
			myField: querypb.Type_INT32,
		},
		{
			vtField: querypb.Type_INT64,
			myField: querypb.Type_INT32,
		},
		{
			fail:    true,
			vtField: querypb.Type_FLOAT32,
			myField: querypb.Type_INT32,
		},
		{
			fail:    true,
			vtField: querypb.Type_TIMESTAMP,
			myField: querypb.Type_TUPLE,
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%s_%s", c.vtField.String(), c.myField.String()), func(t *testing.T) {
			tt := &testing.T{}
			checkFields(tt, "col", createField(c.vtField), createField(c.myField))
			require.Equal(t, c.fail, tt.Failed())
		})
	}
}

func TestCreateMySQL(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &mysqlParams)
	require.NoError(t, err)
	AssertMatches(t, conn, "show databases;", `[[VARCHAR("information_schema")] [VARCHAR("ks")] [VARCHAR("mysql")] [VARCHAR("performance_schema")] [VARCHAR("sys")]]`)
	AssertMatches(t, conn, "show tables;", `[[VARCHAR("t1")]]`)
	Exec(t, conn, "insert into t1(id1, id2, id3) values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	AssertMatches(t, conn, "select * from t1;", `[[INT64(1) INT64(1) INT64(1)] [INT64(2) INT64(2) INT64(2)] [INT64(3) INT64(3) INT64(3)]]`)
}

func TestSetSuperReadOnlyMySQL(t *testing.T) {
	require.NotNil(t, mysqld)
	isSuperReadOnly, _ := mysqld.IsSuperReadOnly(context.Background())
	assert.False(t, isSuperReadOnly, "super_read_only should be set to False")
	retFunc1, err := mysqld.SetSuperReadOnly(context.Background(), true)
	assert.NotNil(t, retFunc1, "SetSuperReadOnly is supposed to return a defer function")
	assert.NoError(t, err, "SetSuperReadOnly should not have failed")

	isSuperReadOnly, _ = mysqld.IsSuperReadOnly(context.Background())
	assert.True(t, isSuperReadOnly, "super_read_only should be set to True")
	// if value is already true then retFunc2 will be nil
	retFunc2, err := mysqld.SetSuperReadOnly(context.Background(), true)
	assert.Nil(t, retFunc2, "SetSuperReadOnly is supposed to return a nil function")
	assert.NoError(t, err, "SetSuperReadOnly should not have failed")

	retFunc1()
	isSuperReadOnly, _ = mysqld.IsSuperReadOnly(context.Background())
	assert.False(t, isSuperReadOnly, "super_read_only should be set to False")
	isReadOnly, _ := mysqld.IsReadOnly(context.Background())
	assert.True(t, isReadOnly, "read_only should be set to True")

	isSuperReadOnly, _ = mysqld.IsSuperReadOnly(context.Background())
	assert.False(t, isSuperReadOnly, "super_read_only should be set to False")
	retFunc1, err = mysqld.SetSuperReadOnly(context.Background(), false)
	assert.Nil(t, retFunc1, "SetSuperReadOnly is supposed to return a nil function")
	assert.NoError(t, err, "SetSuperReadOnly should not have failed")

	_, err = mysqld.SetSuperReadOnly(context.Background(), true)
	assert.NoError(t, err)

	isSuperReadOnly, _ = mysqld.IsSuperReadOnly(context.Background())
	assert.True(t, isSuperReadOnly, "super_read_only should be set to True")
	retFunc1, err = mysqld.SetSuperReadOnly(context.Background(), false)
	assert.NotNil(t, retFunc1, "SetSuperReadOnly is supposed to return a defer function")
	assert.NoError(t, err, "SetSuperReadOnly should not have failed")

	isSuperReadOnly, _ = mysqld.IsSuperReadOnly(context.Background())
	assert.False(t, isSuperReadOnly, "super_read_only should be set to False")
	// if value is already false then retFunc2 will be nil
	retFunc2, err = mysqld.SetSuperReadOnly(context.Background(), false)
	assert.Nil(t, retFunc2, "SetSuperReadOnly is supposed to return a nil function")
	assert.NoError(t, err, "SetSuperReadOnly should not have failed")

	retFunc1()
	isSuperReadOnly, _ = mysqld.IsSuperReadOnly(context.Background())
	assert.True(t, isSuperReadOnly, "super_read_only should be set to True")
	isReadOnly, _ = mysqld.IsReadOnly(context.Background())
	assert.True(t, isReadOnly, "read_only should be set to True")
}

func TestGetMysqlPort(t *testing.T) {
	require.NotNil(t, mysqld)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	port, err := mysqld.GetMysqlPort(ctx)

	// Expected port should be one less than the port returned by GetAndReservePort
	// As we are calling this second time to get port
	want := clusterInstance.GetAndReservePort() - 1
	assert.Equal(t, want, int(port))
	assert.NoError(t, err)
}

func TestGetServerID(t *testing.T) {
	require.NotNil(t, mysqld)

	sid, err := mysqld.GetServerID(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, mycnf.ServerID, sid)

	suuid, err := mysqld.GetServerUUID(context.Background())
	assert.NoError(t, err)
	assert.NotEmpty(t, suuid)
}

func TestReplicationStatus(t *testing.T) {
	require.NotNil(t, mysqld)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Initially we should expect an error for no replication status
	_, err := mysqld.ReplicationStatus(context.Background())
	assert.ErrorContains(t, err, "no replication status")

	conn, err := mysql.Connect(ctx, &mysqlParams)
	require.NoError(t, err)

	port, err := mysqld.GetMysqlPort(ctx)
	require.NoError(t, err)
	host := "localhost"

	q := conn.SetReplicationSourceCommand(&mysqlParams, host, port, 0, int(port))
	res := Exec(t, conn, q)
	require.NotNil(t, res)

	r, err := mysqld.ReplicationStatus(ctx)
	assert.NoError(t, err)
	assert.Equal(t, port, r.SourcePort)
	assert.Equal(t, host, r.SourceHost)
}

func TestPrimaryStatus(t *testing.T) {
	require.NotNil(t, mysqld)

	res, err := mysqld.PrimaryStatus(context.Background())
	assert.NoError(t, err)

	r, err := mysqld.ReplicationStatus(context.Background())
	assert.NoError(t, err)

	assert.True(t, res.Position.Equal(r.Position), "primary replication status should be same as replication status here")
}

func TestReplicationConfiguration(t *testing.T) {
	require.NotNil(t, mysqld)

	replConfig, err := mysqld.ReplicationConfiguration(context.Background())
	assert.NoError(t, err)

	require.NotNil(t, replConfig)
	// For a properly configured mysql, the heartbeat interval is half of the replication net timeout.
	require.EqualValues(t, math.Round(replConfig.HeartbeatInterval*2), replConfig.ReplicaNetTimeout)
}

func TestGTID(t *testing.T) {
	require.NotNil(t, mysqld)

	res, err := mysqld.GetGTIDPurged(context.Background())
	assert.Empty(t, res.String())
	assert.NoError(t, err)

	primaryPosition, err := mysqld.PrimaryPosition(context.Background())
	assert.NotNil(t, primaryPosition)
	assert.NoError(t, err)

	// Now we set gtid_purged for testing
	conn, err := mysql.Connect(context.Background(), &mysqlParams)
	require.NoError(t, err)

	gtid := "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8"
	r := Exec(t, conn, fmt.Sprintf("SET GLOBAL gtid_purged='%s'", gtid))
	require.NotNil(t, r)

	res, err = mysqld.GetGTIDPurged(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, gtid, res.String())

	primaryPosition, err = mysqld.PrimaryPosition(context.Background())
	assert.NoError(t, err)
	assert.Contains(t, primaryPosition.String(), gtid)
}

func TestSetReplicationPosition(t *testing.T) {
	require.NotNil(t, mysqld)

	pos := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	sid := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	pos.GTIDSet = pos.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 1})

	err := mysqld.SetReplicationPosition(context.Background(), pos)
	assert.NoError(t, err)

	want := "00010203-0405-0607-0809-0a0b0c0d0e0f:1"
	res, err := mysqld.GetGTIDPurged(context.Background())
	assert.NoError(t, err)
	assert.Contains(t, res.String(), want)
}

func TestSetAndResetReplication(t *testing.T) {
	require.NotNil(t, mysqld)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	port, err := mysqld.GetMysqlPort(ctx)
	require.NoError(t, err)
	host := "localhost"

	var heartbeatInterval float64 = 5.4
	err = mysqld.SetReplicationSource(context.Background(), host, port, heartbeatInterval, true, true)
	assert.NoError(t, err)

	r, err := mysqld.ReplicationStatus(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, port, r.SourcePort)
	assert.Equal(t, host, r.SourceHost)

	replConfig, err := mysqld.ReplicationConfiguration(context.Background())
	require.NoError(t, err)
	assert.EqualValues(t, heartbeatInterval, replConfig.HeartbeatInterval)

	err = mysqld.ResetReplication(context.Background())
	assert.NoError(t, err)

	r, err = mysqld.ReplicationStatus(context.Background())
	assert.ErrorContains(t, err, "no replication status")
	assert.Equal(t, "", r.SourceHost)
	assert.Equal(t, int32(0), r.SourcePort)

	err = mysqld.SetReplicationSource(context.Background(), host, port, 0, true, true)
	assert.NoError(t, err)

	r, err = mysqld.ReplicationStatus(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, port, r.SourcePort)
	assert.Equal(t, host, r.SourceHost)

	err = mysqld.ResetReplication(context.Background())
	assert.NoError(t, err)

	r, err = mysqld.ReplicationStatus(context.Background())
	assert.ErrorContains(t, err, "no replication status")
	assert.Equal(t, "", r.SourceHost)
	assert.Equal(t, int32(0), r.SourcePort)
}

func TestGetBinlogInformation(t *testing.T) {
	require.NotNil(t, mysqld)

	// Default values
	binlogFormat, logBin, logReplicaUpdates, binlogRowImage, err := mysqld.GetBinlogInformation(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "ROW", binlogFormat)
	assert.True(t, logBin)
	assert.True(t, logReplicaUpdates)
	assert.Equal(t, "FULL", binlogRowImage)

	conn, err := mysql.Connect(context.Background(), &mysqlParams)
	require.NoError(t, err)

	res := Exec(t, conn, "SET GLOBAL binlog_format = 'STATEMENT'")
	require.NotNil(t, res)

	res = Exec(t, conn, "SET GLOBAL binlog_row_image = 'MINIMAL'")
	require.NotNil(t, res)

	binlogFormat, logBin, logReplicaUpdates, binlogRowImage, err = mysqld.GetBinlogInformation(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "STATEMENT", binlogFormat)
	assert.True(t, logBin)
	assert.True(t, logReplicaUpdates)
	assert.Equal(t, "MINIMAL", binlogRowImage)

	// Set to default
	res = Exec(t, conn, "SET GLOBAL binlog_format = 'ROW'")
	require.NotNil(t, res)

	res = Exec(t, conn, "SET GLOBAL binlog_row_image = 'FULL'")
	require.NotNil(t, res)
}

func TestGetGTIDMode(t *testing.T) {
	require.NotNil(t, mysqld)

	// Default value
	ctx := context.Background()
	res, err := mysqld.GetGTIDMode(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "ON", res)

	conn, err := mysql.Connect(context.Background(), &mysqlParams)
	require.NoError(t, err)

	// Change value for the purpose of testing
	r := Exec(t, conn, "SET GLOBAL gtid_mode = 'ON_PERMISSIVE'")
	require.NotNil(t, r)

	res, err = mysqld.GetGTIDMode(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "ON_PERMISSIVE", res)

	// Back to default
	r = Exec(t, conn, "SET GLOBAL gtid_mode = 'ON'")
	require.NotNil(t, r)
}

func TestBinaryLogs(t *testing.T) {
	require.NotNil(t, mysqld)

	res, err := mysqld.GetBinaryLogs(context.Background())
	assert.NoError(t, err)
	oldNumLogs := len(res)

	err = mysqld.FlushBinaryLogs(context.Background())
	assert.NoError(t, err)

	res, err = mysqld.GetBinaryLogs(context.Background())
	assert.NoError(t, err)
	newNumLogs := len(res)
	assert.Equal(t, 1, newNumLogs-oldNumLogs, "binary logs should have been flushed once")
}

func TestGetPreviousGTIDs(t *testing.T) {
	require.NotNil(t, mysqld)

	res, err := mysqld.GetBinaryLogs(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, res)

	ctx := context.Background()
	r, err := mysqld.GetPreviousGTIDs(ctx, res[0])
	assert.NoError(t, err)
	assert.Empty(t, r)

	_, err = mysqld.GetPreviousGTIDs(ctx, "invalid_binlog_file")
	assert.ErrorContains(t, err, "Could not find target log")
}

func TestSemiSyncEnabled(t *testing.T) {
	require.NotNil(t, mysqld)

	err := mysqld.SetSemiSyncEnabled(context.Background(), true, false)
	assert.NoError(t, err)

	p, r := mysqld.SemiSyncEnabled(context.Background())
	assert.True(t, p)
	assert.False(t, r)

	err = mysqld.SetSemiSyncEnabled(context.Background(), false, true)
	assert.NoError(t, err)

	p, r = mysqld.SemiSyncEnabled(context.Background())
	assert.False(t, p)
	assert.True(t, r)
}

func TestWaitForReplicationStart(t *testing.T) {
	require.NotNil(t, mysqld)

	err := mysqlctl.WaitForReplicationStart(context.Background(), mysqld, 1)
	assert.ErrorContains(t, err, "no replication status")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	port, err := mysqld.GetMysqlPort(ctx)
	require.NoError(t, err)
	host := "localhost"

	err = mysqld.SetReplicationSource(context.Background(), host, port, 0, true, true)
	assert.NoError(t, err)

	err = mysqlctl.WaitForReplicationStart(context.Background(), mysqld, 1)
	assert.NoError(t, err)

	err = mysqld.ResetReplication(context.Background())
	require.NoError(t, err)
}

func TestStartReplication(t *testing.T) {
	require.NotNil(t, mysqld)

	err := mysqld.StartReplication(context.Background(), map[string]string{})
	assert.ErrorContains(t, err, "The server is not configured as replica")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	port, err := mysqld.GetMysqlPort(ctx)
	require.NoError(t, err)
	host := "localhost"

	// Set startReplicationAfter to false as we want to test StartReplication here
	err = mysqld.SetReplicationSource(context.Background(), host, port, 0, true, false)
	assert.NoError(t, err)

	err = mysqld.StartReplication(context.Background(), map[string]string{})
	assert.NoError(t, err)

	err = mysqld.ResetReplication(context.Background())
	require.NoError(t, err)
}

func TestStopReplication(t *testing.T) {
	require.NotNil(t, mysqld)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	port, err := mysqld.GetMysqlPort(ctx)
	require.NoError(t, err)
	host := "localhost"

	err = mysqld.SetReplicationSource(context.Background(), host, port, 0, true, true)
	assert.NoError(t, err)

	r, err := mysqld.ReplicationStatus(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, host, r.SourceHost)
	assert.Equal(t, port, r.SourcePort)
	assert.Equal(t, replication.ReplicationStateRunning, r.SQLState)

	err = mysqld.StopReplication(context.Background(), map[string]string{})
	assert.NoError(t, err)

	r, err = mysqld.ReplicationStatus(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, replication.ReplicationStateStopped, r.SQLState)
}

func TestStopSQLThread(t *testing.T) {
	require.NotNil(t, mysqld)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	port, err := mysqld.GetMysqlPort(ctx)
	require.NoError(t, err)
	host := "localhost"

	err = mysqld.SetReplicationSource(context.Background(), host, port, 0, true, true)
	assert.NoError(t, err)

	r, err := mysqld.ReplicationStatus(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, host, r.SourceHost)
	assert.Equal(t, port, r.SourcePort)
	assert.Equal(t, replication.ReplicationStateRunning, r.SQLState)

	err = mysqld.StopSQLThread(context.Background())
	assert.NoError(t, err)

	r, err = mysqld.ReplicationStatus(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, replication.ReplicationStateStopped, r.SQLState)
}
