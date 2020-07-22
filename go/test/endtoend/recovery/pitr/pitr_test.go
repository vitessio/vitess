package pitr

import (
	"fmt"
	"testing"
	"time"

	"github.com/magiconair/properties/assert"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/require"
)

var (
	createTable = `create table product (id bigint(20) primary key, name char(10), created bigint(20));`
	insertTable = `insert into product (id, name, created) values(%d, '%s', unix_timestamp());`
	selectMaxID = `select max(id) from product`
)

func TestPointInTimeRecovery(t *testing.T) {
	// create table and insert 2 rows
	_, err := masterTablet.VttabletProcess.QueryTablet(createTable, keyspaceName, true)
	require.NoError(t, err)
	insertRow(t, 1, "p1", false)
	insertRow(t, 2, "p2", false)

	// wait till replica catchup
	cluster.VerifyRowsInTabletForTable(t, replicaTablet, keyspaceName, 2, "product")

	//start the binlog server and point it to master
	bs, err := newBinlogServer(hostname, clusterInstance.GetAndReservePort())
	defer bs.stop()
	require.NoError(t, err)

	err = bs.start(mysqlMaster{
		hostname: binlogHost,
		port:     masterTablet.MysqlctlProcess.MySQLPort,
		username: mysqlUserName,
	})
	require.NoError(t, err)

	// take the backup (to simulate the regular backup)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Backup", replicaTablet.Alias)
	require.NoError(t, err)

	backups, err := clusterInstance.ListBackups(shardKsName)
	require.NoError(t, err)
	require.Equal(t, len(backups), 1)

	// now insert some more data to simulate the changes after regular backup
	// every insert has some time lag/difference to simulate the time gap between rows
	// and when we recover to certain time, this time gap will be able to identify the exact eligible row
	var timeToRecover string
	for counter := 3; counter <= 7; counter++ {
		if counter == 5 { // we want to recovery till this, so noting the time
			tm := time.Now().Add(1 * time.Second).UTC()
			timeToRecover = tm.Format(time.RFC3339)
		}
		insertRow(t, counter, fmt.Sprintf("prd-%d", counter), true)
	}
	currentTime := time.Now().UTC()
	timeForCompleteBinlogRecover := currentTime.Format(time.RFC3339)
	// create restoreSnapshot
	createRestoreSnapshot(t, timeToRecover, partialRestoreKSName)

	// test the recovery with smaller binlog_lookup_timeout
	recoveryTabletWithSmallTimeout := clusterInstance.NewVttabletInstance("replica", 0, cell)
	launchRecoveryTablet(t, recoveryTabletWithSmallTimeout, bs, "1ms", partialRestoreKSName)

	// since we have smaller timeout, it will just get whatever available in the backup
	sqlRes, err := recoveryTabletWithSmallTimeout.VttabletProcess.QueryTablet(selectMaxID, keyspaceName, true)
	require.NoError(t, err)
	assert.Equal(t, sqlRes.Rows[0][0].String(), "INT64(2)")

	// test the recovery with valid binlog_lookup_timeout and timeToRecover pointing to 5th row
	recoveryTablet1 := clusterInstance.NewVttabletInstance("replica", 0, cell)
	launchRecoveryTablet(t, recoveryTablet1, bs, "2m", partialRestoreKSName)

	sqlRes, err = recoveryTablet1.VttabletProcess.QueryTablet(selectMaxID, keyspaceName, true)
	require.NoError(t, err)
	assert.Equal(t, sqlRes.Rows[0][0].String(), "INT64(5)")

	// test the recovery with timetorecover > (timestmap of last binlog event in binlog server)
	createRestoreSnapshot(t, timeForCompleteBinlogRecover, fullRestoreKSName)

	recoveryTablet2 := clusterInstance.NewVttabletInstance("replica", 0, cell)
	launchRecoveryTablet(t, recoveryTablet2, bs, "2m", fullRestoreKSName)

	sqlRes, err = recoveryTablet2.VttabletProcess.QueryTablet(selectMaxID, keyspaceName, true)
	require.NoError(t, err)
	assert.Equal(t, sqlRes.Rows[0][0].String(), "INT64(7)")

	defer recoveryTablet1.MysqlctlProcess.Stop()
	defer recoveryTablet1.VttabletProcess.TearDown()

	defer recoveryTablet2.MysqlctlProcess.Stop()
	defer recoveryTablet2.VttabletProcess.TearDown()

	defer recoveryTabletWithSmallTimeout.MysqlctlProcess.Stop()
	defer recoveryTabletWithSmallTimeout.VttabletProcess.TearDown()
}

func insertRow(t *testing.T, id int, productName string, isSlow bool) {
	_, err := masterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTable, id, productName), keyspaceName, true)
	require.NoError(t, err)
	if isSlow {
		time.Sleep(1 * time.Second)
	}
}

func createRestoreSnapshot(t *testing.T, timeToRecover, restoreKSName string) {
	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("CreateKeyspace",
		"-keyspace_type=SNAPSHOT", "-base_keyspace="+keyspaceName,
		"-snapshot_time", timeToRecover, restoreKSName)
	log.Info(output)
	require.Nil(t, err)
}

func launchRecoveryTablet(t *testing.T, tablet *cluster.Vttablet, binlogServer *binLogServer, lookupTimeout, restoreKSName string) {
	tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
	tablet.MysqlctlProcess.InitDBFile = initDBFile
	err := tablet.MysqlctlProcess.Start()
	require.NoError(t, err)

	tablet.VttabletProcess = cluster.VttabletProcessInstance(tablet.HTTPPort,
		tablet.GrpcPort,
		tablet.TabletUID,
		clusterInstance.Cell,
		shardName,
		keyspaceName,
		clusterInstance.VtctldProcess.Port,
		tablet.Type,
		clusterInstance.TopoProcess.Port,
		clusterInstance.Hostname,
		clusterInstance.TmpDirectory,
		clusterInstance.VtTabletExtraArgs,
		clusterInstance.EnableSemiSync)
	tablet.Alias = tablet.VttabletProcess.TabletPath
	tablet.VttabletProcess.SupportsBackup = true
	tablet.VttabletProcess.Keyspace = restoreKSName
	tablet.VttabletProcess.EnableSemiSync = true
	tablet.VttabletProcess.ExtraArgs = []string{
		"-disable_active_reparents",
		"-enable_replication_reporter=false",
		"-init_db_name_override", dbName,
		"-init_tablet_type", "replica",
		"-init_keyspace", restoreKSName,
		"-init_shard", shardName,
		"-binlog_host", binlogServer.hostname,
		"-binlog_port", fmt.Sprintf("%d", binlogServer.port),
		"-binlog_user", binlogServer.username,
		"-pitr_gtid_lookup_timeout", lookupTimeout,
		"-vreplication_healthcheck_topology_refresh", "1s",
		"-vreplication_healthcheck_retry_delay", "1s",
		"-vreplication_tablet_type", "replica",
		"-vreplication_retry_delay", "1s",
		"-degraded_threshold", "5s",
		"-lock_tables_timeout", "5s",
		"-watch_replication_stream",
		"-serving_state_grace_period", "1s",
	}
	tablet.VttabletProcess.ServingStatus = ""

	err = tablet.VttabletProcess.Setup()
	require.NoError(t, err)

	tablet.VttabletProcess.WaitForTabletTypesForTimeout([]string{"SERVING"}, 20*time.Second)
	require.Nil(t, err)
}
