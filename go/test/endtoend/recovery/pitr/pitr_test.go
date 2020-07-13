package pitr

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/magiconair/properties/assert"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/require"
)

var (
	createTable       = `create table product (id bigint(20) primary key, name char(10), created bigint(20));`
	insertTable       = `set time_zone='+00:00';insert into product (id, name, created) values(%d, '%s', unix_timestamp());`
	slowInsert        = `select sleep(%d);set time_zone='+00:00';insert into product (id, name, created) values(%d, '%s', unix_timestamp());`
	selectRecoverTime = `select created from product where id = %d`
	selectMaxID       = `select max(id) from product`
)

func TestPointInTimeRecovery(t *testing.T) {
	// create table and insert 2 rows
	_, err := masterTablet.VttabletProcess.QueryTablet(createTable, keyspaceName, true)
	require.NoError(t, err)
	_, err = masterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTable, 1, "p1"), keyspaceName, true)
	require.NoError(t, err)
	_, err = masterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTable, 2, "p2"), keyspaceName, true)
	require.NoError(t, err)

	//start the binlog server and point it to master
	bs, err := newBinlogServer(hostname, clusterInstance.GetAndReservePort())
	defer bs.stop()
	require.NoError(t, err)

	err = bs.start(mysqlMaster{
		hostname: "127.0.0.1",
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
	_, err = masterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(slowInsert, 3, 3, "p3"), keyspaceName, true)
	require.NoError(t, err)
	_, err = masterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(slowInsert, 4, 4, "p4"), keyspaceName, true)
	require.NoError(t, err)
	_, err = masterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(slowInsert, 5, 5, "p5"), keyspaceName, true)
	require.NoError(t, err)
	_, err = masterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(slowInsert, 6, 6, "p6"), keyspaceName, true)
	require.NoError(t, err)
	_, err = masterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(slowInsert, 7, 7, "p7"), keyspaceName, true)
	require.NoError(t, err)

	// fetch the time we want to recover to
	timeToRecover := getRecoveryTimeInUTC(t, 5)

	// start the recovery
	recoveryTablet := clusterInstance.NewVttabletInstance("replica", 0, cell)
	launchRecoveryTablet(t, recoveryTablet, bs, timeToRecover)

	sqlRes, err := recoveryTablet.VttabletProcess.QueryTablet(selectMaxID, keyspaceName, true)
	require.NoError(t, err)

	fmt.Println(sqlRes.Rows[0][0].String())
	assert.Equal(t, sqlRes.Rows[0][0].String(), "INT64(2)")
	defer recoveryTablet.MysqlctlProcess.Stop()
	defer recoveryTablet.VttabletProcess.TearDown()
}

func getRecoveryTimeInUTC(t *testing.T, rowNum int) string {
	sqlRes, err := masterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(selectRecoverTime, rowNum), keyspaceName, true)
	require.NoError(t, err)
	epochTime, err := strconv.ParseInt(string(sqlRes.Rows[0][0].ToBytes()), 10, 64)
	require.NoError(t, err)
	timeToRecover := time.Unix(epochTime, 0)
	timeToRecover = timeToRecover.Add(1 * time.Second)
	loc, err := time.LoadLocation("UTC")
	require.NoError(t, err)
	return timeToRecover.In(loc).Format(time.RFC3339)
}

func launchRecoveryTablet(t *testing.T, tablet *cluster.Vttablet, binlogServer *binLogServer, timeToRecover string) {
	tm := time.Now().UTC()
	fmt.Println(tm.Format(time.RFC3339), timeToRecover)
	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("CreateKeyspace",
		"-keyspace_type=SNAPSHOT", "-base_keyspace="+keyspaceName,
		"-snapshot_time", tm.Format(time.RFC3339), restoreKSName)
	log.Info(output)
	require.Nil(t, err)

	tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
	tablet.MysqlctlProcess.InitDBFile = initDBFile
	err = tablet.MysqlctlProcess.Start()
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
		"-binlog_timeout", "2m",
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
	//require.Nil(t, err)
}
