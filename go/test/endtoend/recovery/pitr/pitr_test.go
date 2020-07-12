package pitr

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	createTable = `create table product (id bigint(20) primary key AUTO_INCREMENT, name char(10), created bigint(20));`
	insertTable = `set time_zone='+00:00';insert into product (name, created) values('%s', unix_timestamp());`
)

func TestPointInTimeRecovery(t *testing.T) {
	_, err := masterTablet.VttabletProcess.QueryTablet(createTable, keyspaceName, true)
	require.NoError(t, err)
	println(fmt.Sprintf(insertTable, "p1"))
	_, err = masterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTable, "p1"), keyspaceName, true)

	bs, err := newBinlogServer(hostname, clusterInstance.GetAndReservePort())
	defer bs.stop()
	require.NoError(t, err)

	err = bs.start(mysqlMaster{
		hostname: "127.0.0.1",
		port:     masterTablet.MysqlctlProcess.MySQLPort,
		username: mysqlUserName,
	})
	fmt.Println("mysql -u vt_dba -h 127.0.0.1 -P", replicaTablet.MysqlctlProcess.MySQLPort, " vt_ks -e \"select * from product\" ")
	fmt.Println("mysql -h 127.0.0.1 -P ", bs.port, " -u ripple -e \"SELECT @@global.gtid_executed\"")
	time.Sleep(1 * time.Minute)

}
