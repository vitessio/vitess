package pitr

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	masterTablet    *cluster.Vttablet
	replicaTablet   *cluster.Vttablet
	initDBFile      string

	cell          = "zone1"
	hostname      = "localhost"
	keyspaceName  = "ks"
	restoreKSName = "resoreks"
	dbName        = "vt_ks"
	shardName     = "0"
	shardKsName   = "ks/0"
	mysqlUserName = "vt_dba"
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			log.Error(err)
			return 1, err
		}
		initDBFile = updateDBInitFile()
		if err := clusterInstance.VtctlProcess.CreateKeyspace(keyspaceName); err != nil {
			log.Error(err)
			return 1, err
		}
		clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, cluster.Keyspace{Name: keyspaceName})

		masterTablet = clusterInstance.NewVttabletInstance("replica", 0, cell)
		replicaTablet = clusterInstance.NewVttabletInstance("replica", 0, cell)

		err := startTablets([]*cluster.Vttablet{masterTablet, replicaTablet}, initDBFile)
		if err != nil {
			return 1, err
		}
		err = clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, masterTablet.TabletUID)
		if err != nil {
			return 1, err
		}
		return m.Run(), nil
	}()
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	os.Exit(exitCode)

}

// crates a vt_dba user to login to the mysql without password
func updateDBInitFile() string {
	initDb, _ := ioutil.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"))
	sql := string(initDb)
	newInitDBFile := path.Join(clusterInstance.TmpDirectory, "init_db_custom.sql")
	sql = sql + fmt.Sprintf(`
CREATE USER '%s'@'%%' ;
GRANT ALL ON *.* TO '%s'@'%%';
GRANT GRANT OPTION ON *.* TO '%s'@'%%';
FLUSH PRIVILEGES;
create database %s;
`, mysqlUserName, mysqlUserName, mysqlUserName, dbName)
	ioutil.WriteFile(newInitDBFile, []byte(sql), 0666)
	return newInitDBFile
}

func startTablets(tablets []*cluster.Vttablet, initDBFile string) error {
	var mysqlProcesses []*exec.Cmd
	shard := &cluster.Shard{
		Name: shardName,
	}
	for _, tablet := range tablets {
		//tablet.MysqlctlProcess = cluster.MysqlCtlProcessInstance()
		tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
		tablet.MysqlctlProcess.InitDBFile = initDBFile
		if proc, err := tablet.MysqlctlProcess.StartProcess(); err != nil {
			return err
		} else {
			// ignore golint warning, we need the else block to use proc
			mysqlProcesses = append(mysqlProcesses, proc)
		}
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
		shard.Vttablets = append(shard.Vttablets, tablet)
		shard.Vttablets = append(shard.Vttablets, tablet)
	}
	for _, proc := range mysqlProcesses {
		proc.Wait()
	}
	for _, tablet := range tablets {
		err := tablet.VttabletProcess.Setup()
		if err != nil {
			log.Error(err)
			return err
		}
	}
	clusterInstance.Keyspaces[0].Shards = []cluster.Shard{*shard}
	return nil
}
