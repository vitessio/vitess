package pitr

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	masterTablet    *cluster.Vttablet
	replicaTablet   *cluster.Vttablet
	initDBFile      string

	cell                 = "zone1"
	hostname             = "localhost"
	binlogHost           = "127.0.0.1"
	keyspaceName         = "ks"
	partialRestoreKSName = "restoreks1"
	fullRestoreKSName    = "restoreks2"
	dbName               = "vt_ks"
	shardName            = "0"
	shardKsName          = "ks/0"
	mysqlUserName        = "vt_dba"
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

		if err := clusterInstance.VtctlProcess.CreateKeyspace(keyspaceName); err != nil {
			log.Error(err)
			return 1, err
		}
		clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, cluster.Keyspace{Name: keyspaceName})

		masterTablet = clusterInstance.NewVttabletInstance("replica", 0, cell)
		replicaTablet = clusterInstance.NewVttabletInstance("replica", 0, cell)

		err := startTablets([]*cluster.Vttablet{masterTablet, replicaTablet})
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

func startTablets(tablets []*cluster.Vttablet) error {
	var mysqlProcesses []*exec.Cmd
	shard := &cluster.Shard{
		Name: shardName,
	}
	for _, tablet := range tablets {
		tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
		tablet.MysqlctlProcess.InitDBFile = initDBFile
		if proc, err := tablet.MysqlctlProcess.StartProcess(); err != nil {
			return err
		} else {
			// ignore golint warning, we need the else block to use proc
			mysqlProcesses = append(mysqlProcesses, proc) //nolint
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
	queryCmds := []string{
		fmt.Sprintf("CREATE USER '%s'@'%%';", mysqlUserName),
		fmt.Sprintf("GRANT ALL ON *.* TO '%s'@'%%';", mysqlUserName),
		fmt.Sprintf("GRANT GRANT OPTION ON *.* TO '%s'@'%%';", mysqlUserName),
		"FLUSH PRIVILEGES;",
		fmt.Sprintf("create database %s;", dbName),
	}
	for _, tablet := range tablets {
		for _, query := range queryCmds {
			_, err := tablet.VttabletProcess.QueryTablet(query, keyspaceName, false)
			if err != nil {
				log.Error(err)
				return err
			}
		}
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
