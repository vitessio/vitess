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

package encryption

import (
	"flag"
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	keyspace        = "test_keyspace"
	hostname        = "localhost"
	shardName       = "0"
	cell            = "zone1"
	certDirectory   string
)

func TestSecure(t *testing.T) {
	testReplicationBase(t, true)
	testReplicationBase(t, false)
}

// This test makes sure that we can use SSL replication with Vitess.
func testReplicationBase(t *testing.T, isClientCertPassed bool) {
	flag.Parse()

	// initialize cluster
	code, err := initializeCluster(t)
	if err != nil {
		t.Errorf("setup failed with status code %d", code)
	}
	defer teardownCluster()

	masterTablet := *clusterInstance.Keyspaces[0].Shards[0].Vttablets[0]
	replicaTablet := *clusterInstance.Keyspaces[0].Shards[0].Vttablets[1]

	err = clusterInstance.VtctlclientProcess.InitTablet(&masterTablet, clusterInstance.Cell, keyspace, hostname, shardName)
	assert.Nil(t, err)
	err = masterTablet.VttabletProcess.CreateDB(keyspace)
	assert.Nil(t, err)

	if isClientCertPassed {
		replicaTablet.VttabletProcess.ExtraArgs = append(replicaTablet.VttabletProcess.ExtraArgs, "-db_flags", "2048",
			"-db_ssl_ca", path.Join(certDirectory, "ca-cert.pem"),
			"-db_ssl_cert", path.Join(certDirectory, "client-cert.pem"),
			"-db_ssl_key", path.Join(certDirectory, "client-key.pem"),
		)
	}
	err = clusterInstance.VtctlclientProcess.InitTablet(&replicaTablet, clusterInstance.Cell, keyspace, hostname, shardName)
	assert.Nil(t, err)
	err = replicaTablet.VttabletProcess.CreateDB(keyspace)
	assert.Nil(t, err)

	for _, tablet := range []cluster.Vttablet{masterTablet, replicaTablet} {
		_ = tablet.VttabletProcess.Setup()
	}

	err = clusterInstance.VtctlclientProcess.InitShardMaster(keyspace, shardName, clusterInstance.Cell, masterTablet.TabletUID)
	if isClientCertPassed {
		assert.Nil(t, err)
	} else {
		assert.NotNil(t, err)
	}
}

func initializeCluster(t *testing.T) (int, error) {
	var mysqlProcesses []*exec.Cmd
	clusterInstance = cluster.NewCluster(cell, hostname)

	// Start topo server
	if err := clusterInstance.StartTopo(); err != nil {
		return 1, err
	}

	// create certs directory
	t.Log("Creating certificates")
	certDirectory = path.Join(clusterInstance.TmpDirectory, "certs")
	_ = createDirectory(certDirectory, 0700)

	err := executeVttlstestCommand("-root", certDirectory, "CreateCA")
	assert.Nil(t, err)

	err = executeVttlstestCommand("-root", certDirectory, "CreateSignedCert", "-common_name", "Mysql Server", "-serial", "01", "server")
	assert.Nil(t, err)

	err = executeVttlstestCommand("-root", certDirectory, "CreateSignedCert", "-common_name", "Mysql Client", "-serial", "02", "client")
	assert.Nil(t, err)

	extraMyCnf := path.Join(certDirectory, "secure.cnf")
	f, err := os.Create(extraMyCnf)
	assert.Nil(t, err)

	_, err = f.WriteString("require_secure_transport=" + "true\n")
	assert.Nil(t, err)
	_, err = f.WriteString("ssl-ca=" + certDirectory + "/ca-cert.pem\n")
	assert.Nil(t, err)
	_, err = f.WriteString("ssl-cert=" + certDirectory + "/server-cert.pem\n")
	assert.Nil(t, err)
	_, err = f.WriteString("ssl-key=" + certDirectory + "/server-key.pem\n")
	assert.Nil(t, err)

	err = f.Close()
	assert.Nil(t, err)

	err = os.Setenv("EXTRA_MY_CNF", extraMyCnf)

	assert.Nil(t, err)

	for _, keyspaceStr := range []string{keyspace} {
		KeyspacePtr := &cluster.Keyspace{Name: keyspaceStr}
		keyspace := *KeyspacePtr
		if err := clusterInstance.VtctlProcess.CreateKeyspace(keyspace.Name); err != nil {
			return 1, err
		}
		shard := &cluster.Shard{
			Name: shardName,
		}
		for i := 0; i < 2; i++ {
			// instantiate vttablet object with reserved ports
			tabletUID := clusterInstance.GetAndReserveTabletUID()
			tablet := clusterInstance.GetVttabletInstance("replica", tabletUID, cell)

			// Start Mysqlctl process
			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
			if proc, err := tablet.MysqlctlProcess.StartProcess(); err != nil {
				return 1, err
			} else {
				mysqlProcesses = append(mysqlProcesses, proc)
			}
			// start vttablet process
			tablet.VttabletProcess = cluster.VttabletProcessInstance(tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				clusterInstance.Cell,
				shardName,
				keyspace.Name,
				clusterInstance.VtctldProcess.Port,
				tablet.Type,
				clusterInstance.TopoProcess.Port,
				clusterInstance.Hostname,
				clusterInstance.TmpDirectory,
				clusterInstance.VtTabletExtraArgs,
				clusterInstance.EnableSemiSync)
			tablet.Alias = tablet.VttabletProcess.TabletPath
			shard.Vttablets = append(shard.Vttablets, tablet)
		}
		keyspace.Shards = append(keyspace.Shards, *shard)
		clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, keyspace)
	}
	for _, proc := range mysqlProcesses {
		err := proc.Wait()
		if err != nil {
			return 1, err
		}
	}
	return 0, nil
}

func teardownCluster() {
	clusterInstance.Teardown()
}

func createDirectory(dirName string, mode os.FileMode) error {
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		return os.Mkdir(dirName, mode)
	}
	return nil
}

func executeVttlstestCommand(args ...string) error {
	tmpProcess := exec.Command(
		"vttlstest",
		args...,
	)
	return tmpProcess.Run()
}
