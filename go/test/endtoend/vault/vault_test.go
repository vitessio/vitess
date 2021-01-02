/*
Copyright 2020 The Vitess Authors.

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

package vault

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

var (
	createTable = `create table product (id bigint(20) primary key, name char(10), created bigint(20));`
	insertTable = `insert into product (id, name, created) values(%d, '%s', unix_timestamp());`
)

var (
	clusterInstance *cluster.LocalProcessCluster

	master  *cluster.Vttablet
	replica *cluster.Vttablet

	cell            = "zone1"
	hostname        = "localhost"
	keyspaceName    = "ks"
	shardName       = "0"
	dbName          = "vt_ks"
	mysqlUsers      = []string{"vt_dba", "vt_app", "vt_appdebug", "vt_repl", "vt_filtered"}
	mysqlPassword   = "password"
	vtgateUser      = "vtgate_user"
	vtgatePassword  = "password123"
	commonTabletArg = []string{
		"-vreplication_healthcheck_topology_refresh", "1s",
		"-vreplication_healthcheck_retry_delay", "1s",
		"-vreplication_retry_delay", "1s",
		"-degraded_threshold", "5s",
		"-lock_tables_timeout", "5s",
		"-watch_replication_stream",
		// Frequently reload schema, generating some tablet traffic,
		//   so we can speed up token refresh
		"-queryserver-config-schema-reload-time", "5",
		"-serving_state_grace_period", "1s"}
	vaultTabletArg = []string{
		"-db-credentials-server", "vault",
		"-db-credentials-vault-timeout", "3s",
		"-db-credentials-vault-path", "kv/prod/dbcreds",
		// This is overriden by our env VAULT_ADDR
		"-db-credentials-vault-addr", "https://127.0.0.1:8200",
		// This is overriden by our env VAULT_CACERT
		"-db-credentials-vault-tls-ca", "/path/to/ca.pem",
		// This is provided by our env VAULT_ROLEID
		//"-db-credentials-vault-roleid", "34644576-9ffc-8bb5-d046-4a0e41194e15",
		// Contents of this file provided by our env VAULT_SECRETID
		//"-db-credentials-vault-secretidfile", "/path/to/file/containing/secret_id",
		// Make this small, so we can get a renewal
		"-db-credentials-vault-ttl", "21s"}
	vaultVTGateArg = []string{
		"-mysql_auth_server_impl", "vault",
		"-mysql_auth_vault_timeout", "3s",
		"-mysql_auth_vault_path", "kv/prod/vtgatecreds",
		// This is overriden by our env VAULT_ADDR
		"-mysql_auth_vault_addr", "https://127.0.0.1:8200",
		// This is overriden by our env VAULT_CACERT
		"-mysql_auth_vault_tls_ca", "/path/to/ca.pem",
		// This is provided by our env VAULT_ROLEID
		//"-mysql_auth_vault_roleid", "34644576-9ffc-8bb5-d046-4a0e41194e15",
		// Contents of this file provided by our env VAULT_SECRETID
		//"-mysql_auth_vault_role_secretidfile", "/path/to/file/containing/secret_id",
		// Make this small, so we can get a renewal
		"-mysql_auth_vault_ttl", "21s"}
	mysqlctlArg = []string{
		"-db_dba_password", mysqlPassword}
	vttabletLogFileName = "vttablet.INFO"
	tokenRenewalString  = "Vault client status: token renewed"
)

func TestVaultAuth(t *testing.T) {
	defer cluster.PanicHandler(nil)

	// Instantiate Vitess Cluster objects and start topo
	initializeClusterEarly(t)
	defer clusterInstance.Teardown()

	// start Vault server
	vs := startVaultServer(t, master)
	defer vs.stop()

	// Wait for Vault server to come up
	for i := 0; i < 60; i++ {
		time.Sleep(250 * time.Millisecond)
		ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", hostname, vs.port1))
		if err != nil {
			// Vault is now up, we can continue
			break
		}
		ln.Close()
	}

	roleID, secretID := setupVaultServer(t, vs)
	require.NotEmpty(t, roleID)
	require.NotEmpty(t, secretID)

	// Passing via environment, easier than trying to modify
	// vtgate/vttablet flags within our test machinery
	os.Setenv("VAULT_ROLEID", roleID)
	os.Setenv("VAULT_SECRETID", secretID)

	// Bring up rest of the Vitess cluster
	initializeClusterLate(t)

	// Create a table
	_, err := master.VttabletProcess.QueryTablet(createTable, keyspaceName, true)
	require.NoError(t, err)

	// This tests the vtgate Vault auth & indirectly vttablet Vault auth too
	insertRow(t, 1, "prd-1")
	insertRow(t, 2, "prd-2")

	cluster.VerifyRowsInTabletForTable(t, replica, keyspaceName, 2, "product")

	// Sleep for a while; giving enough time for a token renewal
	//   and it making it into the (asynchronous) log
	time.Sleep(30 * time.Second)
	// Check the log for the Vault token renewal message
	//   If we don't see it, that is a test failure
	logContents, _ := ioutil.ReadFile(path.Join(clusterInstance.TmpDirectory, vttabletLogFileName))
	require.True(t, bytes.Contains(logContents, []byte(tokenRenewalString)))
}

func startVaultServer(t *testing.T, masterTablet *cluster.Vttablet) *VaultServer {
	vs := &VaultServer{
		address: hostname,
		port1:   clusterInstance.GetAndReservePort(),
		port2:   clusterInstance.GetAndReservePort(),
	}
	err := vs.start()
	require.NoError(t, err)

	return vs
}

// Setup everything we need in the Vault server
func setupVaultServer(t *testing.T, vs *VaultServer) (string, string) {
	// The setup script uses these environment variables
	//   We also reuse VAULT_ADDR and VAULT_CACERT later on
	os.Setenv("VAULT", vs.execPath)
	os.Setenv("VAULT_ADDR", fmt.Sprintf("https://%s:%d", vs.address, vs.port1))
	os.Setenv("VAULT_CACERT", path.Join(os.Getenv("PWD"), vaultCAFileName))
	setup := exec.Command(
		"/bin/bash",
		path.Join(os.Getenv("PWD"), vaultSetupScript),
	)

	logFilePath := path.Join(vs.logDir, "log_setup.txt")
	logFile, _ := os.Create(logFilePath)
	setup.Stderr = logFile
	setup.Stdout = logFile

	setup.Env = append(setup.Env, os.Environ()...)
	log.Infof("Running Vault setup command: %v", strings.Join(setup.Args, " "))
	err := setup.Start()
	if err != nil {
		log.Errorf("Error during Vault setup: %v", err)
	}

	setup.Wait()
	var secretID, roleID string
	file, err := os.Open(logFilePath)
	if err != nil {
		log.Error(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "ROLE_ID=") {
			roleID = strings.Split(scanner.Text(), "=")[1]
		} else if strings.HasPrefix(scanner.Text(), "SECRET_ID=") {
			secretID = strings.Split(scanner.Text(), "=")[1]
		}
	}
	if err := scanner.Err(); err != nil {
		log.Error(err)
	}

	return roleID, secretID
}

// Setup cluster object and start topo
//   We need this before vault, because we re-use the port reservation code
func initializeClusterEarly(t *testing.T) {
	clusterInstance = cluster.NewCluster(cell, hostname)

	// Start topo server
	err := clusterInstance.StartTopo()
	require.NoError(t, err)
}

func initializeClusterLate(t *testing.T) {
	// Start keyspace
	keyspace := &cluster.Keyspace{
		Name: keyspaceName,
	}
	clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, *keyspace)
	shard := &cluster.Shard{
		Name: shardName,
	}

	master = clusterInstance.NewVttabletInstance("replica", 0, "")
	// We don't really need the replica to test this feature
	//   but keeping it in to excercise the vt_repl user/password path
	replica = clusterInstance.NewVttabletInstance("replica", 0, "")

	shard.Vttablets = []*cluster.Vttablet{master, replica}

	clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, commonTabletArg...)
	clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, vaultTabletArg...)
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, vaultVTGateArg...)

	err := clusterInstance.SetupCluster(keyspace, []cluster.Shard{*shard})
	require.NoError(t, err)

	// Start MySQL
	var mysqlCtlProcessList []*exec.Cmd
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			proc, err := tablet.MysqlctlProcess.StartProcess()
			require.NoError(t, err)
			mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
		}
	}

	// Wait for MySQL startup
	for _, proc := range mysqlCtlProcessList {
		err = proc.Wait()
		require.NoError(t, err)
	}

	for _, tablet := range []*cluster.Vttablet{master, replica} {
		for _, user := range mysqlUsers {
			query := fmt.Sprintf("ALTER USER '%s'@'%s' IDENTIFIED BY '%s';", user, hostname, mysqlPassword)
			_, err = tablet.VttabletProcess.QueryTablet(query, keyspace.Name, false)
			// Reset after the first ALTER, or we lock ourselves out.
			tablet.VttabletProcess.DbPassword = mysqlPassword
			if err != nil {
				query = fmt.Sprintf("ALTER USER '%s'@'%%' IDENTIFIED BY '%s';", user, mysqlPassword)
				_, err = tablet.VttabletProcess.QueryTablet(query, keyspace.Name, false)
				require.NoError(t, err)
			}
		}
		query := fmt.Sprintf("create database %s;", dbName)
		_, err = tablet.VttabletProcess.QueryTablet(query, keyspace.Name, false)
		require.NoError(t, err)

		tablet.VttabletProcess.EnableSemiSync = true
		err = tablet.VttabletProcess.Setup()
		require.NoError(t, err)

		// Modify mysqlctl password too, or teardown will be locked out
		tablet.MysqlctlProcess.ExtraArgs = append(tablet.MysqlctlProcess.ExtraArgs, mysqlctlArg...)
	}

	err = clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shard.Name, cell, master.TabletUID)
	require.NoError(t, err)

	// Start vtgate
	err = clusterInstance.StartVtgate()
	require.NoError(t, err)
}

func insertRow(t *testing.T, id int, productName string) {
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host:  clusterInstance.Hostname,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: vtgateUser,
		Pass:  vtgatePassword,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	insertSmt := fmt.Sprintf(insertTable, id, productName)
	_, err = conn.ExecuteFetch(insertSmt, 1000, true)
	require.NoError(t, err)
}
