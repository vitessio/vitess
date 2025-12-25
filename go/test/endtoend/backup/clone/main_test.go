/*
Copyright 2025 The Vitess Authors.

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

package clone

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	vtutils "vitess.io/vitess/go/vt/utils"
)

var (
	primary          *cluster.Vttablet
	replica1         *cluster.Vttablet
	replica2         *cluster.Vttablet
	localCluster     *cluster.LocalProcessCluster
	newInitDBFile    string
	cell             = cluster.DefaultCell
	hostname         = "localhost"
	keyspaceName     = "ks"
	shardName        = "0"
	dbPassword       = "VtDbaPass"
	shardKsName      = fmt.Sprintf("%s/%s", keyspaceName, shardName)
	dbCredentialFile string
	commonTabletArg  = []string{
		vtutils.GetFlagVariantForTests("--vreplication-retry-delay"), "1s",
		vtutils.GetFlagVariantForTests("--degraded-threshold"), "5s",
		vtutils.GetFlagVariantForTests("--lock-tables-timeout"), "5s",
		vtutils.GetFlagVariantForTests("--watch-replication-stream"),
		vtutils.GetFlagVariantForTests("--enable-replication-reporter"),
		vtutils.GetFlagVariantForTests("--serving-state-grace-period"), "1s",
	}
	vtInsertTest = `
		create table if not exists vt_insert_test (
		id bigint auto_increment,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB;`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode, err := func() (int, error) {
		localCluster = cluster.NewCluster(cell, hostname)
		defer localCluster.Teardown()

		// Setup EXTRA_MY_CNF for clone plugin
		if err := setupExtraMyCnf(); err != nil {
			log.Errorf("Failed to setup extra MySQL config: %v", err)
			return 1, err
		}

		// Start topo server
		err := localCluster.StartTopo()
		if err != nil {
			return 1, err
		}

		// Start keyspace
		localCluster.Keyspaces = []cluster.Keyspace{
			{
				Name: keyspaceName,
				Shards: []cluster.Shard{
					{
						Name: shardName,
					},
				},
			},
		}
		shard := &localCluster.Keyspaces[0].Shards[0]
		vtctldClientProcess := cluster.VtctldClientProcessInstance(localCluster.VtctldProcess.GrpcPort, localCluster.TopoPort, "localhost", localCluster.TmpDirectory)
		_, err = vtctldClientProcess.ExecuteCommandWithOutput("CreateKeyspace", keyspaceName, "--durability-policy=semi_sync")
		if err != nil {
			return 1, err
		}

		// Create a new init_db.sql file that sets up passwords for all users and clone user
		dbCredentialFile = cluster.WriteDbCredentialToTmp(localCluster.TmpDirectory)
		initDb, _ := os.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"))
		initClone, err := os.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_clone.sql"))
		if err != nil {
			log.Warningf("init_clone.sql not found, clone tests may fail: %v", err)
			initClone = []byte("")
		}

		sql := string(initDb)
		// The original init_db.sql does not have any passwords. Here we update the init file with passwords
		sql, err = utils.GetInitDBSQL(sql, cluster.GetPasswordUpdateSQL(localCluster), string(initClone))
		if err != nil {
			return 1, err
		}
		newInitDBFile = path.Join(localCluster.TmpDirectory, "init_db_with_passwords_and_clone.sql")
		err = os.WriteFile(newInitDBFile, []byte(sql), 0666)
		if err != nil {
			return 1, err
		}

		extraArgs := []string{"--db-credentials-file", dbCredentialFile}
		commonTabletArg = append(commonTabletArg, "--db-credentials-file", dbCredentialFile)

		primary = localCluster.NewVttabletInstance("replica", 0, "")
		replica1 = localCluster.NewVttabletInstance("replica", 0, "")
		replica2 = localCluster.NewVttabletInstance("replica", 0, "")
		shard.Vttablets = []*cluster.Vttablet{primary, replica1, replica2}

		// Start MySql processes
		var mysqlProcs []*exec.Cmd
		for _, tablet := range shard.Vttablets {
			tablet.VttabletProcess = localCluster.VtprocessInstanceFromVttablet(tablet, shard.Name, keyspaceName)
			tablet.VttabletProcess.DbPassword = dbPassword
			tablet.VttabletProcess.ExtraArgs = commonTabletArg
			tablet.VttabletProcess.SupportsBackup = true

			mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
			if err != nil {
				return 1, err
			}
			tablet.MysqlctlProcess = *mysqlctlProcess
			tablet.MysqlctlProcess.InitDBFile = newInitDBFile
			tablet.MysqlctlProcess.ExtraArgs = extraArgs
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

		if localCluster.VtTabletMajorVersion >= 16 {
			// If vttablets are any lower than version 16, then they are running the replication manager.
			// Running VTOrc and replication manager sometimes creates the situation where VTOrc has set up semi-sync on the primary,
			// but the replication manager starts replication on the replica without setting semi-sync. This hangs the primary.
			// Even if VTOrc fixes it, since there is no ongoing traffic, the state remains blocked.
			if err := localCluster.StartVTOrc(keyspaceName); err != nil {
				return 1, err
			}
		}

		return m.Run(), nil
	}()

	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	} else {
		os.Exit(exitCode)
	}
}

// setupExtraMyCnf sets EXTRA_MY_CNF to include clone plugin configuration
func setupExtraMyCnf() error {
	cloneCnfPath := path.Join(os.Getenv("VTROOT"), "config", "mycnf", "clone.cnf")
	if _, err := os.Stat(cloneCnfPath); os.IsNotExist(err) {
		return fmt.Errorf("clone.cnf not found at %s", cloneCnfPath)
	}

	// Check if EXTRA_MY_CNF is already set
	existing := os.Getenv("EXTRA_MY_CNF")
	if existing != "" {
		// Append clone.cnf to existing
		if err := os.Setenv("EXTRA_MY_CNF", existing+":"+cloneCnfPath); err != nil {
			return fmt.Errorf("failed to set EXTRA_MY_CNF: %v", err)
		}
	} else {
		if err := os.Setenv("EXTRA_MY_CNF", cloneCnfPath); err != nil {
			return fmt.Errorf("failed to set EXTRA_MY_CNF: %v", err)
		}
	}

	log.Infof("Set EXTRA_MY_CNF to include clone plugin: %s", os.Getenv("EXTRA_MY_CNF"))
	return nil
}

// getMySQLVersion retrieves the MySQL version from a running tablet
func getMySQLVersion(t *testing.T, tablet *cluster.Vttablet) string {
	qr, err := tablet.VttabletProcess.QueryTablet("SELECT VERSION()", keyspaceName, true)
	if err != nil {
		t.Logf("Failed to get MySQL version: %v", err)
		return ""
	}
	if len(qr.Rows) == 0 {
		return ""
	}
	return qr.Rows[0][0].ToString()
}

// mysqlVersionSupportsClone checks if the MySQL version supports CLONE plugin
func mysqlVersionSupportsClone(versionStr string) bool {
	// Parse version string to extract numeric version
	// Format might be: "8.0.35-27" or "8.0.35"
	parts := strings.Split(versionStr, "-")
	versionPart := parts[0]

	// Parse the version
	flavor, version, err := mysqlctl.ParseVersionString(versionPart)
	if err != nil {
		return false
	}

	// Clone is only supported on MySQL 8.0.17+
	if flavor != mysqlctl.FlavorMySQL && flavor != mysqlctl.FlavorPercona {
		return false
	}
	if version.Major < 8 || (version.Major == 8 && version.Minor == 0 && version.Patch < 17) {
		return false
	}

	// Verify clone capability
	cleanVersion := fmt.Sprintf("%d.%d.%d", version.Major, version.Minor, version.Patch)
	capableOf := mysql.ServerVersionCapableOf(cleanVersion)
	if capableOf == nil {
		return false
	}
	hasClone, err := capableOf(capabilities.MySQLClonePluginFlavorCapability)
	return err == nil && hasClone
}

// clonePluginAvailable checks if the clone plugin is installed and active
func clonePluginAvailable(t *testing.T, tablet *cluster.Vttablet) bool {
	qr, err := tablet.VttabletProcess.QueryTablet(
		"SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'clone'",
		keyspaceName, true)
	if err != nil {
		t.Logf("Failed to check clone plugin: %v", err)
		return false
	}
	if len(qr.Rows) == 0 {
		return false
	}
	status := qr.Rows[0][0].ToString()
	return status == "ACTIVE"
}

// parseVersionFromRow parses MySQL version from a result row
func parseVersionFromRow(row []sqltypes.Value) (int, int, int, error) {
	if len(row) == 0 {
		return 0, 0, 0, errors.New("empty row")
	}

	versionStr := row[0].ToString()
	// Version format: "8.0.35" or "8.0.35-27"
	parts := strings.Split(versionStr, "-")
	versionPart := parts[0]

	versionNums := strings.Split(versionPart, ".")
	if len(versionNums) < 3 {
		return 0, 0, 0, fmt.Errorf("invalid version format: %s", versionStr)
	}

	major, err := strconv.Atoi(versionNums[0])
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid major version: %s", versionNums[0])
	}

	minor, err := strconv.Atoi(versionNums[1])
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid minor version: %s", versionNums[1])
	}

	patch, err := strconv.Atoi(versionNums[2])
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid patch version: %s", versionNums[2])
	}

	return major, minor, patch, nil
}
