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

package vtbackup

import (
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/backup/testhelper"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	vtutils "vitess.io/vitess/go/vt/utils"
)

// TestVtbackupS3 runs the backup flow against an S3-compatible backend (e.g. MicroCeph).
// Skips if AWS_* env vars are not set.
func TestVtbackupS3(t *testing.T) {
	cfg := testhelper.RequireS3Config(t)
	testhelper.SetS3Env(t, cfg)

	origCluster := localCluster
	origPrimary := primary
	origReplica1 := replica1
	origReplica2 := replica2
	origNewInitDBFile := newInitDBFile
	origDbCredentialFile := dbCredentialFile
	origCommonTabletArg := make([]string, len(commonTabletArg))
	copy(origCommonTabletArg, commonTabletArg)

	defer func() {
		localCluster = origCluster
		primary = origPrimary
		replica1 = origReplica1
		replica2 = origReplica2
		newInitDBFile = origNewInitDBFile
		dbCredentialFile = origDbCredentialFile
		commonTabletArg = origCommonTabletArg
		s3ConfigForVtbackup = nil
	}()

	s3Cluster := cluster.NewCluster(cell, hostname)
	defer s3Cluster.Teardown()

	s3Cluster.VtctldExtraArgs = append(s3Cluster.VtctldExtraArgs, "--backup-storage-implementation", "s3")
	err := s3Cluster.StartTopo()
	require.NoError(t, err)

	s3Cluster.Keyspaces = []cluster.Keyspace{
		{
			Name: keyspaceName,
			Shards: []cluster.Shard{
				{Name: shardName},
			},
		},
	}
	shard := &s3Cluster.Keyspaces[0].Shards[0]
	vtctldClientProcess := cluster.VtctldClientProcessInstance(s3Cluster.VtctldProcess.GrpcPort, s3Cluster.TopoPort, "localhost", s3Cluster.TmpDirectory)
	_, err = vtctldClientProcess.ExecuteCommandWithOutput("CreateKeyspace", keyspaceName, "--durability-policy=semi_sync")
	require.NoError(t, err)

	dbCredentialFile = cluster.WriteDbCredentialToTmp(s3Cluster.TmpDirectory)
	initDb, err := os.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"))
	require.NoError(t, err)
	sql, err := utils.GetInitDBSQL(string(initDb), cluster.GetPasswordUpdateSQL(s3Cluster), "")
	require.NoError(t, err)
	newInitDBFile = path.Join(s3Cluster.TmpDirectory, "init_db_with_passwords.sql")
	err = os.WriteFile(newInitDBFile, []byte(sql), 0o666)
	require.NoError(t, err)

	extraArgs := []string{"--db-credentials-file", dbCredentialFile}
	s3TabletArgs := testhelper.S3TabletArgs(cfg)
	ver := s3Cluster.VtTabletMajorVersion
	baseTabletArgs := []string{
		vtutils.GetFlagVariantForTestsByVersion("--vreplication-retry-delay", ver), "1s",
		vtutils.GetFlagVariantForTestsByVersion("--degraded-threshold", ver), "5s",
		vtutils.GetFlagVariantForTestsByVersion("--lock-tables-timeout", ver), "5s",
		vtutils.GetFlagVariantForTestsByVersion("--watch-replication-stream", ver),
		vtutils.GetFlagVariantForTestsByVersion("--enable-replication-reporter", ver),
		vtutils.GetFlagVariantForTestsByVersion("--serving-state-grace-period", ver), "1s",
	}
	commonTabletArg = append(append(baseTabletArgs, "--db-credentials-file", dbCredentialFile), s3TabletArgs...)

	s3Primary := s3Cluster.NewVttabletInstance("replica", 0, "")
	s3Replica1 := s3Cluster.NewVttabletInstance("replica", 0, "")
	s3Replica2 := s3Cluster.NewVttabletInstance("replica", 0, "")
	shard.Vttablets = []*cluster.Vttablet{s3Primary, s3Replica1, s3Replica2}

	var mysqlProcs []*exec.Cmd
	for _, tablet := range shard.Vttablets {
		tablet.VttabletProcess = s3Cluster.VtprocessInstanceFromVttablet(tablet, shard.Name, keyspaceName)
		tablet.VttabletProcess.DbPassword = dbPassword
		tablet.VttabletProcess.ExtraArgs = commonTabletArg
		tablet.VttabletProcess.SupportsBackup = true

		mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, s3Cluster.TmpDirectory)
		require.NoError(t, err)
		tablet.MysqlctlProcess = *mysqlctlProcess
		tablet.MysqlctlProcess.InitDBFile = newInitDBFile
		tablet.MysqlctlProcess.ExtraArgs = extraArgs
		proc, err := tablet.MysqlctlProcess.StartProcess()
		require.NoError(t, err)
		mysqlProcs = append(mysqlProcs, proc)
	}
	for _, proc := range mysqlProcs {
		require.NoError(t, proc.Wait())
	}

	if s3Cluster.VtTabletMajorVersion >= 16 {
		err = s3Cluster.StartVTOrc(cell, keyspaceName)
		require.NoError(t, err)
	}

	localCluster = s3Cluster
	primary = s3Primary
	replica1 = s3Replica1
	replica2 = s3Replica2
	s3ConfigForVtbackup = &cfg

	primary.VttabletProcess.ServingStatus = "NOT_SERVING"
	replica1.VttabletProcess.ServingStatus = "NOT_SERVING"

	initTablets(t, true, true)
	firstBackupTest(t, true)
	tearDown(t, false)
}
