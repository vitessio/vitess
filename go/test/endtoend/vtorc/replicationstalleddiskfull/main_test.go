//go:build linux

/*
Copyright 2026 The Vitess Authors.

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

package replicationstalleddiskfull

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

const (
	cellName     = "zone1"
	hostname     = "localhost"
	keyspaceName = "ks"
	shardName    = "0"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtorcProcess    *cluster.VTOrcProcess
	primaryTablet   *cluster.Vttablet
	replicaTablet   *cluster.Vttablet
	diskMount       *mount
)

// shouldSkip returns a non-empty reason string when the e2e cannot run on the
// current host. Linux is enforced via the build tag; this checks the runtime
// preconditions: must NOT be root (Vitess binaries refuse to start as root)
// but must have passwordless sudo available (loopback mount + mkfs.ext4
// require it). Also requires VT_TEST_DISK_FULL=1 — an explicit opt-in so a
// developer who happens to have passwordless sudo on Linux doesn't trigger
// the test by accident.
func shouldSkip() string {
	if os.Getenv("VT_TEST_DISK_FULL") != "1" {
		return "set VT_TEST_DISK_FULL=1 to run; this test mounts a loopback filesystem"
	}
	if os.Geteuid() == 0 {
		return "must run as a non-root user; Vitess binaries refuse to start as root"
	}
	if err := exec.Command("sudo", "-n", "true").Run(); err != nil {
		return "passwordless sudo required (loopback mount + mkfs.ext4)"
	}
	if _, err := exec.LookPath("mkfs.ext4"); err != nil {
		return "mkfs.ext4 not found in PATH"
	}
	return ""
}

func TestMain(m *testing.M) {
	if reason := shouldSkip(); reason != "" {
		fmt.Fprintln(os.Stderr, "skipping replicationstalleddiskfull e2e:", reason)
		os.Exit(0)
	}

	exitCode, err := func() (int, error) {
		// Loopback mount must come up first; the replica's mysqld will
		// initialise its data dir on it.
		var err error
		mountRoot, err := os.MkdirTemp("", "vtorc_diskfull_*")
		if err != nil {
			return 1, err
		}
		defer os.RemoveAll(mountRoot)

		diskMount, err = newLoopbackMount(mountRoot)
		if err != nil {
			return 1, err
		}
		defer diskMount.cleanup()

		replicaDataDir := filepath.Join(diskMount.mountDir, "replica_data")
		replicaTmpDir := filepath.Join(diskMount.mountDir, "replica_tmp")
		for _, dir := range []string{replicaDataDir, replicaTmpDir} {
			if err := os.MkdirAll(dir, 0o777); err != nil {
				return 1, err
			}
		}

		cnfPath := filepath.Join(diskMount.mountDir, "replica_extra.cnf")
		if err := os.WriteFile(cnfPath, []byte(replicaCnf(replicaDataDir, replicaTmpDir)), 0o644); err != nil {
			return 1, err
		}

		clusterInstance = cluster.NewCluster(cellName, hostname)
		defer clusterInstance.Teardown()

		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// 1 primary + 1 replica.
		primaryTablet = clusterInstance.NewVttabletInstance("replica", 100, cellName)
		replicaTablet = clusterInstance.NewVttabletInstance("replica", 101, cellName)

		keyspace := &cluster.Keyspace{Name: keyspaceName}
		shard := &cluster.Shard{Name: shardName, Vttablets: []*cluster.Vttablet{primaryTablet, replicaTablet}}
		if err := clusterInstance.SetupCluster(keyspace, []cluster.Shard{*shard}); err != nil {
			return 1, err
		}

		// Inject the replica-only my.cnf overrides AFTER SetupCluster has
		// created the MysqlctlProcess but BEFORE we start mysqld.
		replicaTablet.MysqlctlProcess.ExtraMyCnfPath = cnfPath

		// Start mysqlctl on both tablets.
		var mysqlctlProcs []*exec.Cmd
		for _, t := range []*cluster.Vttablet{primaryTablet, replicaTablet} {
			proc, err := t.MysqlctlProcess.StartProcess()
			if err != nil {
				return 1, err
			}
			mysqlctlProcs = append(mysqlctlProcs, proc)
		}
		for _, p := range mysqlctlProcs {
			if err := p.Wait(); err != nil {
				return 1, fmt.Errorf("mysqlctl init failed: %w", err)
			}
		}

		// Start vttablets.
		for _, t := range []*cluster.Vttablet{primaryTablet, replicaTablet} {
			t.VttabletProcess.ServingStatus = ""
			if err := t.VttabletProcess.Setup(); err != nil {
				return 1, err
			}
		}
		for _, t := range []*cluster.Vttablet{primaryTablet, replicaTablet} {
			if err := t.VttabletProcess.WaitForTabletStatuses([]string{"SERVING", "NOT_SERVING"}); err != nil {
				return 1, err
			}
		}

		out, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(
			"SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy="+policy.DurabilityNone)
		if err != nil {
			return 1, fmt.Errorf("SetKeyspaceDurabilityPolicy: %w (%s)", err, out)
		}

		// Promote primaryTablet via PRS.
		out, err = clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(
			"PlannedReparentShard", keyspaceName+"/"+shardName,
			"--new-primary", primaryTablet.Alias)
		if err != nil {
			return 1, fmt.Errorf("PlannedReparentShard: %w (%s)", err, out)
		}

		// Start vtorc with a fast poll cadence so we observe the analysis quickly.
		vtorcProcess = clusterInstance.NewVTOrcProcess(cluster.VTOrcConfiguration{
			InstancePollTime:     "1s",
			RecoveryPollDuration: "1s",
		}, cellName)
		if err := vtorcProcess.Setup(); err != nil {
			return 1, err
		}
		clusterInstance.VTOrcProcesses = append(clusterInstance.VTOrcProcesses, vtorcProcess)

		return m.Run(), nil
	}()

	if err != nil {
		fmt.Fprintf(os.Stderr, "TestMain setup error: %v\n", err)
	}
	os.Exit(exitCode)
}

// replicaCnf is the EXTRA_MY_CNF fragment we point the replica's mysqld at.
// EXTRA_MY_CNF is loaded after the generated cnf so these directives win.
//
// CRITICAL: only the InnoDB filesystem moves to the small mount. The relay
// log and binlog stay at their generated $VTDATAROOT/vt_<uid>/ paths on the
// regular disk. The whole point of the analysis is to catch the case where
// InnoDB's filesystem hits ENOSPC while the relay log filesystem still has
// space — InnoDB silently retries inside ha_commit_trans (Slave_*_Running
// stay Yes) while the relay log never fails. If both filesystems are on
// the same mount, the IO thread's relay-log write fails first with
// ER_REPLICA_RELAY_LOG_WRITE_FAILURE → Slave_IO_Running flips to No, which
// is the OTHER (already-detected) scenario.
//
// Sizing knobs:
//   - innodb_redo_log_capacity = 8 MB (vs 100 MB default; valid on 8.0.30+,
//     ignored on older versions)
//   - innodb_buffer_pool_size  = 32 MB (memory only; doesn't change disk)
//   - performance_schema = ON (required for our detection query)
func replicaCnf(dataDir, tmpDir string) string {
	return fmt.Sprintf(`
[mysqld]
datadir                   = %[1]s
innodb_data_home_dir      = %[1]s
innodb_log_group_home_dir = %[1]s
tmpdir                    = %[2]s

innodb_redo_log_capacity = 8388608
innodb_buffer_pool_size  = 33554432
innodb_log_buffer_size   = 1048576
performance_schema       = ON
`, dataDir, tmpDir)
}
