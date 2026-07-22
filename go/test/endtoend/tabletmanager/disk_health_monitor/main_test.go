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

package diskhealthmonitor

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/tabletmanager/disk_health_monitor/testfs"
	"vitess.io/vitess/go/vt/log"
)

const (
	cell         = "zone1"
	hostname     = "localhost"
	keyspaceName = "ks"

	// Aggressive monitor cadence so the test asserts within a few seconds.
	diskWriteInterval = 500 * time.Millisecond
	diskWriteTimeout  = 2 * time.Second

	// Generous status timeout per CLAUDE.md guidance — CI runners can be
	// resource-starved and disk I/O on FUSE is slower than ext4.
	tabletStatusTimeout = 30 * time.Second
)

var (
	clusterInstance *cluster.LocalProcessCluster
	primaryTablet   *cluster.Vttablet

	testfsCmd     *exec.Cmd
	testfsBacking string
	testfsMount   string

	// helperDied is closed by a single watcher goroutine once the testfs
	// subprocess exits, so any number of readers can check liveness via a
	// non-blocking select without racing for the Wait() result.
	helperDied    chan struct{}
	helperWaitErr error // set by the watcher before closing helperDied
)

func TestMain(m *testing.M) {
	flag.Parse()

	if os.Getenv("CI") == "" && os.Getenv("GITHUB_ACTIONS") == "" {
		fmt.Println("skipping disk_health_monitor e2e test: requires a Linux CI worker with FUSE")
		os.Exit(0)
	}

	os.Exit(run(m))
}

func run(m *testing.M) int {
	tmpDir, err := os.MkdirTemp("", "vt_disk_health_monitor_")
	if err != nil {
		errf("failed to create temp dir: %v", err)
		return 1
	}
	defer os.RemoveAll(tmpDir)

	testfsBacking = filepath.Join(tmpDir, "backing")
	testfsMount = filepath.Join(tmpDir, "mount")
	for _, d := range []string{testfsBacking, testfsMount} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			errf("mkdir %s: %v", d, err)
			return 1
		}
	}

	helperBin := filepath.Join(tmpDir, "testfs")
	if err := buildTestFS(helperBin); err != nil {
		errf("build testfs: %v", err)
		return 1
	}

	if err := startTestFS(helperBin); err != nil {
		errf("start testfs: %v", err)
		return 1
	}
	defer stopTestFS()

	clusterInstance = cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	// Only --disk-write-dir lives on the gated FUSE mount, so the monitor's
	// probe writes stall when the helper is stalled. mysqld's datadir and
	// vttablet's logs stay on real disk — keeping cluster I/O (including
	// failure-path log reads in the harness) outside the gate.
	clusterInstance.VtTabletExtraArgs = []string{
		"--disk-write-dir", testfsMount,
		"--disk-write-interval", diskWriteInterval.String(),
		"--disk-write-timeout", diskWriteTimeout.String(),
	}

	if err := clusterInstance.StartTopo(); err != nil {
		errf("StartTopo: %v", err)
		return 1
	}

	keyspace := &cluster.Keyspace{Name: keyspaceName}
	if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false, clusterInstance.Cell); err != nil {
		errf("StartUnshardedKeyspace: %v", err)
		return 1
	}

	for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
		if tablet.Type == "primary" {
			primaryTablet = tablet
			break
		}
	}
	if primaryTablet == nil {
		errf("no primary tablet found after StartUnshardedKeyspace")
		return 1
	}

	return m.Run()
}

func buildTestFS(out string) error {
	build := exec.Command("go", "build", "-o", out, "./testfs/cmd/testfs")
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	return build.Run()
}

func startTestFS(bin string) error {
	cmd := exec.Command(bin, "-mount", testfsMount, "-backing", testfsBacking)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}

	ready := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "READY" {
				ready <- nil
				go io.Copy(io.Discard, stdout)
				return
			}
		}
		if err := scanner.Err(); err != nil {
			ready <- err
		} else {
			ready <- errors.New("testfs exited before printing READY")
		}
	}()

	select {
	case err := <-ready:
		if err != nil {
			_ = cmd.Process.Kill()
			return err
		}
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		return errors.New("timed out waiting for testfs READY")
	}

	testfsCmd = cmd
	helperDied = make(chan struct{})
	go func() {
		helperWaitErr = cmd.Wait()
		close(helperDied)
	}()
	log.Info("testfs ready", slog.String("mount", testfsMount), slog.Int("pid", cmd.Process.Pid))
	return nil
}

func stopTestFS() {
	if testfsCmd == nil || testfsCmd.Process == nil {
		return
	}
	// Always Clear before Close: a test that panicked mid-stall must not
	// leave testfs gating waiters with in-flight ops wedged at unmount.
	_ = testfs.Clear(testfsCmd.Process.Pid)
	_ = testfs.Close(testfsCmd.Process.Pid)
	select {
	case <-helperDied:
	case <-time.After(30 * time.Second):
		errf("testfs did not exit on SIGTERM, killing")
		_ = testfsCmd.Process.Kill()
		<-helperDied
	}
	// Belt-and-suspenders: unmount before the temp dir is removed.
	if err := exec.Command("fusermount", "-u", testfsMount).Run(); err != nil {
		_ = exec.Command("fusermount3", "-u", testfsMount).Run()
	}
}

func errf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "disk_health_monitor e2e: "+format+"\n", args...)
}
