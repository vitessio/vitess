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

package cluster

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/syscallutil"
	"vitess.io/vitess/go/vt/log"
)

const verifyDeadTimeout = 5 * time.Second

// mysqlPIDFile returns the path to the mysql.pid file for a given tablet UID.
func mysqlPIDFile(tabletUID int) string {
	return path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.pid", tabletUID))
}

// mysqlForceShutdown reads the PID file for the given tablet UID, kills
// mysqld_safe first (to prevent it from restarting mysqld), then kills the
// process group (falling back to the individual PID), and verifies the
// process is dead.
func mysqlForceShutdown(tabletUID int) error {
	pidFile := mysqlPIDFile(tabletUID)
	pidBytes, err := os.ReadFile(pidFile)
	if err != nil {
		// PID file doesn't exist — server must have stopped already.
		return nil
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil {
		return fmt.Errorf("failed to parse PID from %s: %w", pidFile, err)
	}

	log.Info(fmt.Sprintf("Force-killing MySQL for tablet %d (pid %d)", tabletUID, pid))

	// Kill mysqld_safe first so it doesn't restart mysqld after we kill it.
	killMysqldSafe(tabletUID)

	// Try killing the entire process group. If that fails (e.g. the PGID
	// doesn't match mysqld's PID), fall back to killing the individual process.
	if err := syscallutil.KillProcessGroup(pid, syscall.SIGKILL); err != nil && !isNoSuchProcess(err) {
		log.Warn(fmt.Sprintf("Failed to kill process group %d, falling back to individual kill: %v", pid, err))
		if err := syscallutil.Kill(pid, syscall.SIGKILL); err != nil && !isNoSuchProcess(err) {
			return fmt.Errorf("failed to kill MySQL pid %d: %w", pid, err)
		}
	}

	return verifyProcessDead(pid, verifyDeadTimeout)
}

// killMysqldSafe finds and kills any mysqld_safe/mariadbd-safe process
// associated with the given tablet UID. Errors are logged but not returned
// since this is best-effort.
func killMysqldSafe(tabletUID int) {
	out, err := exec.Command("sh", "-c",
		fmt.Sprintf("ps auxww | grep -E 'mysqld_safe|mariadbd-safe' | grep vt_%010d | grep -v grep | awk '{print $2}'", tabletUID)).Output()
	if err != nil {
		return
	}
	pidStr := strings.TrimSpace(string(out))
	if pidStr == "" {
		return
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil || pid <= 0 {
		return
	}
	log.Info(fmt.Sprintf("Killing mysqld_safe for tablet %d (pid %d)", tabletUID, pid))
	if err := syscallutil.Kill(pid, syscall.SIGKILL); err != nil && !isNoSuchProcess(err) {
		log.Warn(fmt.Sprintf("Failed to kill mysqld_safe pid %d: %v", pid, err))
	}
}

// isNoSuchProcess returns true if the error indicates the process doesn't exist (ESRCH).
func isNoSuchProcess(err error) bool {
	return errors.Is(err, syscall.ESRCH)
}

// verifyProcessDead polls with signal 0 until the process is confirmed dead
// or the timeout expires.
func verifyProcessDead(pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		err := syscallutil.Kill(pid, 0)
		if err != nil {
			// Process is gone.
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("process %d still alive after %s", pid, timeout)
}
