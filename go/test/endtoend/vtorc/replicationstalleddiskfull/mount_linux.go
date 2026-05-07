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
	"os/user"
	"path/filepath"
	"syscall"
)

// loopbackMount creates a small ext4 loopback filesystem and mounts it. It
// returns the mount path. Caller must call cleanup().
//
// The test process itself must run as a non-root user — Vitess's `vtctld`
// and other binaries refuse to start as root (`servenv.Init: running this
// as root makes no sense`). We therefore drop privileges by running each
// privileged step (`mkfs.ext4`, `mount`, `chown`, `umount`) under `sudo`,
// and chown the mount point back to the calling user so mysqld can write
// to it. The CI runner has passwordless `sudo` for ubuntu-24.04 by default.
//
// Sized so the *first* InnoDB autoextend attempt fails. Only the InnoDB
// filesystem lives here — relay log + binlog are on the regular disk, so
// the floor is just InnoDB artifacts created at mysql --initialize:
//
//	mysql.ibd, sys schema                          ~35 MB
//	ibdata1 (initial)                              ~12 MB
//	undo_001 + undo_002                            ~32 MB
//	innodb_redo_log_capacity (we override to 8 MB)   8 MB
//	doublewrite buffer + scratch                    ~8 MB
//	──────────────────────────────────────────────
//	floor:                                         ~95 MB
//
// innodb_autoextend_increment defaults to 64 MB. When the SQL thread fills
// the initial 12 MB ibdata1, InnoDB tries to grow it by 64 MB. With ~65 MB
// of headroom in a 160 MB image the first autoextend attempt fails —
// MY-012814 lands in performance_schema.error_log, and InnoDB silently
// retries (the wedge state our analysis is designed to detect).
//
// Bump to 192 MB if mysql --initialize ever fails to fit on 160 MB after a
// future Vitess cnf change.
const loopbackImageSizeMB = 160

type mount struct {
	imagePath string
	mountDir  string
}

func newLoopbackMount(rootDir string) (*mount, error) {
	imagePath := filepath.Join(rootDir, "diskfull.img")
	mountDir := filepath.Join(rootDir, "mnt")
	if err := os.MkdirAll(mountDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir mount dir: %w", err)
	}

	// dd writes to a path the calling user can write to; no sudo needed.
	if err := runCmd("dd", "if=/dev/zero", "of="+imagePath, "bs=1M",
		fmt.Sprintf("count=%d", loopbackImageSizeMB), "status=none"); err != nil {
		return nil, fmt.Errorf("dd: %w", err)
	}
	if err := runCmd("sudo", "mkfs.ext4", "-F", "-q", imagePath); err != nil {
		return nil, fmt.Errorf("mkfs.ext4: %w", err)
	}
	if err := runCmd("sudo", "mount", "-o", "loop", imagePath, mountDir); err != nil {
		return nil, fmt.Errorf("mount: %w", err)
	}

	// After mount, the mount point is owned by root. Hand it back to the
	// calling user so mysqld (running as the same user) can write to it.
	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("user.Current: %w", err)
	}
	owner := u.Uid + ":" + u.Gid
	if err := runCmd("sudo", "chown", "-R", owner, mountDir); err != nil {
		return nil, fmt.Errorf("chown: %w", err)
	}

	return &mount{imagePath: imagePath, mountDir: mountDir}, nil
}

func (m *mount) cleanup() {
	// Lazy unmount tolerates lingering mysqld file handles after the test.
	_ = exec.Command("sudo", "umount", "-l", m.mountDir).Run()
	_ = os.Remove(m.imagePath)
	_ = os.Remove(m.mountDir)
}

// freeBytes returns bytes available on the mount.
func (m *mount) freeBytes() (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(m.mountDir, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

func runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %v: %w (%s)", name, args, err, out)
	}
	return nil
}
