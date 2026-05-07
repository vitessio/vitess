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
	"syscall"
)

// loopbackMount creates a small ext4 loopback filesystem and mounts it. It
// returns the mount path. Caller must call cleanup(). Requires root.
//
// Sized deliberately tight so the test fills the disk fast in CI. Floor
// comes from what the Vitess cnf creates on first init, with our
// EXTRA_MY_CNF overrides applied:
//
//	mysql.ibd, sys schema, ibdata1                 ~50 MB
//	undo_001 + undo_002                            ~32 MB
//	innodb_redo_log_capacity (we override to 8 MB)   8 MB
//	first binlog + relay log files (capped at 16M)  ~10 MB
//	misc init artifacts, slow log, error log        ~10 MB
//	──────────────────────────────────────────────
//	floor:                                         ~110 MB
//
// 256 MB image → ~145 MB headroom → ~145 1-MB BLOB inserts to wedge,
// roughly 15–25 s of insert traffic. If a future Vitess cnf change pushes
// the floor higher and mysqld fails to start in CI, bump this to 320 MB.
const loopbackImageSizeMB = 256

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

	if err := runCmd("dd", "if=/dev/zero", "of="+imagePath, "bs=1M",
		fmt.Sprintf("count=%d", loopbackImageSizeMB), "status=none"); err != nil {
		return nil, fmt.Errorf("dd: %w", err)
	}
	if err := runCmd("mkfs.ext4", "-F", "-q", imagePath); err != nil {
		return nil, fmt.Errorf("mkfs.ext4: %w", err)
	}
	if err := runCmd("mount", "-o", "loop", imagePath, mountDir); err != nil {
		return nil, fmt.Errorf("mount: %w", err)
	}
	// MySQL needs to write here as the unprivileged tablet user; the test
	// itself runs as root so just open it up.
	if err := os.Chmod(mountDir, 0o777); err != nil {
		return nil, fmt.Errorf("chmod: %w", err)
	}

	return &mount{imagePath: imagePath, mountDir: mountDir}, nil
}

func (m *mount) cleanup() {
	// Lazy unmount tolerates lingering mysqld file handles after the test.
	_ = exec.Command("umount", "-l", m.mountDir).Run()
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
