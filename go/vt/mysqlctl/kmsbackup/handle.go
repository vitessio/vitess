package kmsbackup

import (
	"context"
	"fmt"
	"io"
	"path"

	"github.com/planetscale/common-libs/files"

	"vitess.io/vitess/go/vt/concurrency"
)

type filesBackupHandle struct {
	concurrency.AllErrorRecorder

	fs       files.Files
	rootPath string

	// dir and name are stored and returned as is from the original request.
	dir  string
	name string
}

func newFilesBackupHandle(fs files.Files, rootPath, dir, name string) *filesBackupHandle {
	return &filesBackupHandle{
		fs:       fs,
		rootPath: rootPath,
		dir:      dir,
		name:     name,
	}
}

// Directory satisfiles backupstorage.BackupHandle.
func (fbh *filesBackupHandle) Directory() string {
	return fbh.dir
}

// Name satisfiles backupstorage.BackupHandle.
func (fbh *filesBackupHandle) Name() string {
	return fbh.name
}

func (fbh *filesBackupHandle) createRoot(ctx context.Context) error {
	err := fbh.fs.MkdirAll(ctx, fbh.rootPath)
	if err != nil {
		return err
	}

	// Make sure the target dir is empty.
	// TODO(sougou): this type of check does not fully protect against races.
	// I tried creating a lock file, but S3 is too permissive.
	// For now, we'll just trust that the caller will never
	// attempt two concurrent backups with the same ID.
	dir, err := fbh.fs.ReadDir(ctx, fbh.rootPath)
	if err != nil {
		return err
	}
	if len(dir) != 0 {
		return fmt.Errorf("target directory not empty: has %d entries", len(dir))
	}
	return nil
}

// AddFile satisfiles backupstorage.BackupHandle.
func (fbh *filesBackupHandle) AddFile(ctx context.Context, filename string, approxFileSize int64) (io.WriteCloser, error) {
	filePath := path.Join(fbh.rootPath, filename)
	return fbh.fs.Create(ctx, filePath, true, files.WithSizeHint(approxFileSize))
}

// ReadFile satisfiles backupstorage.BackupHandle.
func (fbh *filesBackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	filePath := path.Join(fbh.rootPath, filename)
	return fbh.fs.Open(ctx, filePath)
}

// EndBackup satisfiles backupstorage.BackupHandle. It's a no-op.
func (fbh *filesBackupHandle) EndBackup(ctx context.Context) error {
	return nil
}

// AbortBackup satisfiles backupstorage.BackupHandle. It's a no-op.
func (fbh *filesBackupHandle) AbortBackup(ctx context.Context) error {
	return nil
}
