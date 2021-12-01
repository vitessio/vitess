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
func (f *filesBackupHandle) Directory() string {
	return f.dir
}

// Name satisfiles backupstorage.BackupHandle.
func (f *filesBackupHandle) Name() string {
	return f.name
}

func (f *filesBackupHandle) createRoot(ctx context.Context) error {
	err := f.fs.MkdirAll(ctx, f.rootPath)
	if err != nil {
		return err
	}

	// Make sure the target dir is empty.
	// TODO(sougou): this type of check does not fully protect against races.
	// I tried creating a lock file, but S3 is too permissive.
	// For now, we'll just trust that the caller will never
	// attempt two concurrent backups with the same ID.
	dir, err := f.fs.ReadDir(ctx, f.rootPath)
	if err != nil {
		return err
	}

	if len(dir) != 0 {
		return fmt.Errorf("target directory not empty: has %d entries", len(dir))
	}

	return nil
}

// AddFile satisfiles backupstorage.BackupHandle.
func (f *filesBackupHandle) AddFile(ctx context.Context, filename string, approxFileSize int64) (io.WriteCloser, error) {
	filePath := path.Join(f.rootPath, filename)
	return f.fs.Create(ctx, filePath, true, files.WithSizeHint(approxFileSize))
}

// ReadFile satisfiles backupstorage.BackupHandle.
func (f *filesBackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	filePath := path.Join(f.rootPath, filename)
	return f.fs.Open(ctx, filePath)
}

// EndBackup satisfiles backupstorage.BackupHandle. It's a no-op.
func (f *filesBackupHandle) EndBackup(ctx context.Context) error {
	return nil
}

// AbortBackup satisfiles backupstorage.BackupHandle. It's a no-op.
func (f *filesBackupHandle) AbortBackup(ctx context.Context) error {
	return nil
}
