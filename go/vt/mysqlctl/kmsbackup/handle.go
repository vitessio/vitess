package kmsbackup

import (
	"context"
	"fmt"
	"io"
	"path"
	"strconv"
	"sync"

	"github.com/planetscale/common-libs/files"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	// backupManifestFileName is the MANIFEST file name within a backup.
	// pulled from go/vt/mysqlctl/backup.go
	backupManifestFileName = "MANIFEST"

	// backupSizeFileName is the SIZE file name within a backup.
	// it contains the total size of all the files in bytes for
	// the given keyspace+shard pair
	backupSizeFileName = "SIZE"
)

type filesBackupHandle struct {
	concurrency.AllErrorRecorder

	fs       files.Files
	rootPath string

	// filesAdded contains all files added so far and their length in bytes.
	// It's used for sanity checks as well as calculating the total size
	filesAdded map[string]int64
	filesMu    sync.Mutex // protects filesAdded

	// dir and name are stored and returned as is from the original request.
	dir  string
	name string
}

func newFilesBackupHandle(fs files.Files, rootPath, dir, name string) *filesBackupHandle {
	return &filesBackupHandle{
		fs:         fs,
		rootPath:   rootPath,
		dir:        dir,
		name:       name,
		filesAdded: make(map[string]int64),
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
	f.filesMu.Lock()
	f.filesAdded[filePath] = approxFileSize
	f.filesMu.Unlock()

	// since the manifest file is added last(see // go/vt/mysqlctl/builtinbackupengine.go),
	// once we got the manifest file, we make sure:
	//  * to upload the SIZE file
	//  * to upload the MANIFEST file
	//  * check that all created files exist
	//
	if filename == backupManifestFileName {
		totalSize := f.calculateTotalSize()
		if err := f.uploadSizeFile(ctx, totalSize); err != nil {
			return nil, fmt.Errorf("kmsbackup.AddFile - uploading SIZE file has failed: %v", err)
		}

		wrcloser, err := f.fs.Create(ctx, filePath, true, files.WithSizeHint(approxFileSize))
		if err != nil {
			return nil, err
		}

		// we need to call write to create the file
		_, err = wrcloser.Write([]byte{})
		if err != nil {
			return nil, fmt.Errorf("creating the manifest file has failed: %v", err)
		}
		// call close to flush the data
		wrcloser.Close()

		if err := f.sanityCheck(ctx); err != nil {
			return nil, err
		}
	}

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

// sanityCheck verifies that all added files are present.
func (f *filesBackupHandle) sanityCheck(ctx context.Context) error {
	// let's copy just the filenames, because the stat commands might take
	// time, and we shouldn't keep the lock for longer times.
	f.filesMu.Lock()
	files := make([]string, 0, len(f.filesAdded))
	for filename := range f.filesAdded {
		files = append(files, filename)
	}
	f.filesMu.Unlock()

	for _, filename := range files {
		if _, err := f.fs.Stat(ctx, filename); err != nil {
			return vterrors.Wrapf(err, "file %v does not exist", filename)
		}
	}

	return nil
}

// uploadSizeFile creates the SIZE file, writes the size to it, and then uploads it.
// we convert the size to a string instead of something more efficient to make
// debugging and working with the file easier
func (f *filesBackupHandle) uploadSizeFile(ctx context.Context, size int64) error {
	filePath := path.Join(f.rootPath, backupSizeFileName)
	sizeAsString := strconv.FormatInt(size, 10)

	wrcloser, err := f.fs.Create(ctx, filePath, true, files.WithSizeHint(int64(len(sizeAsString))))
	if err != nil {
		return err
	}
	defer wrcloser.Close()

	if _, err := wrcloser.Write([]byte(sizeAsString)); err != nil {
		return err
	}

	return nil
}

// calculateTotalSize returns the size of all the files in the backup.
func (f *filesBackupHandle) calculateTotalSize() int64 {
	var totalSize int64

	f.filesMu.Lock()
	for _, fileSize := range f.filesAdded {
		totalSize += fileSize
	}
	f.filesMu.Unlock()

	return totalSize
}
