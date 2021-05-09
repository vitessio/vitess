package kmsbackup

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"time"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vterrors"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/planetscale/common-libs/files"

	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

const (
	annotationsFilePath = "ANNOTATIONS_FILE_PATH"
	lastBackupLabel     = "psdb.co/last-backup-id"
	backupIDLabel       = "psdb.co/backup-id"
)

var (
	backupRegion = flag.String("psdb.backup_region", "", "The region for the backups")
	backupBucket = flag.String("psdb.backup_bucket", "", "S3 bucket for backups")
	backupARN    = flag.String("psdb.backup_arn", "", "ARN for the S3 bucket")
)

func init() {
	backupstorage.BackupStorageMap["kmsbackup"] = &FilesBackupStorage{}
}

// FilesBackupStorage satisfies backupstorage.BackupStorage.
type FilesBackupStorage struct {
}

// ListBackups satisfies backupstorage.BackupStorage.
// This is a custom implementation that returns at most a single value based on the pod label.
// It uses the k8s downward api feature to extract the label values.
func (fbs *FilesBackupStorage) ListBackups(ctx context.Context, dir string) ([]backupstorage.BackupHandle, error) {
	lastBackupID, err := loadTag(lastBackupLabel)
	if err != nil {
		return nil, err
	}
	// lastBackupID won't be set if there was no previous backup.
	if lastBackupID == "" {
		return nil, nil
	}

	// We have to provide a vitess compliant name. Some vitess tools parse this info.
	// This code is copied from mysqlctl/backup.go.
	tabletAlias := &topodatapb.TabletAlias{
		Cell: "vtbackup",
		Uid:  1,
	}
	name := fmt.Sprintf("%v.%v", time.Now().UTC().Format(mysqlctl.BackupTimestampFormat), tabletAlias)

	fbh, err := fbs.createHandle(ctx, lastBackupID, dir, name)
	if err != nil {
		return nil, err
	}
	return []backupstorage.BackupHandle{fbh}, nil
}

// StartBackup satisfies backupstorage.BackupStorage.
func (fbs *FilesBackupStorage) StartBackup(ctx context.Context, dir string, name string) (backupstorage.BackupHandle, error) {
	backupID, err := loadTag(backupIDLabel)
	if err != nil {
		return nil, err
	}
	handle, err := fbs.createHandle(ctx, backupID, dir, name)
	if err != nil {
		return nil, err
	}
	if err := handle.createRoot(ctx); err != nil {
		return nil, err
	}
	return handle, nil
}

func (fbs *FilesBackupStorage) createHandle(ctx context.Context, backupID, dir, name string) (*filesBackupHandle, error) {
	if *backupRegion == "" {
		return nil, errors.New("backup_region is not specified")
	}
	if *backupBucket == "" {
		return nil, errors.New("backup_bucket is not specified")
	}
	if *backupARN == "" {
		return nil, errors.New("backup_arn is not specified")
	}
	if backupID == "" {
		return nil, errors.New("backup_id is not specified")
	}

	sess, err := session.NewSession()
	if err != nil {
		return nil, vterrors.Wrap(err, "failed to initialize aws session")
	}

	impl, err := files.NewEncryptedS3Files(sess, *backupRegion, *backupBucket, "", *backupARN)
	if err != nil {
		return nil, vterrors.Wrap(err, "could not create encrypted s3 files")
	}

	rootPath := path.Join("/", backupID, dir)
	return newFilesBackupHandle(impl, rootPath, dir, name), nil
}

// RemoveBackup satisfies backupstorage.BackupStorage.
// This function is a no-op because removal of backups is handled by singularity.
func (fbs *FilesBackupStorage) RemoveBackup(ctx context.Context, dir string, name string) error {
	return nil
}

// Close satisfies backupstorage.BackupStorage.
// This function is a no-op because an aws session does not need to be closed.
func (fbs *FilesBackupStorage) Close() error {
	return nil
}

func loadTag(label string) (string, error) {
	tags, err := loadTags()
	if err != nil {
		return "", err
	}
	return tags[label], nil
}

// loadTags was adapted from vttablet-starter/main.go.
func loadTags() (map[string]string, error) {
	result := map[string]string{}

	filePath := os.Getenv(annotationsFilePath)
	if filePath == "" {
		return nil, fmt.Errorf("%v was not specified", annotationsFilePath)
	}

	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("can't read file %v: %v", filePath, err)
	}
	lines := bytes.Split(data, []byte{'\n'})

	for _, line := range lines {
		line = bytes.TrimSpace(line)

		if len(line) == 0 {
			continue
		}

		parts := bytes.SplitN(line, []byte{'='}, 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("can't parse line: %v", line)
		}
		key := string(parts[0])
		value, err := strconv.Unquote(string(parts[1]))
		if err != nil {
			return nil, fmt.Errorf("can't parse quoted value: %q", parts[1])
		}
		result[key] = value
	}
	return result, nil
}
