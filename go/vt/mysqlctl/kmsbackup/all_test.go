package kmsbackup

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListBackups(t *testing.T) {
	ctx := context.Background()
	setTestDefaults(t)

	content := fmt.Sprintf(`%s="1234"`, lastBackupLabel)
	tmpfile := createTempFile(t, content)
	defer os.Remove(tmpfile)
	os.Setenv(annotationsFilePath, tmpfile)

	fbs := &FilesBackupStorage{}
	backups, err := fbs.ListBackups(ctx, "a/b")
	assert.NoError(t, err)
	require.Equal(t, 1, len(backups))

	backup := backups[0]
	assert.Equal(t, "a/b", backup.Directory())
	assert.NotEmpty(t, backup.Name())

	fbh := backup.(*filesBackupHandle)
	assert.Equal(t, "/1234/a/b", fbh.rootPath)

	// It should fail if backup params are not set.
	*backupRegion = ""
	_, err = fbs.ListBackups(ctx, "a/b")
	assert.Error(t, err)
	setTestDefaults(t)

	// ListBackups should return empty if the label is not set.
	tmpfile = createTempFile(t, "")
	defer os.Remove(tmpfile)
	os.Setenv(annotationsFilePath, tmpfile)

	backups, err = fbs.ListBackups(ctx, "a/b")
	assert.NoError(t, err)
	require.Equal(t, 0, len(backups))

	// it should fail if loadDownwardAPIMap fails.
	os.Setenv(annotationsFilePath, "nosuchfile")
	_, err = fbs.ListBackups(ctx, "a/b")
	assert.Error(t, err)
}

func TestListBackupsWithExcludedKeyspace(t *testing.T) {
	ctx := context.Background()
	setTestDefaults(t)

	content := fmt.Sprintf("%s=\"1234\"\n%s=\"not-included\"", lastBackupLabel, lastBackupExcludedKeyspacesLabel)
	tmpfile := createTempFile(t, content)
	defer os.Remove(tmpfile)
	os.Setenv(annotationsFilePath, tmpfile)

	fbs := &FilesBackupStorage{}
	backups, err := fbs.ListBackups(ctx, "a/b")
	assert.NoError(t, err)
	require.Equal(t, 1, len(backups))

	backups, err = fbs.ListBackups(ctx, "not-included/b")
	assert.NoError(t, err)
	require.Equal(t, 0, len(backups))
}

func TestStartBackup(t *testing.T) {
	ctx := context.Background()
	setTestDefaults(t)

	backupID := rand.Int63()
	content := fmt.Sprintf(`%v="%v"`, backupIDLabel, backupID)
	tmpfile := createTempFile(t, content)
	defer os.Remove(tmpfile)
	os.Setenv(annotationsFilePath, tmpfile)
	t.Logf("backup ID: %v", backupID)

	fbs := &FilesBackupStorage{}

	handle, err := fbs.StartBackup(ctx, "a", "b")
	require.NoError(t, err)
	defer handle.(*filesBackupHandle).fs.RemoveAll(ctx, fmt.Sprintf("/%v", backupID))

	w, err := handle.AddFile(ctx, "ssfile", 10)
	require.NoError(t, err)

	input := []byte("test content")
	_, err = w.Write(input)
	require.NoError(t, err)
	w.Close()

	// Make sure you can't start another backup in the same place.
	_, err = fbs.StartBackup(ctx, "a", "b")
	assert.Error(t, err)

	r, err := handle.ReadFile(ctx, "ssfile")
	require.NoError(t, err)
	output, err := ioutil.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, input, output)
	r.Close()

	// Test fbh no-op API
	assert.NoError(t, handle.EndBackup(ctx))
	assert.NoError(t, handle.AbortBackup(ctx))
}

func TestBackupStorageAPI(t *testing.T) {
	ctx := context.Background()
	fbs := &FilesBackupStorage{}
	assert.NoError(t, fbs.RemoveBackup(ctx, "", ""))
	assert.NoError(t, fbs.Close())
}

func setTestDefaults(t *testing.T) {
	os.Setenv(annotationsFilePath, "")
	*backupRegion = "us-east-2"
	*backupBucket = "sougou-bucket"
	*backupARN = "arn:aws:kms:us-east-2:396684171460:key/d1ade011-ed2a-4960-8d6e-c7a5bdb282f5"
}

// createTempFile creates a temp file with the provided contents
// and returns the name of the file.
func createTempFile(t *testing.T, content string) string {
	t.Helper()
	tmpfile, err := ioutil.TempFile("", "backup_labels_test")
	require.NoError(t, err)
	defer tmpfile.Close()

	_, err = tmpfile.Write([]byte(content))
	require.NoError(t, err)
	return tmpfile.Name()
}

func TestMain(m *testing.M) {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}
