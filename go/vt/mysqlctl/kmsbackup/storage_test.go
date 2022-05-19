package kmsbackup

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/planetscale/common-libs/files"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	awsCredFile    = flag.String("aws-credentials-file", "", "AWS Credentials file")
	awsCredProfile = flag.String("aws-credentials-profile", "", "Profile for AWS Credentials")
	awsS3Bucket    = flag.String("aws-s3-bucket", "planetscale-vitess-private-ci", "Bucket to use for S3 for AWS Credentials")
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestFileBackupStorage_ListBackups(t *testing.T) {
	ctx := context.Background()

	content := fmt.Sprintf(`%s="1234"`, lastBackupLabel)
	tmpfile := createTempFile(t, content)
	t.Cleanup(func() { os.Remove(tmpfile) })

	os.Setenv(annotationsFilePath, tmpfile)

	fbs := testFilesBackupStorage(t)

	backups, err := fbs.ListBackups(ctx, "a/b")
	assert.NoError(t, err)
	require.Equal(t, 1, len(backups))

	backup := backups[0]
	assert.Equal(t, "a/b", backup.Directory())
	assert.NotEmpty(t, backup.Name())

	fbh := backup.(*filesBackupHandle)
	assert.Equal(t, "/1234/a/b", fbh.rootPath)

	// It should fail if backup params are not set.
	fbs.region = ""
	_, err = fbs.ListBackups(ctx, "a/b")
	assert.Error(t, err)

	// ListBackups should return empty if the label is not set.
	tmpfile = createTempFile(t, "")
	t.Cleanup(func() { os.Remove(tmpfile) })

	os.Setenv(annotationsFilePath, tmpfile)

	backups, err = fbs.ListBackups(ctx, "a/b")
	assert.NoError(t, err)
	require.Equal(t, 0, len(backups))

	// it should fail if loadDownwardAPIMap fails.
	os.Setenv(annotationsFilePath, "nosuchfile")
	_, err = fbs.ListBackups(ctx, "a/b")
	assert.Error(t, err)
}

func TestFilesBackupStorage_ListBackups_withExcludedKeyspace(t *testing.T) {
	ctx := context.Background()

	content := fmt.Sprintf("%s=\"1234\"\n%s=\"not-included\"", lastBackupLabel, lastBackupExcludedKeyspacesLabel)
	tmpfile := createTempFile(t, content)
	defer os.Remove(tmpfile)
	os.Setenv(annotationsFilePath, tmpfile)

	fbs := testFilesBackupStorage(t)
	backups, err := fbs.ListBackups(ctx, "a/b")
	assert.NoError(t, err)
	require.Equal(t, 1, len(backups))

	backups, err = fbs.ListBackups(ctx, "not-included/b")
	assert.NoError(t, err)
	require.Equal(t, 0, len(backups))
}

func TestFilesBackupStorage_StartBackup(t *testing.T) {
	ctx := context.Background()

	backupID := rand.Int63()
	content := fmt.Sprintf(`%v="%v"`, backupIDLabel, backupID)
	tmpfile := createTempFile(t, content)
	t.Cleanup(func() { os.Remove(tmpfile) })

	os.Setenv(annotationsFilePath, tmpfile)

	fbs := testFilesBackupStorage(t)

	handle, err := fbs.StartBackup(ctx, "a", "b")
	require.NoError(t, err)

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

func TestFilesBackupStorage_StartBackup_sanityCheck(t *testing.T) {
	ctx := context.Background()

	backupID := rand.Int63()
	content := fmt.Sprintf(`%v="%v"`, backupIDLabel, backupID)
	tmpfile := createTempFile(t, content)
	t.Cleanup(func() { os.Remove(tmpfile) })

	os.Setenv(annotationsFilePath, tmpfile)

	fbs := testFilesBackupStorage(t)

	handle, err := fbs.StartBackup(ctx, "a", "b")
	require.NoError(t, err)
	t.Cleanup(func() {
		handle.(*filesBackupHandle).fs.RemoveAll(ctx, fmt.Sprintf("/%v", backupID))
	})

	w, err := handle.AddFile(ctx, "ssfile", 10)
	require.NoError(t, err)
	w.Write([]byte("test content"))
	require.NoError(t, err)
	w.Close()

	w, err = handle.AddFile(ctx, backupManifestFileName, 10)
	require.NoError(t, err)
	w.Close()

	// make sure that a nonexistent file causes an error
	fbh := handle.(*filesBackupHandle)
	fbh.filesAdded["/nonexistent"] = int64(10)
	_, err = handle.AddFile(ctx, backupManifestFileName, 10)
	assert.Error(t, err)

}

func TestFilesBackupStorage_API(t *testing.T) {
	ctx := context.Background()
	fbs := testFilesBackupStorage(t)
	assert.NoError(t, fbs.RemoveBackup(ctx, "", ""))
	assert.NoError(t, fbs.Close())
}

func TestFilesBackupStorage_StartBackup_uploadSizeFile(t *testing.T) {
	ctx := context.Background()

	backupID := rand.Int63()
	content := fmt.Sprintf(`%v="%v"`, backupIDLabel, backupID)
	tmpfile := createTempFile(t, content)
	t.Cleanup(func() { os.Remove(tmpfile) })

	os.Setenv(annotationsFilePath, tmpfile)

	fbs := testFilesBackupStorage(t)

	handle, err := fbs.StartBackup(ctx, "a", "b")
	require.NoError(t, err)
	t.Cleanup(func() {
		handle.(*filesBackupHandle).fs.RemoveAll(ctx, fmt.Sprintf("/%v", backupID))
	})

	w, err := handle.AddFile(ctx, "ssfile", 10)
	require.NoError(t, err)
	w.Write([]byte("test content"))
	require.NoError(t, err)
	w.Close()

	w, err = handle.AddFile(ctx, backupManifestFileName, 10)
	require.NoError(t, err)
	w.Close()

	input := []byte("20")
	r, err := handle.ReadFile(ctx, "SIZE")
	require.NoError(t, err)
	output, err := ioutil.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, input, output)
	r.Close()

}

func testFilesBackupStorage(t *testing.T) *FilesBackupStorage {
	t.Helper()

	region := "us-east-1"
	bucket := *awsS3Bucket
	arn := "not-used"

	testDir := t.TempDir()

	fs, err := files.NewLocalFiles(testDir)
	require.NoError(t, err)

	if *awsCredFile != "" {
		sess, err := session.NewSession(&aws.Config{
			Credentials: credentials.NewSharedCredentials(*awsCredFile, *awsCredProfile),
			Region:      aws.String(region),
		})
		require.NoError(t, err)

		fs = files.NewS3Files(sess, region, bucket, "")
	} else {
		t.Logf("s3 integration is disabled, using local filesystem abstraction")
	}

	f := &FilesBackupStorage{
		region: region,
		bucket: bucket,
		arn:    arn,
		files:  fs,
	}

	return f
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
