package gcsbackup

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLabels(t *testing.T) {
	t.Run("existing path", func(t *testing.T) {
		var assert = require.New(t)
		var path = setupLabels(t)

		all, err := load(path)

		assert.NoError(err)
		assert.Equal("last-backup-id", all.LastBackupID)
		assert.Equal([]string{"a", "b"}, all.LastBackupExcludedKeyspaces)
		assert.Equal("backup-id", all.BackupID)
	})

	t.Run("missing path", func(t *testing.T) {
		var assert = require.New(t)

		_, err := load("foo")

		assert.EqualError(err, `open annotations file: open foo: no such file or directory`)
	})

	t.Run("empty path", func(t *testing.T) {
		var assert = require.New(t)

		f, err := ioutil.TempFile(t.TempDir(), "empty_labels")
		assert.NoError(err)
		assert.NoError(f.Close())

		all, err := load(f.Name())

		assert.NoError(err)
		assert.Empty(all.LastBackupID)
		assert.Empty(all.LastBackupExcludedKeyspaces)
		assert.Empty(all.BackupID)
	})
}

func setupLabels(t testing.TB) string {
	t.Helper()

	f, err := ioutil.TempFile(t.TempDir(), "labels.*")
	if err != nil {
		t.Fatalf("tempfile: %s", err)
	}
	defer f.Close()

	all := map[string]string{
		"key1":                           "foo",
		"key2":                           "baz",
		lastBackupIDLabel:                "last-backup-id",
		lastBackupExcludedKeyspacesLabel: "a, , b",
		backupIDLabel:                    "backup-id",
	}

	for key, value := range all {
		fmt.Fprintf(f, "%s= %q \n", key, value)
	}

	if err := f.Sync(); err != nil {
		t.Fatalf("sync: %s", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}

	return f.Name()
}
