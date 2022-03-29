package gcsbackup

import (
	"context"
	"errors"
	"flag"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var (
	credsFile = flag.String("gcp-credentials-file", "", "GCP Credentials file")
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestStorage(t *testing.T) {
	if *credsFile == "" {
		t.Skip("GCS backup integration tests are disabled, set --gcp-credentials-file to run them.")
	}

	t.Run("start backup, list, read", func(t *testing.T) {
		ctx := context.Background()
		assert := require.New(t)
		storage := setupStorage(t)
		data := []byte("data")

		makeBackup(t, storage, "dir", "backup")

		all, err := storage.ListBackups(ctx, "dir")
		assert.NoError(err)
		assert.Equal(1, len(all))
		assert.Equal("dir", all[0].Directory())
		assert.Equal("backup", all[0].Name())

		r, err := all[0].ReadFile(ctx, "test")
		assert.NoError(err)

		buf, err := ioutil.ReadAll(r)
		assert.NoError(err)
		assert.Equal(data, buf)
	})

	t.Run("list sorted", func(t *testing.T) {
		ctx := context.Background()
		assert := require.New(t)
		storage := setupStorage(t)
		names := []string{"a", "b"}
		dir := "dir"

		for _, name := range names {
			makeBackup(t, storage, dir, name)
		}

		all, err := storage.ListBackups(ctx, dir)
		assert.NoError(err)
		assert.Equal(len(names), len(all))

		backups := make([]string, len(names))
		for i, h := range all {
			backups[i] = h.Name()
		}

		assert.Equal(names, backups)
	})

	t.Run("list readonly", func(t *testing.T) {
		ctx := context.Background()
		assert := require.New(t)
		storage := setupStorage(t)

		makeBackup(t, storage, "dir", "backup")

		all, err := storage.ListBackups(ctx, "dir")
		assert.NoError(err)
		assert.Equal(1, len(all))

		_, err = all[0].AddFile(ctx, "foo", 5)
		assert.ErrorIs(err, errReadonly)

		err = all[0].AbortBackup(ctx)
		assert.ErrorIs(err, errReadonly)

		err = all[0].EndBackup(ctx)
		assert.ErrorIs(err, errReadonly)
	})

	t.Run("list excluded", func(t *testing.T) {
		ctx := context.Background()
		assert := require.New(t)
		storage := setupStorage(t)

		for _, space := range []string{"a", "b", "c"} {
			dir := path.Join(space, "dir")
			makeBackup(t, storage, dir, "backup")
		}

		for _, excluded := range []string{"a", "b"} {
			dir := path.Join(excluded, "dir")
			all, err := storage.ListBackups(ctx, dir)
			assert.NoError(err)
			assert.Equal(0, len(all))
		}

		all, err := storage.ListBackups(ctx, "c/dir")
		assert.NoError(err)
		assert.Equal(1, len(all))
	})

	t.Run("list empty", func(t *testing.T) {
		ctx := context.Background()
		assert := require.New(t)
		storage := setupStorage(t)

		all, err := storage.ListBackups(ctx, "dir")
		assert.NoError(err)
		assert.Equal(0, len(all))
	})
}

func makeBackup(t testing.TB, s *Storage, dir, name string) {
	t.Helper()

	ctx := context.Background()
	data := []byte("data")

	b, err := s.StartBackup(ctx, dir, name)
	if err != nil {
		t.Fatalf("start backup: %s", err)
	}

	w, err := b.AddFile(ctx, "test", int64(len(data)))
	if err != nil {
		t.Fatalf("add file: %s", err)
	}

	_, err = w.Write(data)
	if err != nil {
		t.Fatalf("write: %s", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}

	if err := b.EndBackup(ctx); err != nil {
		t.Fatalf("end backup: %s", err)
	}
}

// The function will setup a new storage.
//
// It ensures that the bucket is emptied before each test.
// This isn't fast but ensures that the all tests have a clean state.
func setupStorage(t testing.TB) *Storage {
	t.Helper()

	const (
		bucket = "gcsbackup-test"
		keyURI = "projects/planetscale-gcsbackup-test/locations/global/keyRings/gcsbackup-test/cryptoKeys/test"
	)

	// Timeout if setup fails.
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client, err := storage.NewClient(ctx,
		option.WithCredentialsFile(*credsFile),
	)
	if err != nil {
		t.Fatalf("new client: %s", err)
	}

	b := client.Bucket(bucket)
	iter := b.Objects(ctx, nil)

	for {
		attrs, err := iter.Next()

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			t.Fatalf("next: %s", err)
		}

		if attrs.Name == "" {
			t.Fatalf("received an object with empty name: %+v", attrs)
		}

		if err := b.Object(attrs.Name).Delete(ctx); err != nil {
			t.Fatalf("object delete: %s", err)
		}
	}

	return &Storage{
		Bucket:    bucket,
		CredsPath: *credsFile,
		KeyURI:    keyURI,
		loader: func(string) (*labels, error) {
			return &labels{
				BackupID:                    "backup-id",
				LastBackupID:                "backup-id",
				LastBackupExcludedKeyspaces: []string{"a", "b"},
			}, nil
		},
	}
}
