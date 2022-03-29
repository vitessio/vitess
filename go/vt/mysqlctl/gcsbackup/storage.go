package gcsbackup

import (
	"context"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	cloudkms "google.golang.org/api/cloudkms/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	// AnnotationsPath is the annotations file path to read
	// labels from.
	annotationsPath = "ANNOTATIONS_FILE_PATH"
)

// Storage implements the backup storage.
type Storage struct {
	// Bucket is the bucket name to use.
	//
	// If empty, all storage methods return an error.
	Bucket string

	// CredsPath is the GCP service account credentials.json
	//
	// If empty, all storage methods return an error.
	CredsPath string

	// KeyURI is the key is the keyring URI to use.
	//
	// If empty, all storage methods return an error.
	KeyURI string

	// Loader is a labels loader to use.
	//
	// By default the internal `load` is used which will load
	// all necessary labels from k8s annotations.
	//
	// It is overriden in tests.
	loader func(path string) (*labels, error)

	// Client and bucket are initialized once, if initialization
	// fails the error is stored and returned from all methods.
	client *storage.Client
	kms    *kms
	bucket *storage.BucketHandle
	once   sync.Once
	err    error
}

// Init initializes the storage.
func (s *Storage) init(ctx context.Context) error {
	s.once.Do(func() {
		if s.CredsPath == "" {
			s.err = errors.New("empty credentials path")
			return
		}

		if s.Bucket == "" {
			s.err = errors.New("empty bucket name")
			return
		}

		if s.KeyURI == "" {
			s.err = errors.New("empty key uri")
			return
		}

		c, err := storage.NewClient(ctx,
			option.WithCredentialsFile(s.CredsPath),
		)
		if err != nil {
			s.err = err
			return
		}

		service, err := cloudkms.NewService(ctx,
			option.WithCredentialsFile(s.CredsPath),
		)
		if err != nil {
			s.err = err
		}

		if s.loader == nil {
			s.loader = load
		}

		s.kms = newKMS(s.KeyURI, service)
		s.client = c
		s.bucket = c.Bucket(s.Bucket)
	})
	return s.err
}

// ListBackups lists backups in a directory.
func (s *Storage) ListBackups(ctx context.Context, dir string) ([]backupstorage.BackupHandle, error) {
	if err := s.init(ctx); err != nil {
		return nil, err
	}

	labels, err := s.labels()
	if err != nil {
		return nil, err
	}

	if labels.LastBackupID == "" {
		return nil, nil
	}

	parts := strings.Split(dir, "/")
	if len(parts) == 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid backup directory: %v", dir)
	}

	keyspace := parts[0]
	for _, excluded := range labels.LastBackupExcludedKeyspaces {
		if excluded == keyspace {
			return nil, nil
		}
	}

	iter := s.bucket.Objects(ctx, &storage.Query{
		Delimiter: "/",
		Prefix:    path.Join(labels.LastBackupID, dir) + "/",
	})

	var names []string

	for {
		attrs, err := iter.Next()

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, vterrors.Wrap(err, "iterating over objects")
		}

		name := path.Base(attrs.Prefix)
		names = append(names, name)
	}

	sort.Strings(names)
	ret := make([]backupstorage.BackupHandle, 0, len(names))

	for _, name := range names {
		h := newHandle(s.bucket, s.kms, labels.LastBackupID, dir, name)
		ret = append(ret, h.readonly())
	}

	return ret, nil
}

// StartBackup creates a new backup with the given name at dir.
func (s *Storage) StartBackup(ctx context.Context, dir, name string) (backupstorage.BackupHandle, error) {
	if err := s.init(ctx); err != nil {
		return nil, err
	}

	labels, err := s.labels()
	if err != nil {
		return nil, err
	}

	if labels.BackupID == "" {
		return nil, errors.New("gcsbackup: missing backup-id annotation")
	}

	return newHandle(s.bucket, s.kms, labels.BackupID, dir, name), nil
}

// RemoveBackup implementation.
//
// This is a no-op because singularity is in charge of removing backups.
func (s *Storage) RemoveBackup(ctx context.Context, dir, name string) error {
	if err := s.init(ctx); err != nil {
		return err
	}
	return nil
}

// Labels reads and returns labels from the annotations filepath.
func (s *Storage) labels() (*labels, error) {
	return s.loader(os.Getenv(annotationsPath))
}

// Close closes the storage.
func (s *Storage) Close() error {
	return nil
}
