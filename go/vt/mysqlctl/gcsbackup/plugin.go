// Package gcsbackup implements Vitess backup plugin over GCS.
//
// The package uses the Storage SDK and Tink SDK to perform client side
// envelope encryption for the backup while storing it in GCS.
package gcsbackup

import (
	"flag"

	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

// The only exported API we expose to Vitess is kmsbackup.Plugin. Ensure we
// properly implement its backup interfaces. All other logic lives in this
// repository.
var (
	_ backupstorage.BackupHandle  = (*handle)(nil)
	_ backupstorage.BackupStorage = (*Storage)(nil)
)

// Init registers the backup plugin.
func init() {
	backupstorage.BackupStorageMap["gcsbackup"] = plugin()
}

// Plugin creates the gcsbackup plugin.
func plugin() backupstorage.BackupStorage {
	s := &Storage{}
	flag.StringVar(&s.Bucket, "psdb.gcs_backup.bucket", "", "GCS bucket to use for backups.")
	flag.StringVar(&s.CredsPath, "psdb.gcs_backup.creds_path", "", "Credentials JSON for service account to use.")
	flag.StringVar(&s.KeyURI, "psdb.gcs_backup.key_uri", "", "GCP KMS Keyring URI to use.")
	return s
}
