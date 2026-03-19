/*
Copyright 2025 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testhelper

import (
	"os"
	"strings"
	"testing"
)

var s3EnvRequired = []string{
	"AWS_ACCESS_KEY_ID",
	"AWS_SECRET_ACCESS_KEY",
	"AWS_BUCKET",
	"AWS_ENDPOINT",
	"AWS_REGION",
}

// S3ConfigAvailable returns true if all five AWS_* env vars are set (e.g. by MicroCeph).
func S3ConfigAvailable() bool {
	for _, name := range s3EnvRequired {
		if os.Getenv(name) == "" {
			return false
		}
	}
	return true
}

// S3Config holds the five AWS_* env vars required for S3 backup tests.
type S3Config struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
}

// RequireS3Config checks all five AWS_* env vars. If any are missing, skips the
// test with a message listing what to set. If all are present, returns the
// populated S3Config.
func RequireS3Config(t *testing.T) S3Config {
	t.Helper()
	var missing []string
	for _, name := range s3EnvRequired {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Skipf("missing AWS secrets to run this test: please set: %s", strings.Join(missing, ", "))
	}
	return S3Config{
		Endpoint:  os.Getenv("AWS_ENDPOINT"),
		Bucket:    os.Getenv("AWS_BUCKET"),
		AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		Region:    os.Getenv("AWS_REGION"),
	}
}

// S3TabletArgs returns vttablet ExtraArgs for S3 backup storage. Append these
// to tablet.VttabletProcess.ExtraArgs when using MicroCeph or another S3 backend.
func S3TabletArgs(cfg S3Config) []string {
	return append(
		[]string{"--backup_storage_implementation", "s3"},
		S3StorageExtraArgs(cfg)...,
	)
}

// S3StorageExtraArgs returns the S3 connection flags (endpoint, bucket, etc.)
// without the implementation flag. Use with vtbackup when BackupStorageImplementation
// is already set to "s3".
func S3StorageExtraArgs(cfg S3Config) []string {
	return []string{
		"--s3-backup-aws-endpoint", cfg.Endpoint,
		"--s3-backup-storage-bucket", cfg.Bucket,
		"--s3-backup-aws-region", cfg.Region,
		"--s3-backup-force-path-style=true",
		"--s3-backup-tls-skip-verify-cert=true",
	}
}

// SetS3Env sets AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the test environment.
// The vttablet and vtbackup processes inherit the parent env via os.Environ(), so
// these credentials will be available to the AWS SDK in child processes. Uses
// t.Setenv to restore original values when the test ends.
func SetS3Env(t *testing.T, cfg S3Config) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", cfg.AccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", cfg.SecretKey)
}
