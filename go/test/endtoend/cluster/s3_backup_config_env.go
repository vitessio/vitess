/*
Copyright 2026 The Vitess Authors.

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

package cluster

import "os"

// S3BackupConfigFromEnv returns S3BackupConfig from AWS_* env vars when present (e.g. MicroCeph).
// Returns nil when AWS_ENDPOINT is unset.
// Credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) are passed through the process environment.
func S3BackupConfigFromEnv() *S3BackupConfig {
	endpoint := os.Getenv("AWS_ENDPOINT")
	if endpoint == "" {
		return nil
	}
	bucket := os.Getenv("AWS_BUCKET")
	region := os.Getenv("AWS_REGION")
	if bucket == "" || region == "" {
		return nil
	}
	return &S3BackupConfig{
		Endpoint:       endpoint,
		Bucket:         bucket,
		Region:         region,
		ForcePathStyle: true,
	}
}
