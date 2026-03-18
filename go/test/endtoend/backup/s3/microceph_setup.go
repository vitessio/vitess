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

package s3

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// MicroCephConfig holds endpoint and credentials for a MicroCeph RGW (S3) instance.
// In CI the composite action provisions MicroCeph and sets AWS_*; tests only consume env.
type MicroCephConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	Region    string
}

// SkipIfMicroCephUnavailable returns MicroCeph config when AWS_ENDPOINT is set (e.g. by the setup-microceph action).
// Does not install, bootstrap, or destroy (only reads env). Skips the test when MicroCeph is not configured.
func SkipIfMicroCephUnavailable(t *testing.T) *MicroCephConfig {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT")
	if endpoint == "" {
		t.Skip("MicroCeph not configured (AWS_ENDPOINT unset)")
		return nil
	}
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	require.NotEmpty(t, accessKey, "AWS_ENDPOINT is set but AWS_ACCESS_KEY_ID is missing")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	require.NotEmpty(t, secretKey, "AWS_ENDPOINT is set but AWS_SECRET_ACCESS_KEY is missing")
	bucket := os.Getenv("AWS_BUCKET")
	require.NotEmpty(t, bucket, "AWS_ENDPOINT is set but AWS_BUCKET is missing")
	region := os.Getenv("AWS_REGION")
	require.NotEmpty(t, region, "AWS_ENDPOINT is set but AWS_REGION is missing")
	return &MicroCephConfig{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Bucket:    bucket,
		Region:    region,
	}
}
