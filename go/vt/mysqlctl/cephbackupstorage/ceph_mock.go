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

package cephbackupstorage

// FakeConfig is the configuration a test supplies in place of the JSON file
// named by --ceph-backup-storage-config.
type FakeConfig struct {
	AccessKey string
	SecretKey string
	// EndPoint is host:port with no scheme, as in the config file.
	EndPoint string
	UseSSL   bool
}

// NewFakeCephBackupStorage returns a CephBackupStorage that uses cfg instead
// of reading the config file. Tests only.
func NewFakeCephBackupStorage(cfg FakeConfig) *CephBackupStorage {
	return &CephBackupStorage{
		config: &storageConfig{
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
			EndPoint:  cfg.EndPoint,
			UseSSL:    cfg.UseSSL,
		},
	}
}
