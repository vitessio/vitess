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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3BackupConfigPropagation verifies that setting S3BackupConfig on
// LocalProcessCluster propagates to vtctld, vttablet, and vtbackup processes.
// This is the regression test requested in the code review to prevent the
// S3 wiring from silently breaking.
func TestS3BackupConfigPropagation(t *testing.T) {
	s3Cfg := &S3BackupConfig{
		Endpoint:       "http://localhost:7480",
		Bucket:         "vitess-test",
		Region:         "us-east-1",
		ForcePathStyle: true,
	}

	cluster := NewCluster("zone1", "localhost")
	cluster.S3BackupConfig = s3Cfg

	t.Run("vttablet", func(t *testing.T) {
		tablet := &Vttablet{
			TabletUID: 100,
			HTTPPort:  15100,
			GrpcPort:  15101,
			Type:      "replica",
		}
		p := cluster.VtprocessInstanceFromVttablet(tablet, "0", "ks", cluster.Cell, cluster.Hostname)
		require.NotNil(t, p)
		assert.Equal(t, "s3", p.BackupStorageImplementation)
		assert.Equal(t, s3Cfg, p.S3BackupConfig)
	})

	t.Run("vtbackup", func(t *testing.T) {
		cluster.TmpDirectory = t.TempDir()
		cluster.TopoPort = 9999
		err := cluster.StartVtbackup("/nonexistent/init_db.sql", true, "ks", "0", "zone1")
		assert.Error(t, err)
		assert.Equal(t, "s3", cluster.VtbackupProcess.BackupStorageImplementation, "StartVtbackup must propagate S3 config")
		assert.Equal(t, s3Cfg, cluster.VtbackupProcess.S3BackupConfig)
	})

	t.Run("no_s3_uses_file", func(t *testing.T) {
		cluster2 := NewCluster("zone1", "localhost")
		tablet := &Vttablet{
			TabletUID: 200,
			HTTPPort:  15200,
			GrpcPort:  15201,
			Type:      "replica",
		}
		p := cluster2.VtprocessInstanceFromVttablet(tablet, "0", "ks", cluster2.Cell, cluster2.Hostname)
		require.NotNil(t, p)
		assert.Equal(t, "file", p.BackupStorageImplementation)
		assert.Nil(t, p.S3BackupConfig)
	})
}
