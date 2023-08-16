/*
Copyright 2023 The Vitess Authors.

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

// Package mysqlctl_test is the blackbox tests for package mysqlctl.
package mysqlctl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestShouldDrainForBackupBuiltIn(t *testing.T) {
	be := &BuiltinBackupEngine{}

	assert.True(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "auto"}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "99ca8ed4-399c-11ee-861b-0a43f95f28a3:1-197"}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "MySQL56/99ca8ed4-399c-11ee-861b-0a43f95f28a3:1-197"}))
}
