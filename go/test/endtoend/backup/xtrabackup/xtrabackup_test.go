/*
Copyright 2019 The Vitess Authors.

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

package vtctlbackup

import (
	"fmt"
	"testing"

	backup "vitess.io/vitess/go/test/endtoend/backup/vtctlbackup"
)

// TestXtraBackup - tests the back using xtrabackup arguments
func TestXtraBackup(t *testing.T) {
	code, err := backup.LaunchCluster(true)
	fmt.Println(err)
	if err != nil {
		t.Errorf("setup failed with status code %d", code)
	}

	// Run all the backup tests
	backup.TestBackup(t)

	// Teardown the cluster
	backup.TearDownCluster()
}
