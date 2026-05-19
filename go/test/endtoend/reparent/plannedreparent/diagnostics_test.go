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

package plannedreparent

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

const (
	demotePrimaryDiagnosticsMarker = "timed out or canceled while enabling super_read_only during DemotePrimary, dumping MySQL diagnostics"
	demotePrimaryDiagnosticQuery   = "collecting DemotePrimary MySQL diagnostic query"
)

// TestPlannedReparentDumpsDiagnosticsWhenSuperReadOnlyStalls verifies the real PRS path logs
// diagnostics when MySQL is blocked on the super_read_only change.
func TestPlannedReparentDumpsDiagnosticsWhenSuperReadOnlyStalls(t *testing.T) {
	vttabletMajorVersion, err := cluster.GetMajorVersion("vttablet")
	require.NoError(t, err)

	if vttabletMajorVersion < 25 {
		t.Skipf("skipping diagnostic log assertion with old vttablet: vttablet major version %d", vttabletMajorVersion)
	}

	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)

	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	oldPrimary := tablets[0]
	newPrimary := tablets[1]

	lockPrimaryTestTableForWrite(t, oldPrimary)

	out, err := utils.Prs(t, clusterInstance, newPrimary)
	require.Error(t, err)
	require.Contains(t, out, "DeadlineExceeded")

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logContents, err := os.ReadFile(oldPrimary.VttabletProcess.ErrorLog)
		require.NoError(c, err)

		assert.Contains(c, string(logContents), demotePrimaryDiagnosticsMarker)
		assert.Contains(c, string(logContents), demotePrimaryDiagnosticQuery)
	}, 30*time.Second, 250*time.Millisecond)
}

// lockPrimaryTestTableForWrite holds a table lock that stalls MySQL while it
// enables super_read_only.
func lockPrimaryTestTableForWrite(t *testing.T, tablet *cluster.Vttablet) {
	t.Helper()

	socketPath := filepath.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d/mysql.sock", tablet.TabletUID))
	connParams := mysql.ConnParams{Uname: "vt_dba", DbName: "vt_" + utils.KeyspaceName, UnixSocket: socketPath}
	conn, err := mysql.Connect(t.Context(), &connParams)
	require.NoError(t, err)

	_, err = conn.ExecuteFetch("lock tables vt_insert_test write", 0, false)
	require.NoError(t, err)

	var once sync.Once
	unlock := func() {
		once.Do(func() {
			_, _ = conn.ExecuteFetch("unlock tables", 0, false)
			conn.Close()
		})
	}

	t.Cleanup(unlock)
}
