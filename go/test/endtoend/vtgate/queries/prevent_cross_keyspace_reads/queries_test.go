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

package preventcrosskeyspacereads

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
	vtutils "vitess.io/vitess/go/vt/utils"
)

func start(t *testing.T) *mysql.Conn {
	t.Helper()
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// TestPreventCrossKeyspaceReadsVSchemaSetting tests that the prevent_cross_keyspace_reads
// vschema keyspace setting denies cross-keyspace joins.
func TestPreventCrossKeyspaceReadsVSchemaSetting(t *testing.T) {
	conn := start(t)

	// Cross-keyspace join should fail because ks1 has prevent_cross_keyspace_reads: true in its vschema.
	_, err := utils.ExecAllowError(t, conn, "select * from ks1.t1 join ks2.t2 on ks1.t1.id = ks2.t2.id")
	require.ErrorContains(t, err, "cross-keyspace JOIN")

	// The ALLOW_CROSS_KEYSPACE_READS directive should override the restriction.
	_ = utils.Exec(t, conn, "select /*vt+ ALLOW_CROSS_KEYSPACE_READS */ * from ks1.t1 join ks2.t2 on ks1.t1.id = ks2.t2.id")
}

// TestPreventCrossKeyspaceReadsUnqualifiedTables tests that the prevent_cross_keyspace_reads
// check fires even when table names are not qualified with a keyspace prefix,
// relying on global routing to resolve unique table names to their keyspaces.
func TestPreventCrossKeyspaceReadsUnqualifiedTables(t *testing.T) {
	conn := start(t)

	// Unqualified cross-keyspace join should fail — t1 routes to ks1, t2 routes to ks2.
	_, err := utils.ExecAllowError(t, conn, "select * from t1 join t2 on t1.id = t2.id")
	require.ErrorContains(t, err, "cross-keyspace JOIN")

	// The ALLOW_CROSS_KEYSPACE_READS directive should override the restriction.
	_ = utils.Exec(t, conn, "select /*vt+ ALLOW_CROSS_KEYSPACE_READS */ * from t1 join t2 on t1.id = t2.id")

	// Unqualified cross-keyspace UNION should also fail.
	_, err = utils.ExecAllowError(t, conn, "select id from t1 union all select id from t2")
	require.ErrorContains(t, err, "cross-keyspace UNION")

	// The ALLOW_CROSS_KEYSPACE_READS directive should override the restriction.
	_ = utils.Exec(t, conn, "select /*vt+ ALLOW_CROSS_KEYSPACE_READS */ id from t1 union all select id from t2")
}

// TestPreventCrossKeyspaceReadsVSchemaSettingUnion tests that the prevent_cross_keyspace_reads
// vschema keyspace setting also denies cross-keyspace UNIONs.
func TestPreventCrossKeyspaceReadsVSchemaSettingUnion(t *testing.T) {
	conn := start(t)

	// Cross-keyspace UNION should fail because ks1 has prevent_cross_keyspace_reads: true in its vschema.
	_, err := utils.ExecAllowError(t, conn, "select id from ks1.t1 union all select id from ks2.t2")
	require.ErrorContains(t, err, "cross-keyspace UNION")

	// The ALLOW_CROSS_KEYSPACE_READS directive should override the restriction.
	_ = utils.Exec(t, conn, "select /*vt+ ALLOW_CROSS_KEYSPACE_READS */ id from ks1.t1 union all select id from ks2.t2")
}

// TestPreventCrossKeyspaceReadsVTGateFlag tests that the --prevent-cross-keyspace-reads
// vtgate flag denies cross-keyspace joins globally.
func TestPreventCrossKeyspaceReadsVTGateFlag(t *testing.T) {
	// Restart vtgate with the --prevent-cross-keyspace-reads flag.
	originalArgs := clusterInstance.VtGateExtraArgs
	clusterInstance.VtGateExtraArgs = append(slices.Clone(originalArgs),
		vtutils.GetFlagVariantForTests("--prevent-cross-keyspace-reads")+"=true")
	require.NoError(t, clusterInstance.RestartVtgate())
	vtParams = mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}
	t.Cleanup(func() {
		clusterInstance.VtGateExtraArgs = originalArgs
		require.NoError(t, clusterInstance.RestartVtgate())
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
	})

	conn := start(t)

	// Cross-keyspace join should fail because the vtgate flag is set.
	_, err := utils.ExecAllowError(t, conn, "select * from ks1.t1 join ks2.t2 on ks1.t1.id = ks2.t2.id")
	require.ErrorContains(t, err, "cross-keyspace JOIN")

	// The ALLOW_CROSS_KEYSPACE_READS directive should override the flag.
	_ = utils.Exec(t, conn, "select /*vt+ ALLOW_CROSS_KEYSPACE_READS */ * from ks1.t1 join ks2.t2 on ks1.t1.id = ks2.t2.id")
}
