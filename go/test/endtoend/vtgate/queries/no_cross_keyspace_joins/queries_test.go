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

package nocrosskeyspacejoins

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
	vtutils "vitess.io/vitess/go/vt/utils"
)

func start(t *testing.T) (*mysql.Conn, func()) {
	t.Helper()
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	return conn, func() { conn.Close() }
}

// TestNoCrossKeyspaceJoinsVSchemaSettting tests that the no_cross_keyspace_joins
// vschema keyspace setting denies cross-keyspace joins.
func TestNoCrossKeyspaceJoinsVSchemaSetting(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	// Cross-keyspace join should fail because ks1 has no_cross_keyspace_joins: true in its vschema.
	_, err := utils.ExecAllowError(t, conn, "select * from ks1.t1 join ks2.t2 on ks1.t1.id = ks2.t2.id")
	require.ErrorContains(t, err, "cross-keyspace join")

	// The ALLOW_CROSS_KEYSPACE_JOINS directive should override the restriction.
	_ = utils.Exec(t, conn, "select /*vt+ ALLOW_CROSS_KEYSPACE_JOINS */ * from ks1.t1 join ks2.t2 on ks1.t1.id = ks2.t2.id")
}

// TestNoCrossKeyspaceJoinsVTGateFlag tests that the --no-cross-keyspace-joins
// vtgate flag denies cross-keyspace joins globally.
func TestNoCrossKeyspaceJoinsVTGateFlag(t *testing.T) {
	// Restart vtgate with the --no-cross-keyspace-joins flag.
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
		vtutils.GetFlagVariantForTests("--no-cross-keyspace-joins")+"=true")
	require.NoError(t, clusterInstance.RestartVtgate())
	vtParams = mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}

	conn, closer := start(t)
	defer closer()

	// Cross-keyspace join should fail because the vtgate flag is set.
	_, err := utils.ExecAllowError(t, conn, "select * from ks1.t1 join ks2.t2 on ks1.t1.id = ks2.t2.id")
	require.ErrorContains(t, err, "cross-keyspace join")

	// The ALLOW_CROSS_KEYSPACE_JOINS directive should override the flag.
	_ = utils.Exec(t, conn, "select /*vt+ ALLOW_CROSS_KEYSPACE_JOINS */ * from ks1.t1 join ks2.t2 on ks1.t1.id = ks2.t2.id")
}
