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

package mysqlctl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
)

// TestStartGroupReplicationBootstrapTimeoutResetsFlag checks that a bootstrap whose START
// GROUP_REPLICATION outlives the caller's context still turns group_replication_bootstrap_group
// off. The killed statement, or any later START GROUP_REPLICATION, would otherwise create a new
// group.
func TestStartGroupReplicationBootstrapTimeoutResetsFlag(t *testing.T) {
	const (
		bootstrapOn  = "SET GLOBAL group_replication_bootstrap_group = ON"
		bootstrapOff = "SET GLOBAL group_replication_bootstrap_group = OFF"
	)
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cp := *params
	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery(bootstrapOn, &sqltypes.Result{})
	db.AddQuery(bootstrapOff, &sqltypes.Result{})
	db.AddQueryPattern("kill .*", &sqltypes.Result{})
	// The join outlives the caller's context, as when no seed answers.
	db.AddQueryPatternWithCallback("START GROUP_REPLICATION.*", &sqltypes.Result{}, func(string) {
		time.Sleep(500 * time.Millisecond)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := mysqld.StartGroupReplication(ctx, true)
	require.Error(t, err)
	assert.Equal(t, 1, db.GetQueryCalledNum(bootstrapOn))
	assert.Equal(t, 1, db.GetQueryCalledNum(bootstrapOff), "group_replication_bootstrap_group must be reset after a failed bootstrap")
}
