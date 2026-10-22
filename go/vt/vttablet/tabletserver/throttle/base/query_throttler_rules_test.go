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

package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsQueryThrottlerStatementType(t *testing.T) {
	for _, name := range []string{
		"SELECT", "INSERT", "REPLACE", "UPDATE", "DELETE", "DDL", "MIGRATION", "SET", "SHOW", "OTHER",
		"ANALYZE", "EXPLAIN", "SAVEPOINT", "SAVEPOINT_ROLLBACK", "RELEASE", "UNLOCK_TABLES", "FLUSH", "CALL_PROC",
	} {
		assert.Truef(t, IsQueryThrottlerStatementType(name), "%q must be accepted", name)
	}

	// Real statement types that never reach the query throttler, typos, and wrong case.
	for _, name := range []string{
		"BEGIN", "COMMIT", "ROLLBACK", "USE", "LOCK_TABLES", "STREAM", "VSTREAM", "COMMENT_ONLY",
		"PREPARE", "EXECUTE", "DEALLOCATE", "KILL", "UNKNOWN", "SELEC", "select", "",
	} {
		assert.Falsef(t, IsQueryThrottlerStatementType(name), "%q must be rejected", name)
	}
}

func TestIsQueryThrottlerTabletType(t *testing.T) {
	for _, name := range []string{"PRIMARY", "REPLICA", "RDONLY", "EXPERIMENTAL"} {
		assert.Truef(t, IsQueryThrottlerTabletType(name), "%q must be accepted", name)
	}

	// Non-serving types, the BATCH and MASTER aliases, typos, and wrong case.
	for _, name := range []string{
		"UNKNOWN", "SPARE", "BACKUP", "RESTORE", "DRAINED", "BATCH", "MASTER", "PRIMAY", "primary", "",
	} {
		assert.Falsef(t, IsQueryThrottlerTabletType(name), "%q must be rejected", name)
	}
}
