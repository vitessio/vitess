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
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// queryThrottlerStatementTypes are the statement types a vttablet query plan can report
// (planbuilder.Build and BuildStreaming), so the only ones a query throttler rule can match.
// Statements such as BEGIN, USE or LOCK TABLES never reach the query throttler.
var queryThrottlerStatementTypes = func() map[string]struct{} {
	names := make(map[string]struct{})
	for _, s := range []sqlparser.StatementType{
		sqlparser.StmtSelect,
		sqlparser.StmtInsert,
		sqlparser.StmtReplace,
		sqlparser.StmtUpdate,
		sqlparser.StmtDelete,
		sqlparser.StmtDDL,
		sqlparser.StmtMigration,
		sqlparser.StmtSet,
		sqlparser.StmtShow,
		sqlparser.StmtOther,
		sqlparser.StmtAnalyze,
		sqlparser.StmtExplain,
		sqlparser.StmtSavepoint,
		sqlparser.StmtSRollback,
		sqlparser.StmtRelease,
		sqlparser.StmtUnlockTables,
		sqlparser.StmtFlush,
		sqlparser.StmtCallProc,
	} {
		names[s.String()] = struct{}{}
	}
	return names
}()

// IsQueryThrottlerStatementType reports whether a query throttler rule keyed by name can
// ever match a statement.
func IsQueryThrottlerStatementType(name string) bool {
	_, ok := queryThrottlerStatementTypes[name]
	return ok
}

// IsQueryThrottlerTabletType reports whether a query throttler rule keyed by name can ever
// match a tablet. Only serving tablet types run queries, and requiring an exact String()
// round trip rejects typos ("PRIMAY"), wrong case ("primary") and aliases ("MASTER").
func IsQueryThrottlerTabletType(name string) bool {
	v, ok := topodatapb.TabletType_value[name]
	if !ok {
		return false
	}
	tabletType := topodatapb.TabletType(v)
	return tabletType.String() == name && topoproto.IsServingType(tabletType)
}
