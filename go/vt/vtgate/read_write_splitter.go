/*
Copyright 2021 The Vitess Authors.

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
package vtgate

import (
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func suggestTabletType(readWriteSplittingPolicy string, inTransaction, hasCreatedTempTables, hasAdvisoryLock bool, sql string) (tabletType topodatapb.TabletType, err error) {
	suggestedTabletType := defaultTabletType
	if schema.ReadWriteSplittingPolicy(readWriteSplittingPolicy) == schema.ReadWriteSplittingPolicyDisable {
		return suggestedTabletType, nil
	}
	if inTransaction || hasCreatedTempTables || hasAdvisoryLock {
		return suggestedTabletType, nil
	}
	// if not in transaction, and the query is read-only, use REPLICA
	ro, err := IsReadOnly(sql)
	if err != nil {
		return suggestedTabletType, err
	}
	if ro {
		suggestedTabletType = topodatapb.TabletType_REPLICA
	}
	return suggestedTabletType, nil
}

// IsReadOnly : whether the query should be routed to a read-only vttablet
func IsReadOnly(query string) (bool, error) {
	s, _, err := sqlparser.Parse2(query)
	if err != nil {
		return false, err
	}
	// select last_insert_id() is a special case, it's not a read-only query
	if sqlparser.ContainsLastInsertIDStatement(s) {
		return false, nil
	}
	// GET_LOCK/RELEASE_LOCK/IS_USED_LOCK/RELEASE_ALL_LOCKS is a special case, it's not a read-only query
	if sqlparser.ContainsLockStatement(s) {
		return false, nil
	}
	// if HasSystemTable
	if HasSystemTable(s, "") {
		return false, nil
	}
	return sqlparser.IsPureSelectStatement(s), nil
}

func HasSystemTable(sel sqlparser.Statement, ksName string) bool {
	semTable, err := semantics.Analyze(sel, ksName, &semantics.FakeSI{})
	if err != nil {
		return false
	}
	for _, tableInfo := range semTable.Tables {
		if tableInfo.IsInfSchema() {
			return true
		}
	}
	return false
}
