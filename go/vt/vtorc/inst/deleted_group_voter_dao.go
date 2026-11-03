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

package inst

import (
	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/db"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// A voter of a shard's replication group whose tablet record was deleted keeps its tablet record and
// its instance in VTOrc's backend for as long as it is listed: VTOrc keeps discovering it at the
// address it last knew, and the voter changes need its server_uuid, its address and when VTOrc last
// reached it (see PlanGroupVoters). vitess_deleted_group_voter lists such tablets; every other tablet
// whose record is gone is forgotten.

// MarkDeletedGroupVoter records that the tablet record of the voter was deleted, and that VTOrc keeps
// what it knows about the voter.
func MarkDeletedGroupVoter(alias *topodatapb.TabletAlias) error {
	_, err := db.ExecVTOrc(`INSERT OR IGNORE INTO vitess_deleted_group_voter (alias) VALUES (?)`, topoproto.TabletAliasString(alias))
	return err
}

// UnmarkDeletedGroupVoter removes the mark of MarkDeletedGroupVoter: the tablet has a record again, or
// is forgotten.
func UnmarkDeletedGroupVoter(alias *topodatapb.TabletAlias) error {
	_, err := db.ExecVTOrc(`DELETE FROM vitess_deleted_group_voter WHERE alias = ?`, topoproto.TabletAliasString(alias))
	return err
}

// ReadDeletedGroupVoters returns the aliases of the voters whose tablet record was deleted, and that
// VTOrc keeps.
func ReadDeletedGroupVoters() (map[string]bool, error) {
	deleted := make(map[string]bool)
	err := db.QueryVTOrc(`SELECT alias FROM vitess_deleted_group_voter`, nil, func(row sqlutils.RowMap) error {
		deleted[row.GetString("alias")] = true
		return nil
	})
	return deleted, err
}
