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

package chaos

import (
	"fmt"
	"os"
	"slices"
	"strings"
)

// Profile is a deployment configuration the scenarios run against. CHAOS_PROFILE selects it:
//
//	audit            (default) the failover audit's reference deployment: cross_cell, one etcd per
//	                 cell, one VTOrc per cell, Vitess's default my.cnf.
//	semisync-3vtorc  semi_sync durability, one VTOrc per cell (3), the cells' topos on the
//	                 global etcd, VTOrc with ERS and errant-GTID draining enabled, and the
//	                 mysqld settings of semiSyncMyCnf.
//	semisync-1vtorc  as semisync-3vtorc, with a single VTOrc in the first cell (zone1).
//
// CHAOS_VTORC_CELLS (comma separated) overrides the profile's VTOrc cells and CHAOS_PRIMARY_CELL
// moves the primary to a cell (with PRS) before every scenario, e.g. to put the single VTOrc of
// semisync-1vtorc next to the primary (zone1) or away from it (zone2).
type Profile struct {
	Name       string
	Durability string
	OrcCells   []string
	// PrimaryCell is the cell of the primary when the scenario starts; empty: any.
	PrimaryCell string
	// SharedCellTopo stores every cell's topo on the global etcd instead of one etcd per cell.
	SharedCellTopo  bool
	OrcExtraArgs    []string
	TabletExtraArgs []string
	// ExtraMyCnf is added to every mysqld's my.cnf (through EXTRA_MY_CNF).
	ExtraMyCnf string
}

func (p Profile) String() string {
	topo := "one etcd per cell"
	if p.SharedCellTopo {
		topo = "cell topos on the global etcd"
	}
	return fmt.Sprintf("%s: durability=%s vtorc-cells=%v primary-cell=%q %s", p.Name, p.Durability, p.OrcCells, p.PrimaryCell, topo)
}

// semiSyncMyCnf holds the mysqld settings of a durable semi-sync MySQL cluster that matter for
// replication and durability: binlog and InnoDB flushing on every commit, parallel replication, and
// AFTER_SYNC semi-sync that never falls back to asynchronous replication. It is layered on top of
// Vitess's default my.cnf, which keeps relay_log_recovery=1 and the semi-sync timeout of 1e18.
// Settings that MySQL 8.4 removed (binlog_transaction_dependency_tracking, whose WRITESET is the
// 8.4 behavior, and replica_parallel_type, whose LOGICAL_CLOCK is) are left out.
const semiSyncMyCnf = `
sync_binlog = 1
innodb_flush_log_at_trx_commit = 1
binlog_format = ROW
binlog_row_image = FULL
replica_parallel_workers = 20
replica_preserve_commit_order = ON
replica_net_timeout = 8
binlog_expire_logs_seconds = 259200
binlog_transaction_compression = ON
binlog_transaction_compression_level_zstd = 1
transaction_isolation = REPEATABLE-READ
character_set_server = utf8mb4
collation_server = utf8mb4_0900_ai_ci
loose_rpl_semi_sync_source_wait_point = AFTER_SYNC
loose_rpl_semi_sync_source_wait_for_replica_count = 1
loose_rpl_semi_sync_source_timeout = 1000000000000000000
loose_rpl_semi_sync_source_wait_no_replica = 1
`

var profiles = map[string]Profile{
	"audit": {
		Name:       "audit",
		Durability: "cross_cell",
		OrcCells:   cells,
	},
	"semisync-3vtorc": {
		Name:           "semisync-3vtorc",
		Durability:     "semi_sync",
		OrcCells:       cells,
		SharedCellTopo: true,
		OrcExtraArgs: []string{
			"--allow-emergency-reparent=true",
			"--change-tablets-with-errant-gtid-to-drained",
			"--clusters-to-watch", keyspaceName,
		},
		TabletExtraArgs: []string{"--queryserver-config-transaction-timeout", "20s"},
		ExtraMyCnf:      semiSyncMyCnf,
	},
}

func init() {
	r := profiles["semisync-3vtorc"]
	r.Name = "semisync-1vtorc"
	r.OrcCells = cells[:1]
	profiles["semisync-1vtorc"] = r
}

// SelectedProfile returns the profile selected by CHAOS_PROFILE, CHAOS_VTORC_CELLS and
// CHAOS_PRIMARY_CELL.
func SelectedProfile() Profile {
	name := os.Getenv("CHAOS_PROFILE")
	if name == "" {
		name = "audit"
	}
	p, ok := profiles[name]
	if !ok {
		panic(fmt.Sprintf("unknown CHAOS_PROFILE %q", name))
	}
	if v := os.Getenv("CHAOS_VTORC_CELLS"); v != "" {
		p.OrcCells = strings.Split(v, ",")
	}
	if v := os.Getenv("CHAOS_PRIMARY_CELL"); v != "" {
		p.PrimaryCell = v
	}
	if os.Getenv("CHAOS_CELL_TOPO") == "per-cell" {
		p.SharedCellTopo = false
	}
	return p
}

// drainsErrantTablets reports whether VTOrc changes tablets with errant GTIDs to DRAINED.
func (p Profile) drainsErrantTablets() bool {
	return slices.Contains(p.OrcExtraArgs, "--change-tablets-with-errant-gtid-to-drained")
}
