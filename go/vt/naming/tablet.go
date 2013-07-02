// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package naming

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const (
	// According to docs, the tablet uid / (mysql server id) is uint32.
	// However, zero appears to be a sufficiently degenerate value to use
	// as a marker for not having a parent server id.
	// http://dev.mysql.com/doc/refman/5.1/en/replication-options.html
	NO_TABLET = 0
)

// TabletAlias is the minimum required information to locate a tablet.
//
// Tablets are really globally unique, but crawling every cell to find
// out where it lives is time consuming and expensive. This is only
// needed during complex operations.  Tablet cell assignments don't
// change that often, thus using a TabletAlias is efficient.
type TabletAlias struct {
	Cell string
	Uid  uint32
}

func (ta *TabletAlias) String() string {
	return fmtAlias(ta.Cell, ta.Uid)
}

func (ta *TabletAlias) TabletUidStr() string {
	return tabletUidStr(ta.Uid)
}

func ParseTabletAliasString(aliasStr string) (result TabletAlias, err error) {
	nameParts := strings.Split(aliasStr, "-")
	if len(nameParts) != 2 {
		err = fmt.Errorf("invalid tablet alias: %v", aliasStr)
		return
	}
	result.Cell = nameParts[0]
	result.Uid, err = parseUid(nameParts[1])
	if err != nil {
		err = fmt.Errorf("invalid tablet uid %v: %v", aliasStr, err)
		return
	}
	return
}

func tabletUidStr(uid uint32) string {
	return fmt.Sprintf("%010d", uid)
}

func parseUid(value string) (uint32, error) {
	uid, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad tablet uid %v", err)
	}
	return uint32(uid), nil
}

func fmtAlias(cell string, uid uint32) string {
	return fmt.Sprintf("%v-%v", cell, tabletUidStr(uid))
}

// TabletType is the main type for a tablet. It has an implication on:
// - the replication graph
// - the services run by vttablet on a tablet
// - the uptime expectancy
type TabletType string

const (
	// idle -  no keyspace, shard or type assigned
	TYPE_IDLE = TabletType("idle")

	// primary copy of data
	TYPE_MASTER = TabletType("master")

	// a slaved copy of the data ready to be promoted to master
	TYPE_REPLICA = TabletType("replica")

	// a slaved copy of the data for olap load patterns.
	// too many aliases for olap - need to pick one
	TYPE_RDONLY = TabletType("rdonly")
	TYPE_BATCH  = TabletType("batch")

	// a slaved copy of the data ready, but not serving query traffic
	// could be a potential master.
	TYPE_SPARE = TabletType("spare")

	// a slaved copy of the data ready, but not serving query traffic
	// implies something abnormal about the setup - don't consider it
	// a potential master and don't worry about lag when reparenting.
	TYPE_EXPERIMENTAL = TabletType("experimental")

	// a slaved copy of the data intentionally lagged for pseudo backup
	TYPE_LAG = TabletType("lag")
	// when a reparent occurs, the tablet goes into lag_orphan state until
	// it can be reparented properly
	TYPE_LAG_ORPHAN = TabletType("lag_orphan")

	// a slaved copy of the data that was serving but is now applying
	// a schema change. Will go bak to its serving type after the
	// upgrade
	TYPE_SCHEMA_UPGRADE = TabletType("schema_apply")

	// a slaved copy of the data, but offline to queries other than backup
	// replication sql thread may be stopped
	TYPE_BACKUP = TabletType("backup")

	// a slaved copy of the data, where mysqld is *not* running,
	// and we are serving our data files to clone slaves
	// use 'vtctl Snapshot -server-mode ...' to get in this mode
	// use 'vtctl SnapshotSourceEnd ...' to get out of this mode
	TYPE_SNAPSHOT_SOURCE = TabletType("snapshot_source")

	// A tablet that has not been in the replication graph and is restoring
	// from a snapshot.  idle -> restore -> spare
	TYPE_RESTORE = TabletType("restore")

	// a machine with data that needs to be wiped
	TYPE_SCRAP = TabletType("scrap")
)

var AllTabletTypes = []TabletType{TYPE_IDLE,
	TYPE_MASTER,
	TYPE_REPLICA,
	TYPE_RDONLY,
	TYPE_BATCH,
	TYPE_SPARE,
	TYPE_EXPERIMENTAL,
	TYPE_LAG,
	TYPE_LAG_ORPHAN,
	TYPE_SCHEMA_UPGRADE,
	TYPE_BACKUP,
	TYPE_SNAPSHOT_SOURCE,
	TYPE_RESTORE,
	TYPE_SCRAP,
}

var SlaveTabletTypes = []TabletType{
	TYPE_REPLICA,
	TYPE_RDONLY,
	TYPE_BATCH,
	TYPE_SPARE,
	TYPE_EXPERIMENTAL,
	TYPE_LAG,
	TYPE_LAG_ORPHAN,
	TYPE_SCHEMA_UPGRADE,
	TYPE_BACKUP,
	TYPE_SNAPSHOT_SOURCE,
	TYPE_RESTORE,
}

var AllTabletTypeStrings []string
var SlaveTabletTypeStrings []string

func makeTypeList(types []TabletType) []string {
	strs := make([]string, len(types))
	for i, t := range types {
		strs[i] = string(t)
	}
	sort.Strings(strs)
	return strs
}

func init() {
	AllTabletTypeStrings = makeTypeList(AllTabletTypes)
	SlaveTabletTypeStrings = makeTypeList(SlaveTabletTypes)
}

// IsTrivialTypeChange returns if this db type be trivially reassigned
// without changes to the replication graph
func IsTrivialTypeChange(oldTabletType, newTabletType TabletType) bool {
	switch oldTabletType {
	case TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH, TYPE_SPARE, TYPE_LAG, TYPE_LAG_ORPHAN, TYPE_BACKUP, TYPE_SNAPSHOT_SOURCE, TYPE_EXPERIMENTAL, TYPE_SCHEMA_UPGRADE:
		switch newTabletType {
		case TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH, TYPE_SPARE, TYPE_LAG, TYPE_LAG_ORPHAN, TYPE_BACKUP, TYPE_SNAPSHOT_SOURCE, TYPE_EXPERIMENTAL, TYPE_SCHEMA_UPGRADE:
			return true
		}
	case TYPE_SCRAP:
		return newTabletType == TYPE_IDLE
	case TYPE_RESTORE:
		switch newTabletType {
		case TYPE_SPARE, TYPE_IDLE:
			return true
		}
	}
	return false
}

// IsValidTypeChange returns if we should we allow this transition at
// all.  Most transitions are allowed, but some don't make sense under
// any circumstances. If a transistion could be forced, don't disallow
// it here.
func IsValidTypeChange(oldTabletType, newTabletType TabletType) bool {
	switch oldTabletType {
	case TYPE_SNAPSHOT_SOURCE:
		switch newTabletType {
		case TYPE_BACKUP, TYPE_SNAPSHOT_SOURCE:
			return false
		}
	}

	return true
}

// IsServingType returns if a tablet appears in the serving graph
func IsServingType(tt TabletType) bool {
	switch tt {
	case TYPE_MASTER, TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH:
		return true
	}
	return false
}

// IsInReplicationGraph returns if this tablet appears in the replication graph
// Only IDLE and SCRAP are not in the replication graph.
// The other non-obvious types are BACKUP, SNAPSHOT_SOURCE, RESTORE
// and LAG_ORPHAN: these have had a master at some point (or were the
// master), so they are in the graph.
func IsInReplicationGraph(tt TabletType) bool {
	switch tt {
	case TYPE_IDLE, TYPE_SCRAP:
		return false
	}
	return true
}

// IsSlaveType returns if this type should be connected to a master db
// and actively replicating?
// MASTER is not obviously (only support one level replication graph)
// IDLE and SCRAP are not either
// BACKUP, RESTORE, LAG_ORPHAN may or may not be, but we don't know for sure
func IsSlaveType(tt TabletType) bool {
	switch tt {
	case TYPE_MASTER, TYPE_IDLE, TYPE_SCRAP, TYPE_BACKUP, TYPE_RESTORE, TYPE_LAG_ORPHAN:
		return false
	}
	return true
}

// TabletState describe if the tablet is read-only or read-write.
type TabletState string

const (
	// The normal state for a master
	STATE_READ_WRITE = TabletState("ReadWrite")

	// The normal state for a slave, or temporarily a master. Not
	// to be confused with type, which implies a workload.
	STATE_READ_ONLY = TabletState("ReadOnly")
)
