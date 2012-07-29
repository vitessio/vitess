// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"code.google.com/p/vitess/go/zk"
)

/* Functions for extracting and deriving zk paths. */

func VtRootFromTabletPath(zkTabletPath string) string {
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Errorf("invalid tablet path: %v", e))
		}
	}()
	pathParts := strings.Split(zkTabletPath, "/")
	if pathParts[len(pathParts)-2] != "tablets" {
		panic(fmt.Errorf("invalid tablet path: %v", zkTabletPath))
	}

	return strings.Join(pathParts[:len(pathParts)-2], "/")
}

func VtRootFromShardPath(zkShardPath string) string {
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Errorf("invalid shard path: %v %v", zkShardPath, e))
		}
	}()

	pathParts := strings.Split(zkShardPath, "/")
	if pathParts[len(pathParts)-2] != "shards" || pathParts[len(pathParts)-4] != "keyspaces" {
		panic(fmt.Errorf("invalid shard path: %v", zkShardPath))
	}

	if pathParts[2] != "global" {
		panic(fmt.Errorf("invalid shard path - not global: %v", zkShardPath))
	}

	return strings.Join(pathParts[:len(pathParts)-4], "/")
}

func VtRootFromKeyspacePath(zkKeyspacePath string) string {
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Errorf("invalid keyspace path: %v %v", zkKeyspacePath, e))
		}
	}()

	pathParts := strings.Split(zkKeyspacePath, "/")
	if pathParts[len(pathParts)-2] != "keyspaces" {
		panic(fmt.Errorf("invalid keyspace path: %v", zkKeyspacePath))
	}

	if pathParts[2] != "global" {
		panic(fmt.Errorf("invalid keyspace path - not global: %v", zkKeyspacePath))
	}

	return strings.Join(pathParts[:len(pathParts)-2], "/")
}


// /zk/global/vt/keyspaces/<keyspace name>
func MustBeKeyspacePath(zkKeyspacePath string) {
	VtRootFromKeyspacePath(zkKeyspacePath)
}

// /zk/<cell>/vt/tablets/<tablet uid>
func MustBeTabletPath(zkTabletPath string) {
	VtRootFromTabletPath(zkTabletPath)
}

// This is the path that indicates the authoritive table node.
func TabletPath(zkVtRoot string, tabletUid uint) string {
	tabletPath := path.Join(zkVtRoot, "tablets", tabletUidStr(tabletUid))
	MustBeTabletPath(tabletPath)
	return tabletPath
}

func TabletActionPath(zkTabletPath string) string {
	MustBeTabletPath(zkTabletPath)
	return path.Join(zkTabletPath, "action")
}

// /vt/keyspaces/<keyspace>/shards/<shard uid>
func MustBeShardPath(zkShardPath string) {
	VtRootFromShardPath(zkShardPath)
}

// zkVtRoot: /zk/XX/vt
func ShardPath(zkVtRoot, keyspace, shard string) string {
	shardPath := path.Join("/zk/global/vt", "keyspaces", keyspace, "shards", shard)
	MustBeShardPath(shardPath)
	return shardPath
}

// zkShardPath: /zk/global/vt/keyspaces/XX/shards/YY
func ShardActionPath(zkShardPath string) string {
	MustBeShardPath(zkShardPath)
	return path.Join(zkShardPath, "action")
}

// FIXME(msolomon) this is really weak
// Tablet aliases are the nodes that point into /vt/tablets/<uid> from the keyspace
// Note that these are *global*
func IsTabletReplicationPath(zkReplicationPath string) bool {
	_, _, err := parseTabletReplicationPath(zkReplicationPath)
	return err == nil
}

func parseTabletReplicationPath(zkReplicationPath string) (cell string, uid uint, err error) {
	cell = zk.ZkCellFromZkPath(zkReplicationPath)
	if cell != "global" {
		cell = ""
		err = fmt.Errorf("invalid cell, expected global: %v", zkReplicationPath)
		return
	}
	nameParts := strings.Split(path.Base(zkReplicationPath), "-")
	if len(nameParts) != 2 {
		err = fmt.Errorf("invalid path %v", zkReplicationPath)
		return
	}
	cell = nameParts[0]
	_uid, err := strconv.ParseUint(nameParts[1], 10, 0)
	if err != nil {
		err = fmt.Errorf("invalid uid %v: %v", zkReplicationPath, err)
	}
	uid = uint(_uid)
	return
}

func ParseTabletReplicationPath(zkReplicationPath string) (cell string, uid uint) {
	cell, uid, err := parseTabletReplicationPath(zkReplicationPath)
	if err != nil {
		panic(err)
	}
	return
}

func tabletUidStr(uid uint) string {
	return fmt.Sprintf("%010d", uid)
}

func TabletPathForAlias(alias TabletAlias) string {
	return fmt.Sprintf("/zk/%v/vt/tablets/%v", alias.Cell, tabletUidStr(alias.Uid))
}

// zkActionPath is /zk/test/vt/tablets/<uid>/action/0000000001
func TabletPathFromActionPath(zkActionPath string) string {
	zkPathParts := strings.Split(zkActionPath, "/")
	if zkPathParts[len(zkPathParts)-2] != "action" {
		panic(fmt.Errorf("invalid action path: %v", zkActionPath))
	}
	tabletPath := strings.Join(zkPathParts[:len(zkPathParts)-2], "/")
	MustBeTabletPath(tabletPath)
	return tabletPath
}
