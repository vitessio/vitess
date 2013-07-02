// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"code.google.com/p/vitess/go/vt/naming"
	"code.google.com/p/vitess/go/zk"
)

// Functions for extracting and deriving zk paths.

func VtRootFromTabletPath(zkTabletPath string) (string, error) {
	pathParts := strings.Split(zkTabletPath, "/")
	if len(pathParts) < 2 || pathParts[len(pathParts)-2] != "tablets" {
		return "", fmt.Errorf("invalid tablet path: %v", zkTabletPath)
	}

	return strings.Join(pathParts[:len(pathParts)-2], "/"), nil
}

func VtRootFromShardPath(zkShardPath string) (string, error) {
	pathParts := strings.Split(zkShardPath, "/")
	if len(pathParts) < 4 || pathParts[len(pathParts)-2] != "shards" || pathParts[len(pathParts)-4] != "keyspaces" {
		return "", fmt.Errorf("invalid shard path: %v", zkShardPath)
	}

	if pathParts[2] != "global" {
		return "", fmt.Errorf("invalid shard path - not global: %v", zkShardPath)
	}

	return strings.Join(pathParts[:len(pathParts)-4], "/"), nil
}

func VtRootFromKeyspacePath(zkKeyspacePath string) (string, error) {
	pathParts := strings.Split(zkKeyspacePath, "/")
	if len(pathParts) < 2 || pathParts[len(pathParts)-2] != "keyspaces" {
		return "", fmt.Errorf("invalid keyspace path: %v", zkKeyspacePath)
	}

	if pathParts[2] != "global" {
		return "", fmt.Errorf("invalid keyspace path - not global: %v", zkKeyspacePath)
	}

	return strings.Join(pathParts[:len(pathParts)-2], "/"), nil
}

// In most cases the substree is just "vt" - i.e. /zk/global/vt/keyspaces.
func VtSubtree(zkPath string) (string, error) {
	pathParts := strings.Split(zkPath, "/")
	for i, part := range pathParts {
		if i >= 3 && (part == "keyspaces" || part == "tablets") {
			return strings.Join(pathParts[3:i], "/"), nil
		}
	}
	return "", fmt.Errorf("invalid path: %v", zkPath)
}

// /zk/global/vt/keyspaces/<keyspace name>
func IsKeyspacePath(zkKeyspacePath string) error {
	_, err := VtRootFromKeyspacePath(zkKeyspacePath)
	return err
}

// /zk/<cell>/vt/tablets/<tablet uid>
func IsTabletPath(zkTabletPath string) error {
	_, err := VtRootFromTabletPath(zkTabletPath)
	return err
}

// This is the path that indicates the authoritive table node.
func TabletPath(zkVtRoot string, tabletUid uint32) string {
	tabletPath := path.Join(zkVtRoot, "tablets", tabletUidStr(tabletUid))
	if err := IsTabletPath(tabletPath); err != nil {
		panic(err) // this should never happen
	}
	return tabletPath
}

// /zk/<cell>/vt/tablets/<tablet uid>/action
func TabletActionPath(zkTabletPath string) (string, error) {
	if err := IsTabletPath(zkTabletPath); err != nil {
		return "", err
	}
	return path.Join(zkTabletPath, "action"), nil
}

// /zk/<cell>/vt/tablets/<tablet uid>/actionlog
func TabletActionLogPath(zkTabletPath string) (string, error) {
	if err := IsTabletPath(zkTabletPath); err != nil {
		return "", err
	}
	return path.Join(zkTabletPath, "actionlog"), nil
}

// From an action path and a filename, returns the actionlog path, e.g from
// /zk/<cell>/vt/tablets/<tablet uid>/action/0000001 it will return:
// /zk/<cell>/vt/tablets/<tablet uid>/actionlog/0000001
func ActionToActionLogPath(zkTabletActionPath string) string {
	return strings.Replace(zkTabletActionPath, "/action/", "/actionlog/", 1)
}

// /vt/keyspaces/<keyspace>/shards/<shard uid>
func IsShardPath(zkShardPath string) error {
	_, err := VtRootFromShardPath(zkShardPath)
	return err
}

// zkVtRoot: /zk/XX/vt
func ShardPath(zkVtRoot, keyspace, shard string) string {
	shardPath := path.Join("/zk/global/vt", "keyspaces", keyspace, "shards", shard)
	if err := IsShardPath(shardPath); err != nil {
		panic(err) // this should never happen
	}
	return shardPath
}

// zkShardPath: /zk/global/vt/keyspaces/XX/shards/YY
func ShardActionPath(zkShardPath string) (string, error) {
	if err := IsShardPath(zkShardPath); err != nil {
		return "", err
	}
	return path.Join(zkShardPath, "action"), nil
}

// zkShardPath: /zk/global/vt/keyspaces/XX/shards/YY
func ShardActionLogPath(zkShardPath string) (string, error) {
	if err := IsShardPath(zkShardPath); err != nil {
		return "", err
	}
	return path.Join(zkShardPath, "actionlog"), nil
}

// zkVtRoot: /zk/XX/vt
func KeyspacePath(zkVtRoot, keyspace string) string {
	keyspacePath := path.Join("/zk/global/vt", "keyspaces", keyspace)
	if err := IsKeyspacePath(keyspacePath); err != nil {
		panic(err) // this should never happen
	}
	return keyspacePath
}

// zkKeyspacePath: /zk/global/vt/keyspaces/XX
func KeyspaceActionPath(zkKeyspacePath string) (string, error) {
	if err := IsKeyspacePath(zkKeyspacePath); err != nil {
		return "", err
	}
	return path.Join(zkKeyspacePath, "action"), nil
}

// zkKeyspacePath: /zk/global/vt/keyspaces/XX
func KeyspaceActionLogPath(zkKeyspacePath string) (string, error) {
	if err := IsKeyspacePath(zkKeyspacePath); err != nil {
		return "", err
	}
	return path.Join(zkKeyspacePath, "actionlog"), nil
}

// zkKeyspacePath: /zk/global/vt/keyspaces/XX
func KeyspaceShardsPath(zkKeyspacePath string) (string, error) {
	if err := IsKeyspacePath(zkKeyspacePath); err != nil {
		return "", err
	}
	return path.Join(zkKeyspacePath, "shards"), nil
}

// Tablet aliases are the nodes that point into /vt/tablets/<uid> from the keyspace
// Note that these are *global*
func IsTabletReplicationPath(zkReplicationPath string) bool {
	_, _, err := ParseTabletReplicationPath(zkReplicationPath)
	return err == nil
}

func ParseTabletReplicationPath(zkReplicationPath string) (cell string, uid uint32, err error) {
	cell, err = zk.ZkCellFromZkPath(zkReplicationPath)
	if err != nil {
		return
	}
	if cell != "global" {
		return "", 0, fmt.Errorf("invalid replication path cell, expected global: %v", zkReplicationPath)
	}

	// /zk/cell/vt/keyspaces/<k>/shards/<s>/alias/...
	pathParts := strings.Split(zkReplicationPath, "/")
	shardIdx := -1
	for i, part := range pathParts {
		if part == "shards" {
			shardIdx = i
			break
		}
	}
	if shardIdx == -1 || shardIdx+2 >= len(pathParts) {
		return "", 0, fmt.Errorf("invalid replication path %v", zkReplicationPath)
	}

	nameParts := strings.Split(path.Base(zkReplicationPath), "-")
	if len(nameParts) != 2 {
		return "", 0, fmt.Errorf("invalid replication path %v", zkReplicationPath)
	}
	cell = nameParts[0]
	uid, err = ParseUid(nameParts[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid replication path uid %v: %v", zkReplicationPath, err)
	}
	return
}

func tabletUidStr(uid uint32) string {
	return fmt.Sprintf("%010d", uid)
}

func ParseUid(value string) (uint32, error) {
	uid, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad tablet uid %v", err)
	}
	return uint32(uid), nil
}

func fmtAlias(cell string, uid uint32) string {
	return fmt.Sprintf("%v-%v", cell, tabletUidStr(uid))
}

// FIXME(msolomon) This method doesn't take into account the vt subtree.
func TabletPathForAlias(alias naming.TabletAlias) string {
	return fmt.Sprintf("/zk/%v/vt/tablets/%v", alias.Cell, tabletUidStr(alias.Uid))
}

// Extract cell, uid and vt subtree from path and return a local tablet path.
func TabletPathFromReplicationPath(zkReplicationPath string) (string, error) {
	vtSubtree, err := VtSubtree(zkReplicationPath)
	if err != nil {
		return "", err
	}
	cell, uid, err := ParseTabletReplicationPath(zkReplicationPath)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("/zk/%v/%v/tablets/%v", cell, vtSubtree, tabletUidStr(uid)), nil
}

// zkActionPath is /zk/test/vt/tablets/<uid>/action/0000000001
func TabletPathFromActionPath(zkActionPath string) (string, error) {
	zkPathParts := strings.Split(zkActionPath, "/")
	if len(zkPathParts) < 2 || zkPathParts[len(zkPathParts)-2] != "action" {
		return "", fmt.Errorf("invalid action path: %v", zkActionPath)
	}
	tabletPath := strings.Join(zkPathParts[:len(zkPathParts)-2], "/")
	if err := IsTabletPath(tabletPath); err != nil {
		return "", err
	}
	return tabletPath, nil
}
