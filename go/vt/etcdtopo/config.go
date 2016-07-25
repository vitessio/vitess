// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"flag"
	"path"

	"github.com/youtube/vitess/go/flagutil"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
)

const (
	// Paths within the etcd keyspace.
	rootPath           = "/vt"
	cellsDirPath       = rootPath + "/cells"
	keyspacesDirPath   = rootPath + "/keyspaces"
	electionDirPath    = rootPath + "/election"
	tabletsDirPath     = rootPath + "/tablets"
	replicationDirPath = rootPath + "/replication"
	servingDirPath     = rootPath + "/ns"

	// Magic file names. Directories in etcd cannot have data. Files whose names
	// begin with '_' are hidden from directory listings.
	dataFilename             = "_Data"
	keyspaceFilename         = dataFilename
	shardFilename            = dataFilename
	tabletFilename           = dataFilename
	shardReplicationFilename = dataFilename
	srvKeyspaceFilename      = dataFilename

	vschemaFilename = "_VSchema"
)

var (
	globalAddrs flagutil.StringListValue
)

func init() {
	flag.Var(&globalAddrs, "etcd_global_addrs", "comma-separated list of addresses (http://host:port) for global etcd cluster")
}

func cellFilePath(cell string) string {
	return path.Join(cellsDirPath, cell)
}

func keyspaceDirPath(keyspace string) string {
	return path.Join(keyspacesDirPath, keyspace)
}

func keyspaceFilePath(keyspace string) string {
	return path.Join(keyspaceDirPath(keyspace), keyspaceFilename)
}

func vschemaFilePath(keyspace string) string {
	return path.Join(keyspaceDirPath(keyspace), vschemaFilename)
}

func shardsDirPath(keyspace string) string {
	return keyspaceDirPath(keyspace)
}

func shardDirPath(keyspace, shard string) string {
	return path.Join(shardsDirPath(keyspace), shard)
}

func shardFilePath(keyspace, shard string) string {
	return path.Join(shardDirPath(keyspace, shard), shardFilename)
}

func tabletDirPath(tabletAlias *topodatapb.TabletAlias) string {
	return path.Join(tabletsDirPath, topoproto.TabletAliasString(tabletAlias))
}

func tabletFilePath(tabletAlias *topodatapb.TabletAlias) string {
	return path.Join(tabletDirPath(tabletAlias), tabletFilename)
}

func keyspaceReplicationDirPath(keyspace string) string {
	return path.Join(replicationDirPath, keyspace)
}

func shardReplicationDirPath(keyspace, shard string) string {
	return path.Join(keyspaceReplicationDirPath(keyspace), shard)
}

func shardReplicationFilePath(keyspace, shard string) string {
	return path.Join(shardReplicationDirPath(keyspace, shard), shardReplicationFilename)
}

func srvKeyspaceDirPath(keyspace string) string {
	return path.Join(servingDirPath, keyspace)
}

func srvKeyspaceFilePath(keyspace string) string {
	return path.Join(srvKeyspaceDirPath(keyspace), srvKeyspaceFilename)
}

func srvVSchemaDirPath() string {
	return servingDirPath
}

func srvVSchemaFilePath() string {
	return path.Join(srvVSchemaDirPath(), vschemaFilename)
}
