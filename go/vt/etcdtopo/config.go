// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"flag"
	"path"
	"strings"

	"github.com/youtube/vitess/go/flagutil"
)

const (
	// Paths within the etcd keyspace.
	rootPath           = "/vt"
	cellsDirPath       = rootPath + "/cells"
	keyspacesDirPath   = rootPath + "/keyspaces"
	tabletsDirPath     = rootPath + "/tablets"
	replicationDirPath = rootPath + "/replication"
	servingDirPath     = rootPath + "/ns"
	vschemaPath        = rootPath + "/vschema"

	// Magic file names. Directories in etcd cannot have data. Files whose names
	// begin with '_' are hidden from directory listings.
	dataFilename             = "_Data"
	keyspaceFilename         = dataFilename
	shardFilename            = dataFilename
	tabletFilename           = dataFilename
	shardReplicationFilename = dataFilename
	srvKeyspaceFilename      = dataFilename
	srvShardFilename         = dataFilename
	endPointsFilename        = dataFilename
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

func shardsDirPath(keyspace string) string {
	return keyspaceDirPath(keyspace)
}

func shardDirPath(keyspace, shard string) string {
	return path.Join(shardsDirPath(keyspace), shard)
}

func shardFilePath(keyspace, shard string) string {
	return path.Join(shardDirPath(keyspace, shard), shardFilename)
}

func tabletDirPath(tablet string) string {
	return path.Join(tabletsDirPath, tablet)
}

func tabletFilePath(tablet string) string {
	return path.Join(tabletDirPath(tablet), tabletFilename)
}

func shardReplicationDirPath(keyspace, shard string) string {
	return path.Join(replicationDirPath, keyspace, shard)
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

func srvShardDirPath(keyspace, shard string) string {
	return path.Join(srvKeyspaceDirPath(keyspace), shard)
}

func srvShardFilePath(keyspace, shard string) string {
	return path.Join(srvShardDirPath(keyspace, shard), srvShardFilename)
}

func endPointsDirPath(keyspace, shard, tabletType string) string {
	return path.Join(srvShardDirPath(keyspace, shard), tabletType)
}

func endPointsFilePath(keyspace, shard, tabletType string) string {
	return path.Join(endPointsDirPath(keyspace, shard, tabletType), endPointsFilename)
}

// GetSubprocessFlags implements topo.Server.
func (s *Server) GetSubprocessFlags() []string {
	return []string{"-etcd_global_addrs", strings.Join(globalAddrs, ",")}
}
