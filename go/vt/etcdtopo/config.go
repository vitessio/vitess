/*
Copyright 2017 Google Inc.

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
