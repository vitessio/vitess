package wrangler

import (
	"fmt"
	"path"
	"sort"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/naming"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/zk"
	"code.google.com/p/vitess/go/zk/zkns"
	"launchpad.net/gozk/zookeeper"
)

// Export addresses from the VT serving graph to a legacy zkns server.
func (wr *Wrangler) ExportZkns(zkVtRoot string) error {
	vtNsPath := path.Join(zkVtRoot, "ns")
	zkCell, err := zk.ZkCellFromZkPath(zkVtRoot)
	if err != nil {
		return err
	}
	zknsRootPath := fmt.Sprintf("/zk/%v/zkns/vt", zkCell)

	children, err := zk.ChildrenRecursive(wr.zconn, vtNsPath)
	if err != nil {
		return err
	}

	for _, child := range children {
		addrPath := path.Join(vtNsPath, child)
		zknsAddrPath := path.Join(zknsRootPath, child)
		_, stat, err := wr.zconn.Get(addrPath)
		if err != nil {
			return err
		}
		// Leaf nodes correspond to zkns vdns files in the old setup.
		if stat.NumChildren() > 0 {
			continue
		}

		if _, err = wr.exportVtnsToZkns(addrPath, zknsAddrPath); err != nil {
			return err
		}
	}
	return nil
}

// Export addresses from the VT serving graph to a legacy zkns server.
func (wr *Wrangler) ExportZknsForKeyspace(zkKeyspacePath string) error {
	keyspace := path.Base(zkKeyspacePath)
	shardNames, _, err := wr.zconn.Children(path.Join(zkKeyspacePath, "shards"))
	if err != nil {
		return err
	}

	// Scan the first shard to discover which cells need local serving data.
	zkShardPath := tm.ShardPath(keyspace, shardNames[0])
	aliases, err := tm.FindAllTabletAliasesInShard(wr.zconn, zkShardPath)
	if err != nil {
		return err
	}

	cellMap := make(map[string]bool)
	for _, alias := range aliases {
		cellMap[alias.Cell] = true
	}

	for cell, _ := range cellMap {
		vtnsRootPath := fmt.Sprintf("/zk/%v/vt/ns/%v", cell, keyspace)
		zknsRootPath := fmt.Sprintf("/zk/%v/zkns/vt/%v", cell, keyspace)

		// Get the existing list of zkns children. If they don't get rewritten,
		// delete them as stale entries.
		zknsChildren, err := zk.ChildrenRecursive(wr.zconn, zknsRootPath)
		if err != nil {
			if zookeeper.IsError(err, zookeeper.ZNONODE) {
				zknsChildren = make([]string, 0)
			} else {
				return err
			}
		}
		staleZknsPaths := make(map[string]bool)
		for _, child := range zknsChildren {
			staleZknsPaths[path.Join(zknsRootPath, child)] = true
		}

		vtnsChildren, err := zk.ChildrenRecursive(wr.zconn, vtnsRootPath)
		if err != nil {
			if zookeeper.IsError(err, zookeeper.ZNONODE) {
				vtnsChildren = make([]string, 0)
			} else {
				return err
			}
		}
		for _, child := range vtnsChildren {
			vtnsAddrPath := path.Join(vtnsRootPath, child)
			zknsAddrPath := path.Join(zknsRootPath, child)

			_, stat, err := wr.zconn.Get(vtnsAddrPath)
			if err != nil {
				return err
			}
			// Leaf nodes correspond to zkns vdns files in the old setup.
			if stat.NumChildren() > 0 {
				continue
			}
			zknsPathsWritten, err := wr.exportVtnsToZkns(vtnsAddrPath, zknsAddrPath)
			if err != nil {
				return err
			}
			relog.Debug("zknsPathsWritten: %v", zknsPathsWritten)
			for _, zkPath := range zknsPathsWritten {
				delete(staleZknsPaths, zkPath)
			}
		}
		relog.Debug("staleZknsPaths: %v", staleZknsPaths)
		prunePaths := make([]string, 0, len(staleZknsPaths))
		for prunePath, _ := range staleZknsPaths {
			prunePaths = append(prunePaths, prunePath)
		}
		sort.Strings(prunePaths)
		// Prune paths in reverse order so we remove children first
		for i := len(prunePaths) - 1; i >= 0; i-- {
			relog.Info("prune stale zkns path %v", prunePaths[i])
			if err := wr.zconn.Delete(prunePaths[i], -1); err != nil && !zookeeper.IsError(err, zookeeper.ZNOTEMPTY) {
				return err
			}
		}
	}
	return nil
}

func (wr *Wrangler) exportVtnsToZkns(vtnsAddrPath, zknsAddrPath string) ([]string, error) {
	zknsPaths := make([]string, 0, 32)
	addrs, err := naming.ReadAddrs(wr.zconn, vtnsAddrPath)
	if err != nil {
		return nil, err
	}

	// Write the individual endpoints and compute the SRV entries.
	vtoccAddrs := LegacyZknsAddrs{make([]string, 0, 8)}
	defaultAddrs := LegacyZknsAddrs{make([]string, 0, 8)}
	for i, entry := range addrs.Entries {
		zknsAddrPath := fmt.Sprintf("%v/%v", zknsAddrPath, i)
		zknsPaths = append(zknsPaths, zknsAddrPath)
		zknsAddr := zkns.ZknsAddr{Host: entry.Host, Port: entry.NamedPortMap["_mysql"], NamedPortMap: entry.NamedPortMap}
		err := WriteAddr(wr.zconn, zknsAddrPath, &zknsAddr)
		if err != nil {
			return nil, err
		}
		defaultAddrs.Endpoints = append(defaultAddrs.Endpoints, zknsAddrPath)
		vtoccAddrs.Endpoints = append(vtoccAddrs.Endpoints, zknsAddrPath+":_vtocc")
	}

	// Prune any zkns entries that are no longer referenced by the
	// shard graph.
	deleteIdx := len(addrs.Entries)
	for {
		zknsStaleAddrPath := fmt.Sprintf("%v/%v", zknsAddrPath, deleteIdx)
		// A "delete" is a write of sorts - just communicate up that nothing
		// needs to be done to this node.
		zknsPaths = append(zknsPaths, zknsStaleAddrPath)
		err := wr.zconn.Delete(zknsStaleAddrPath, -1)
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			break
		}
		if err != nil {
			return nil, err
		}
		deleteIdx++
	}

	// Write the VDNS entries for both vtocc and mysql
	vtoccVdnsPath := fmt.Sprintf("%v/_vtocc.vdns", zknsAddrPath)
	zknsPaths = append(zknsPaths, vtoccVdnsPath)
	if err = WriteAddrs(wr.zconn, vtoccVdnsPath, &vtoccAddrs); err != nil {
		return nil, err
	}

	defaultVdnsPath := fmt.Sprintf("%v.vdns", zknsAddrPath)
	zknsPaths = append(zknsPaths, defaultVdnsPath)
	if err = WriteAddrs(wr.zconn, defaultVdnsPath, &defaultAddrs); err != nil {
		return nil, err
	}
	return zknsPaths, nil
}

type LegacyZknsAddrs struct {
	Endpoints []string `json:"endpoints"`
}

func WriteAddr(zconn zk.Conn, zkPath string, addr *zkns.ZknsAddr) error {
	data := jscfg.ToJson(addr)
	_, err := zk.CreateOrUpdate(zconn, zkPath, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), true)
	return err
}

func WriteAddrs(zconn zk.Conn, zkPath string, addrs *LegacyZknsAddrs) error {
	data := jscfg.ToJson(addrs)
	_, err := zk.CreateOrUpdate(zconn, zkPath, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), true)
	return err
}
