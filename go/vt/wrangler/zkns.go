package wrangler

import (
	"fmt"
	"path"
	"sort"
	"strings"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/topo"
	"code.google.com/p/vitess/go/vt/zktopo"
	"code.google.com/p/vitess/go/zk"
	"code.google.com/p/vitess/go/zk/zkns"
	"launchpad.net/gozk/zookeeper"
)

// Export addresses from the VT serving graph to a legacy zkns server.
// Note these functions only work with a zktopo.
func (wr *Wrangler) ExportZkns(cell string) error {
	zkTopo, ok := wr.ts.(*zktopo.Server)
	if !ok {
		return fmt.Errorf("ExportZkns only works with zktopo")
	}
	zconn := zkTopo.GetZConn()

	vtNsPath := fmt.Sprintf("/zk/%v/vt/ns", cell)
	zknsRootPath := fmt.Sprintf("/zk/%v/zkns/vt", cell)

	children, err := zk.ChildrenRecursive(zconn, vtNsPath)
	if err != nil {
		return err
	}

	for _, child := range children {
		addrPath := path.Join(vtNsPath, child)
		zknsAddrPath := path.Join(zknsRootPath, child)
		_, stat, err := zconn.Get(addrPath)
		if err != nil {
			return err
		}
		// Leaf nodes correspond to zkns vdns files in the old setup.
		if stat.NumChildren() > 0 {
			continue
		}

		if _, err = wr.exportVtnsToZkns(zconn, addrPath, zknsAddrPath); err != nil {
			return err
		}
	}
	return nil
}

// Export addresses from the VT serving graph to a legacy zkns server.
func (wr *Wrangler) ExportZknsForKeyspace(keyspace string) error {
	zkTopo, ok := wr.ts.(*zktopo.Server)
	if !ok {
		return fmt.Errorf("ExportZknsForKeyspace only works with zktopo")
	}
	zconn := zkTopo.GetZConn()

	shardNames, err := wr.ts.GetShardNames(keyspace)
	if err != nil {
		return err
	}

	// Scan the first shard to discover which cells need local serving data.
	aliases, err := topo.FindAllTabletAliasesInShard(wr.ts, keyspace, shardNames[0])
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
		zknsChildren, err := zk.ChildrenRecursive(zconn, zknsRootPath)
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

		vtnsChildren, err := zk.ChildrenRecursive(zconn, vtnsRootPath)
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

			_, stat, err := zconn.Get(vtnsAddrPath)
			if err != nil {
				return err
			}
			// Leaf nodes correspond to zkns vdns files in the old setup.
			if stat.NumChildren() > 0 {
				continue
			}
			zknsPathsWritten, err := wr.exportVtnsToZkns(zconn, vtnsAddrPath, zknsAddrPath)
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
			if err := zconn.Delete(prunePaths[i], -1); err != nil && !zookeeper.IsError(err, zookeeper.ZNOTEMPTY) {
				return err
			}
		}
	}
	return nil
}

func (wr *Wrangler) exportVtnsToZkns(zconn zk.Conn, vtnsAddrPath, zknsAddrPath string) ([]string, error) {
	zknsPaths := make([]string, 0, 32)
	parts := strings.Split(vtnsAddrPath, "/")
	if len(parts) != 8 {
		return nil, fmt.Errorf("Invalid leaf zk path: %v", vtnsAddrPath)
	}
	cell := parts[2]
	keyspace := parts[5]
	shard := parts[6]
	tabletType := topo.TabletType(parts[7])
	addrs, err := wr.ts.GetSrvTabletType(cell, keyspace, shard, tabletType)
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
		err := WriteAddr(zconn, zknsAddrPath, &zknsAddr)
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
		err := zconn.Delete(zknsStaleAddrPath, -1)
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
	if err = WriteAddrs(zconn, vtoccVdnsPath, &vtoccAddrs); err != nil {
		return nil, err
	}

	defaultVdnsPath := fmt.Sprintf("%v.vdns", zknsAddrPath)
	zknsPaths = append(zknsPaths, defaultVdnsPath)
	if err = WriteAddrs(zconn, defaultVdnsPath, &defaultAddrs); err != nil {
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
