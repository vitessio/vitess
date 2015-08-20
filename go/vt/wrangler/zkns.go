package wrangler

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/zktopo"
	"github.com/youtube/vitess/go/zk"
	"github.com/youtube/vitess/go/zk/zkns"
	"golang.org/x/net/context"
	"launchpad.net/gozk/zookeeper"
)

// ExportZkns exports addresses from the VT serving graph to a legacy zkns server.
// Note these functions only work with a zktopo.
func (wr *Wrangler) ExportZkns(ctx context.Context, cell string) error {
	zkTopo, ok := wr.ts.Impl.(*zktopo.Server)
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

		if _, err = wr.exportVtnsToZkns(ctx, zconn, addrPath, zknsAddrPath); err != nil {
			return err
		}
	}
	return nil
}

// ExportZknsForKeyspace exports addresses from the VT serving graph to a legacy zkns server.
func (wr *Wrangler) ExportZknsForKeyspace(ctx context.Context, keyspace string) error {
	zkTopo, ok := wr.ts.Impl.(*zktopo.Server)
	if !ok {
		return fmt.Errorf("ExportZknsForKeyspace only works with zktopo")
	}
	zconn := zkTopo.GetZConn()

	shardNames, err := wr.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return err
	}

	// Scan the first shard to discover which cells need local serving data.
	aliases, err := wr.ts.FindAllTabletAliasesInShard(ctx, keyspace, shardNames[0])
	if err != nil {
		return err
	}

	cellMap := make(map[string]bool)
	for _, alias := range aliases {
		cellMap[alias.Cell] = true
	}

	for cell := range cellMap {
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
			zknsPathsWritten, err := wr.exportVtnsToZkns(ctx, zconn, vtnsAddrPath, zknsAddrPath)
			if err != nil {
				return err
			}
			log.V(6).Infof("zknsPathsWritten: %v", zknsPathsWritten)
			for _, zkPath := range zknsPathsWritten {
				delete(staleZknsPaths, zkPath)
			}
		}
		log.V(6).Infof("staleZknsPaths: %v", staleZknsPaths)
		prunePaths := make([]string, 0, len(staleZknsPaths))
		for prunePath := range staleZknsPaths {
			prunePaths = append(prunePaths, prunePath)
		}
		sort.Strings(prunePaths)
		// Prune paths in reverse order so we remove children first
		for i := len(prunePaths) - 1; i >= 0; i-- {
			log.Infof("prune stale zkns path %v", prunePaths[i])
			if err := zconn.Delete(prunePaths[i], -1); err != nil && !zookeeper.IsError(err, zookeeper.ZNOTEMPTY) {
				return err
			}
		}
	}
	return nil
}

func (wr *Wrangler) exportVtnsToZkns(ctx context.Context, zconn zk.Conn, vtnsAddrPath, zknsAddrPath string) ([]string, error) {
	zknsPaths := make([]string, 0, 32)
	parts := strings.Split(vtnsAddrPath, "/")
	if len(parts) != 8 && len(parts) != 9 {
		return nil, fmt.Errorf("Invalid leaf zk path: %v", vtnsAddrPath)
	}
	cell := parts[2]
	keyspace := parts[5]
	shard := parts[6]
	tabletTypeStr := parts[7]
	if tabletTypeStr == "action" || tabletTypeStr == "actionlog" {
		return nil, nil
	}
	tabletType, err := topoproto.ParseTabletType(tabletTypeStr)
	if err != nil {
		return nil, err
	}
	addrs, _, err := wr.ts.GetEndPoints(ctx, cell, keyspace, shard, tabletType)
	if err != nil {
		return nil, err
	}

	// Write the individual endpoints and compute the SRV entries.
	vtoccAddrs := LegacyZknsAddrs{make([]string, 0, 8)}
	defaultAddrs := LegacyZknsAddrs{make([]string, 0, 8)}
	for i, entry := range addrs.Entries {
		zknsAddrPath := fmt.Sprintf("%v/%v", zknsAddrPath, i)
		zknsPaths = append(zknsPaths, zknsAddrPath)
		zknsAddr := zkns.ZknsAddr{
			Host:    entry.Host,
			PortMap: entry.PortMap,
		}
		err := writeAddr(zconn, zknsAddrPath, &zknsAddr)
		if err != nil {
			return nil, err
		}
		defaultAddrs.Endpoints = append(defaultAddrs.Endpoints, zknsAddrPath)
		vtoccAddrs.Endpoints = append(vtoccAddrs.Endpoints, zknsAddrPath+":vt")
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
	vtoccVdnsPath := fmt.Sprintf("%v/vt.vdns", zknsAddrPath)
	zknsPaths = append(zknsPaths, vtoccVdnsPath)
	if err = writeAddrs(zconn, vtoccVdnsPath, &vtoccAddrs); err != nil {
		return nil, err
	}

	defaultVdnsPath := fmt.Sprintf("%v.vdns", zknsAddrPath)
	zknsPaths = append(zknsPaths, defaultVdnsPath)
	if err = writeAddrs(zconn, defaultVdnsPath, &defaultAddrs); err != nil {
		return nil, err
	}
	return zknsPaths, nil
}

// LegacyZknsAddrs is what we write to ZK to use for zkns
type LegacyZknsAddrs struct {
	Endpoints []string `json:"endpoints"`
}

func writeAddr(zconn zk.Conn, zkPath string, addr *zkns.ZknsAddr) error {
	data, err := json.MarshalIndent(addr, "", "  ")
	if err != nil {
		return err
	}
	_, err = zk.CreateOrUpdate(zconn, zkPath, string(data), 0, zookeeper.WorldACL(zookeeper.PERM_ALL), true)
	return err
}

func writeAddrs(zconn zk.Conn, zkPath string, addrs *LegacyZknsAddrs) error {
	data, err := json.MarshalIndent(addrs, "", "  ")
	if err != nil {
		return err
	}
	_, err = zk.CreateOrUpdate(zconn, zkPath, string(data), 0, zookeeper.WorldACL(zookeeper.PERM_ALL), true)
	return err
}
