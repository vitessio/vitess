package main

import (
	"fmt"
	"path"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/vt/naming"
	"code.google.com/p/vitess/go/zk"
	"code.google.com/p/vitess/go/zk/zkns"
	"launchpad.net/gozk/zookeeper"
)

// Export addresses from the VT serving graph to a legacy zkns server.
func exportZkns(zconn zk.Conn, zkVtRoot string) error {
	vtNsPath := path.Join(zkVtRoot, "ns")
	zkCell := zk.ZkCellFromZkPath(zkVtRoot)
	zknsRootPath := fmt.Sprintf("/zk/%v/zkns/vt", zkCell)

	children, err := zk.ChildrenRecursive(zconn, vtNsPath)
	if err != nil {
		return err
	}

	for _, child := range children {
		addrPath := path.Join(vtNsPath, child)
		_, stat, err := zconn.Get(addrPath)
		if err != nil {
			return err
		}
		// Leaf nodes correspond to zkns vdns files in the old setup.
		if stat.NumChildren() > 0 {
			continue
		}

		if err = exportVtnsToZkns(zconn, addrPath, zknsRootPath); err != nil {
			return err
		}
	}
	return nil
}

func exportVtnsToZkns(zconn zk.Conn, zkVtnsAddrPath, zknsAddrPath string) error {
	addrs, err := naming.ReadAddrs(zconn, zkVtnsAddrPath)
	if err != nil {
		return err
	}

	// Write the individual endpoints and compute the SRV entries.
	vtoccAddrs := LegacyZknsAddrs{make([]string, 0, 8)}
	defaultAddrs := LegacyZknsAddrs{make([]string, 0, 8)}
	for i, entry := range addrs.Entries {
		zknsAddrPath := fmt.Sprintf("%v/%v", zknsAddrPath, i)
		zknsAddr := zkns.ZknsAddr{Host: entry.Host, Port: entry.NamedPortMap["_mysql"], NamedPortMap: entry.NamedPortMap}
		err := WriteAddr(zconn, zknsAddrPath, &zknsAddr)
		if err != nil {
			return err
		}
		defaultAddrs.Endpoints = append(defaultAddrs.Endpoints, zknsAddrPath)
		vtoccAddrs.Endpoints = append(vtoccAddrs.Endpoints, zknsAddrPath+":_vtocc")
	}

	// Prune any zkns entries that are no longer referenced by the
	// shard graph.
	deleteIdx := len(addrs.Entries)
	for {
		zknsAddrPath := fmt.Sprintf("%v/%v", zknsAddrPath, deleteIdx)
		err := zconn.Delete(zknsAddrPath, -1)
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			break
		}
		if err != nil {
			return err
		}
		deleteIdx++
	}

	// Write the VDNS entries for both vtocc and mysql
	vtoccVdnsPath := fmt.Sprintf("%v/_vtocc.vdns", zknsAddrPath)
	if err = WriteAddrs(zconn, vtoccVdnsPath, &vtoccAddrs); err != nil {
		return err
	}

	defaultVdnsPath := fmt.Sprintf("%v.vdns", zknsAddrPath)
	if err = WriteAddrs(zconn, defaultVdnsPath, &defaultAddrs); err != nil {
		return err
	}
	return nil
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
