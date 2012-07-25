package main


import (
	"encoding/json"
	"fmt"
	"path"

	"code.google.com/p/vitess/go/vt/naming"
	"code.google.com/p/vitess/go/zk"
	"code.google.com/p/vitess/go/zk/zkns"
	"launchpad.net/gozk/zookeeper"
)

/*
 Export addresses from the VT serving graph to a legacy zkns server.
*/

func exportZkns(zconn zk.Conn, zkVtRoot string) error {
	vtNsPath := path.Join(zkVtRoot, "ns")
	zkCell := zk.ZkCellFromZkPath(zkVtRoot)
	zknsRootPath := fmt.Sprintf("/zk/%v/zkns/vt", zkCell)

	children, err := zk.ChildrenRecursive(zconn, vtNsPath)
	if err != nil { return err }

	for _, child := range children {
		addrPath := path.Join(vtNsPath, child)
		_, stat, err := zconn.Get(addrPath)
		if err != nil {
			return err
		}
		if stat.NumChildren() > 0 {
			continue
		}

		addrs, err := naming.ReadAddrs(zconn, addrPath)
		if err != nil { return err }

		vtoccAddrs := LegacyZknsAddrs{make([]string, 0, 8)}
		defaultAddrs := LegacyZknsAddrs{make([]string, 0, 8)}

		// Write the individual endpoints
		for i, entry := range addrs.Entries {
			zknsAddrPath := fmt.Sprintf("%v/%v/%v", zknsRootPath, child, i)
			zknsAddr := zkns.ZknsAddr{Host:entry.Host, Port:entry.NamedPortMap["_mysql"], NamedPortMap:entry.NamedPortMap}
			err := WriteAddr(zconn, zknsAddrPath, &zknsAddr)
			if err != nil { return err }
			defaultAddrs.Endpoints = append(defaultAddrs.Endpoints, zknsAddrPath)
			vtoccAddrs.Endpoints = append(vtoccAddrs.Endpoints, zknsAddrPath + ":_vtocc")
		}

		// Write the VDNS entries for both vtocc and mysql
		vtoccVdnsPath := fmt.Sprintf("%v/%v/_vtocc.vdns", zknsRootPath, child)
		err = WriteAddrs(zconn, vtoccVdnsPath, &vtoccAddrs)
		if err != nil { return err }

		defaultVdnsPath := fmt.Sprintf("%v/%v.vdns", zknsRootPath, child)
		err = WriteAddrs(zconn, defaultVdnsPath, &defaultAddrs)
		if err != nil { return err }
	}
	return nil
}

type LegacyZknsAddrs struct {
	Endpoints []string `json:"endpoints"`
}

func toJson(x interface{}) string {
	data, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(data)
}

func WriteAddr(zconn zk.Conn, zkPath string, addr *zkns.ZknsAddr) error {
	data := toJson(addr)
	_, err := zk.CreateOrUpdate(zconn, zkPath, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), true)
	return err
}

func WriteAddrs(zconn zk.Conn, zkPath string, addrs *LegacyZknsAddrs) error {
	data := toJson(addrs)
	_, err := zk.CreateOrUpdate(zconn, zkPath, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), true)
	return err
}
