package naming

/* Handle logical name resolution  - sort of like DNS but tailored to vt and using zookeeper.

Naming is disconnected from the backend discovery and is used for front end clients.

 The common query is "resolve keyspace.shard.db_type" and return a list of host:port tuples that export our default server (vtocc).  You can get all shards with "keyspace.*.db_type".

/zk/local/vt/ns/<keyspace>/<shard>/<db type>

*/

import (
	"encoding/json"
	"fmt"
	"net"
	"path"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/zk"
	"code.google.com/p/vitess/go/zk/zkns"
)

type VtnsAddr struct {
	Uid          uint           `json:"uid"` // Keep track of which tablet this corresponds to.
	Host         string         `json:"host"`
	Port         int            `json:"port"`
	NamedPortMap map[string]int `json:"named_port_map"`
}

type VtnsAddrs struct {
	Entries []VtnsAddr `json:"entries"`
	version int        // zk version to allow non-stomping writes
}

func NewAddr(uid uint, host string, port int) *VtnsAddr {
	return &VtnsAddr{Uid: uid, Host: host, Port: port, NamedPortMap: make(map[string]int)}
}

func NewAddrs() *VtnsAddrs {
	return &VtnsAddrs{Entries: make([]VtnsAddr, 0, 8), version: -1}
}

func ZkPathForVtKeyspace(cell, keyspace string) string {
	if cell == "" {
		cell = "local"
	}
	return fmt.Sprintf("/zk/%v/vt/ns/%v", cell, keyspace)
}

func ZkPathForVtShard(cell, keyspace, shard string) string {
	return path.Join(ZkPathForVtKeyspace(cell, keyspace), shard)
}

func ZkPathForVtName(cell, keyspace, shard, dbType string) string {
	return path.Join(ZkPathForVtShard(cell, keyspace, shard), dbType)
}

func LookupVtName(zconn zk.Conn, cell, keyspace, shard, dbType, namedPort string) ([]*net.SRV, error) {
	zkPath := ZkPathForVtName(cell, keyspace, shard, dbType)

	addrs, err := ReadAddrs(zconn, zkPath)
	if err != nil {
		return nil, fmt.Errorf("LookupVtName failed: %v %v", zkPath, err)
	}
	srvs, err := SrvEntries(addrs, namedPort)
	if err != nil {
		return nil, fmt.Errorf("LookupVtName failed: %v %v", zkPath, err)
	}
	return srvs, err
}

// FIXME(msolomon) merge with zkns
func SrvEntries(addrs *VtnsAddrs, namedPort string) (srvs []*net.SRV, err error) {
	srvs = make([]*net.SRV, 0, len(addrs.Entries))
	var srvErr error
	for _, entry := range addrs.Entries {
		host := entry.Host
		port := 0
		if namedPort == "" {
			port = entry.Port
		} else {
			port = entry.NamedPortMap[namedPort]
		}
		if port == 0 {
			relog.Warning("vtns: bad port %v %v", namedPort, entry)
			continue
		}
		srvs = append(srvs, &net.SRV{Target: host, Port: uint16(port)})
	}
	zkns.Sort(srvs)
	if srvErr != nil && len(srvs) == 0 {
		return nil, fmt.Errorf("SrvEntries failed: no valid endpoints found")
	}
	return
}

func SrvAddr(srv *net.SRV) string {
	return fmt.Sprintf("%s:%d", srv.Target, srv.Port)
}

// zkPath: a node where children represent individual endpoints for a service.
func writeZknsProcEntry(zconn zk.Conn, zkPath string) error {
	panic("not implemented")
}

func ReadAddrs(zconn zk.Conn, zkPath string) (*VtnsAddrs, error) {
	data, stat, err := zconn.Get(zkPath)
	if err != nil {
		return nil, err
	}
	addrs := new(VtnsAddrs)
	err = json.Unmarshal([]byte(data), addrs)
	if err != nil {
		return nil, err
	}
	addrs.version = stat.Version()
	return addrs, nil
}
