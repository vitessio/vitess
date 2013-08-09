package vttablet

import (
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
)

// TabletParamToTabletAlias takes either an old style ZK tablet path or a
// new style tablet alias as a string, and returns a TabletAlias.
func TabletParamToTabletAlias(param string) topo.TabletAlias {
	if param[0] == '/' {
		// old zookeeper path, convert to new-style string tablet alias
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 6 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[3] != "vt" || zkPathParts[4] != "tablets" {
			log.Fatalf("Invalid tablet path: %v", param)
		}
		param = zkPathParts[2] + "-" + zkPathParts[5]
	}
	result, err := topo.ParseTabletAliasString(param)
	if err != nil {
		log.Fatalf("Invalid tablet alias %v: %v", param, err)
	}
	return result
}
