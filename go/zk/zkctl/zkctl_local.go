package zkctl

import (
	"fmt"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/netutil"
)

// StartLocalZk is a helper method to create a local ZK process.  Used
// in tests, mostly. It will log.Fatal out if there is an error.  Each
// call should use different serverID / ports, so tests don't
// interfere with eachother. Use the testfiles package to achieve this.
func StartLocalZk(id, port int) (*Zkd, string) {
	// Build the config parameters.
	hostname := netutil.FullyQualifiedHostnameOrPanic()
	zkCfg := fmt.Sprintf("%v@%v:%v:%v:%v", id, hostname, port, port+1, port+2)
	zkConfig := MakeZkConfigFromString(zkCfg, uint32(id))
	zkd := NewZkd(zkConfig)

	// Init & start zk.
	if err := zkd.Init(); err != nil {
		log.Fatalf("zkd.Init(%d, %d) failed: %v", id, port, err)
	}

	return zkd, fmt.Sprintf("%v:%v", hostname, port+2)
}
