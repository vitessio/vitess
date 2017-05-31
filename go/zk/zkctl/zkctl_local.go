/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
