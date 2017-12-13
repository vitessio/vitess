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

package zk2topo

import (
	"fmt"
	"path"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/testfiles"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"
	"github.com/youtube/vitess/go/vt/zkctl"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestZk2Topo(t *testing.T) {
	// Start a real single ZK daemon, and close it after all tests are done.
	zkd, serverAddr := zkctl.StartLocalZk(testfiles.GoVtTopoZk2topoZkID, testfiles.GoVtTopoZk2topoPort)
	defer zkd.Teardown()

	// Run the test suite.
	testIndex := 0
	test.TopoServerTestSuite(t, func() *topo.Server {
		// Each test will use its own sub-directories.
		// The directories will be created when used the first time.
		testRoot := fmt.Sprintf("/test-%v", testIndex)
		testIndex++

		globalRoot := path.Join(testRoot, topo.GlobalCell)
		cellRoot := path.Join(testRoot, test.LocalCellName)

		// Note we exercise the observer feature here by passing in
		// the same server twice, with a "|" separator.
		ts, err := topo.OpenServer("zk2", serverAddr+"|"+serverAddr, globalRoot)
		if err != nil {
			t.Fatalf("OpenServer() failed: %v", err)
		}
		if err := ts.CreateCellInfo(context.Background(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: serverAddr,
			Root:          cellRoot,
		}); err != nil {
			t.Fatalf("CreateCellInfo() failed: %v", err)
		}

		return ts
	})
}

func TestHasObservers(t *testing.T) {
	s1, s2, ok := hasObservers("s1:p1,s2:p2")
	if ok {
		t.Errorf("hasObservers(s1:p1,s2:p2): got unexpected %v %v %v", s1, s2, ok)
	}

	s1, s2, ok = hasObservers("s1:p1,s2:p2|o1:p1,o2:p2")
	if !ok || s1 != "s1:p1,s2:p2" || s2 != "o1:p1,o2:p2" {
		t.Errorf("hasObservers(s1:p1,s2:p2|o1:p1,o2:p2): got unexpected %v %v %v", s1, s2, ok)
	}
}
