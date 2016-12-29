package zk2topo

import (
	"fmt"
	"path"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"
	"github.com/youtube/vitess/go/zk/zkctl"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestZk2Topo(t *testing.T) {
	// Start a real single ZK daemon, and close it after all tests are done.
	zkd, serverAddr := zkctl.StartLocalZk(1)
	defer zkd.Teardown()

	// This function will create a toplevel directory for a new test.
	testIndex := 0
	newServer := func(cells ...string) *Server {
		// Each test will use its own sub-directories.
		testRoot := fmt.Sprintf("/test-%v", testIndex)
		testIndex++

		ctx := context.Background()
		c := Connect(serverAddr)
		if _, err := c.Create(ctx, testRoot, nil, 0, zk.WorldACL(PermDirectory)); err != nil {
			t.Fatalf("Create(%v) failed: %v", testRoot, err)
		}
		globalRoot := path.Join(testRoot, "global")
		if _, err := c.Create(ctx, globalRoot, nil, 0, zk.WorldACL(PermDirectory)); err != nil {
			t.Fatalf("Create(%v) failed: %v", globalRoot, err)
		}
		cellsDir := path.Join(globalRoot, cellsPath)
		if _, err := c.Create(ctx, cellsDir, nil, 0, zk.WorldACL(PermDirectory)); err != nil {
			t.Fatalf("Create(%v) failed: %v", cellsDir, err)
		}

		for _, cell := range cells {
			cellRoot := path.Join(testRoot, cell)
			if _, err := c.Create(ctx, cellRoot, nil, 0, zk.WorldACL(PermDirectory)); err != nil {
				t.Fatalf("Create(%v) failed: %v", cellRoot, err)
			}

			// Create the CellInfo for the cell.
			ci := &topodatapb.CellInfo{
				ServerAddress: serverAddr,
				Root:          cellRoot,
			}
			data, err := proto.Marshal(ci)
			if err != nil {
				t.Fatalf("cannot proto.Marshal CellInfo: %v", err)
			}
			cellDir := path.Join(cellsDir, cell)
			if _, err := c.Create(ctx, cellDir, nil, 0, zk.WorldACL(PermDirectory)); err != nil {
				t.Fatalf("Create(%v) failed: %v", cellDir, err)
			}
			nodePath := path.Join(cellDir, topo.CellInfoFile)
			if _, err := c.Create(ctx, nodePath, data, 0, zk.WorldACL(PermFile)); err != nil {
				t.Fatalf("Create(%v) failed: %v", nodePath, err)
			}
		}

		return NewServer(serverAddr, globalRoot)
	}

	test.TopoServerTestSuite(t, func() topo.Impl {
		return newServer("test")
	})
}
