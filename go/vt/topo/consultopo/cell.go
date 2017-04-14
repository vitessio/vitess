package consultopo

import (
	"fmt"
	"path"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/consul/api"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// cellClient wraps a Client for keeping track of cell-local clusters.
type cellClient struct {
	// client is the consul api client.
	client *api.Client
	kv     *api.KV

	// root is the root path for this client.
	root string

	// mu protects the following fields.
	mu sync.Mutex
	// locks is a map of *lockInstance structures.
	// The key is the filepath of the Lock file.
	locks map[string]*lockInstance
}

// lockInstance keeps track of one lock held by this client.
type lockInstance struct {
	// lock has the api.Lock structure.
	lock *api.Lock

	// done is closed when the lock is release by this process.
	done chan struct{}
}

// newCellClient returns a new cellClient for the given address and root.
func newCellClient(serverAddr, root string) (*cellClient, error) {
	cfg := api.DefaultConfig()
	cfg.Address = serverAddr
	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return &cellClient{
		client: client,
		kv:     client.KV(),
		root:   root,
		locks:  make(map[string]*lockInstance),
	}, nil
}

func (c *cellClient) close() {
}

// clientForCell returns a client for the given cell-local consul cluster.
// It caches clients for previously requested cells.
func (s *Server) clientForCell(ctx context.Context, cell string) (*cellClient, error) {
	// Global cell is the easy case.
	if cell == topo.GlobalCell {
		return s.global, nil
	}

	// Return a cached client if present.
	s.mu.Lock()
	client, ok := s.cells[cell]
	s.mu.Unlock()
	if ok {
		return client, nil
	}

	// Fetch cell cluster addresses from the global cluster.
	// These can proceed concurrently (we've released the lock).
	serverAddr, root, err := s.getCellAddrs(ctx, cell)
	if err != nil {
		return nil, err
	}

	// Update the cache.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if another goroutine beat us to creating a client for
	// this cell.
	if client, ok = s.cells[cell]; ok {
		return client, nil
	}

	// Create the client.
	c, err := newCellClient(serverAddr, root)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %v: %v", serverAddr, err)
	}
	s.cells[cell] = c
	return c, nil
}

// getCellAddrs returns the list of consul servers to try for the given
// cell-local cluster, and the root directory. These lists are stored
// in the global consul cluster.
func (s *Server) getCellAddrs(ctx context.Context, cell string) (string, string, error) {
	nodePath := path.Join(s.global.root, cellsPath, cell, topo.CellInfoFile)
	pair, _, err := s.global.kv.Get(nodePath, nil)
	if err != nil {
		return "", "", err
	}
	if pair == nil {
		return "", "", topo.ErrNoNode
	}

	ci := &topodatapb.CellInfo{}
	if err := proto.Unmarshal(pair.Value, ci); err != nil {
		return "", "", fmt.Errorf("cannot unmarshal cell node %v: %v", nodePath, err)
	}
	if ci.ServerAddress == "" {
		return "", "", fmt.Errorf("CellInfo.ServerAddress node %v is empty, expected list of addresses", nodePath)
	}

	return ci.ServerAddress, ci.Root, nil
}
