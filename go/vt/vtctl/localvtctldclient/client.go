package localvtctldclient

import (
	"errors"
	"sync"

	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

var (
	m               sync.RWMutex
	server          vtctlservicepb.VtctldServer
	errStreamClosed = errors.New("stream is closed for sending") // nolint (TODO:@ajm188) this will be used in a future PR, and the codegen will produce invalid code for streaming rpcs without this
)

type localVtctldClient struct {
	s vtctlservicepb.VtctldServer
}

// Close is part of the vtctldclient.VtctldClient interface.
func (client *localVtctldClient) Close() error { return nil }

//go:generate -command localvtctldclient go run ../vtctldclient/codegen
//go:generate localvtctldclient -targetpkg localvtctldclient -impl localVtctldClient -out client_gen.go -local

// New returns a local vtctldclient.VtctldClient that makes method calls on the
// provided VtctldServer implementation. No network traffic takes place between
// the client and server, and all CallOptions are ignored on RPCs.
func New(s vtctlservicepb.VtctldServer) vtctldclient.VtctldClient {
	return &localVtctldClient{s: s}
}

// SetServer sets the server implementation used when creating local clients
// via the vtctldclient.Factory.
//
// This function must be called before calling vtctldclient.New.
func SetServer(s vtctlservicepb.VtctldServer) {
	m.Lock()
	defer m.Unlock()

	server = s
}

func localVtctldClientFactory(addr string) (vtctldclient.VtctldClient, error) {
	m.RLock()
	defer m.RUnlock()

	if server == nil {
		return nil, errors.New("cannot create local vtctldclient without a server; call SetServer first")
	}

	return New(server), nil
}

func init() {
	vtctldclient.Register("local", localVtctldClientFactory)
}
