package vtctld

import (
	"net/http"
	"path"
	"sort"
	"strings"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctld/explorer"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// BackendExplorer is an Explorer implementation that only uses the
// Backend interface of a server Impl. Eventually, all topology
// implementations will use this.
//
// FIXME(alainjobart) GetKnownCells is only on topo.Impl at the moment.
// Soon, when all topo implementations use the 'cells' subdirectory,
// then we can use topo.Backend, as intended.
type BackendExplorer struct {
	// backend is of type topo.Impl now, but will switch to topo.Backend.
	backend topo.Impl
}

// NewBackendExplorer returns an Explorer implementation for topo.Backend.
func NewBackendExplorer(backend topo.Impl) *BackendExplorer {
	return &BackendExplorer{
		backend: backend,
	}
}

// HandlePath is part of the Explorer interface
func (ex *BackendExplorer) HandlePath(zkPath string, r *http.Request) *explorer.Result {
	ctx := context.Background()
	result := &explorer.Result{}

	// Handle toplevel display: global, then one line per cell.
	if zkPath == "/" {
		cells, err := ex.backend.GetKnownCells(ctx)
		if err != nil {
			result.Error = err.Error()
			return result
		}
		sort.Strings(cells)
		result.Children = append([]string{topo.GlobalCell}, cells...)
		return result
	}

	// Now find the cell.
	parts := strings.Split(zkPath, "/")
	if parts[0] != "" || len(parts) < 2 {
		result.Error = "Invalid path: " + zkPath
		return result
	}
	cell := parts[1]
	relativePath := zkPath[len(cell)+1:]

	// Get the file contents, if any.
	data, _, err := ex.backend.Get(ctx, cell, relativePath)
	switch err {
	case nil:
		// It has contents, we just use it if possible.
		decoded, err := DecodeContent(relativePath, data)
		if err != nil {
			result.Error = err.Error()
		} else {
			result.Data = decoded
		}
	case topo.ErrNoNode:
		// Not a file, we pass.
	default:
		// Something else is wrong.
		result.Error = err.Error()
		return result
	}

	// Get the children, if any.
	children, err := ex.backend.ListDir(ctx, cell, relativePath)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.Children = children
	return result
}

// DecodeContent uses the filename to imply a type, and proto-decodes
// the right object, then echoes it as a string.
func DecodeContent(filename string, data []byte) (string, error) {
	name := path.Base(filename)

	var p proto.Message
	switch name {
	case topo.CellInfoFile:
		p = new(topodatapb.CellInfo)
	case topo.KeyspaceFile:
		p = new(topodatapb.Keyspace)
	case topo.ShardFile:
		p = new(topodatapb.Shard)
	case topo.VSchemaFile:
		p = new(vschemapb.Keyspace)
	case topo.ShardReplicationFile:
		p = new(topodatapb.ShardReplication)
	case topo.TabletFile:
		p = new(topodatapb.Tablet)
	case topo.SrvVSchemaFile:
		p = new(vschemapb.SrvVSchema)
	case topo.SrvKeyspaceFile:
		p = new(topodatapb.SrvKeyspace)
	default:
		return string(data), nil
	}

	if err := proto.Unmarshal(data, p); err != nil {
		return string(data), err
	}
	return proto.MarshalTextString(p), nil
}
