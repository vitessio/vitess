/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package faketopo

import (
	"context"
	"sync"

	"vitess.io/vitess/go/vt/topo/memorytopo"

	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/vt/topo"
)

// FakeFactory implements the Factory interface. This is supposed to be used only for testing
type FakeFactory struct {
	// mu protects the following field.
	mu sync.Mutex
	// cells is the toplevel map that has one entry per cell. It has a list of connections that this fake server will return
	cells map[string][]*FakeConn
}

var _ topo.Factory = (*FakeFactory)(nil)

// NewFakeTopoFactory creates a new fake topo factory
func NewFakeTopoFactory(cells ...string) *FakeFactory {
	factory := &FakeFactory{
		mu:    sync.Mutex{},
		cells: map[string][]*FakeConn{},
	}
	for _, cell := range cells {
		factory.cells[cell] = []*FakeConn{}
	}
	factory.cells[topo.GlobalCell] = []*FakeConn{newFakeConnection()}
	return factory
}

// HasGlobalReadOnlyCell implements the Factory interface
func (f *FakeFactory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	return false
}

// Create implements the Factory interface
// It creates a fake connection which is supposed to be used only for testing
func (f *FakeFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	connections, ok := f.cells[cell]
	if !ok || len(connections) == 0 {
		return nil, topo.NewError(topo.NoNode, cell)
	}
	// pick the first connection and remove it from the list
	conn := connections[0]
	f.cells[cell] = connections[1:]

	conn.serverAddr = serverAddr
	conn.cell = cell
	return conn, nil
}

// FakeConn implements the Conn interface. It is used only for testing
type FakeConn struct {
	cell       string
	serverAddr string

	// getResultMap is a map storing the results for each filepath
	getResultMap map[string]result
}

// newFakeConnection creates a new fake connection
func newFakeConnection() *FakeConn {
	return &FakeConn{
		getResultMap: map[string]result{},
	}
}

// result keeps track of the fields needed to respond to a Get function call
type result struct {
	contents []byte
	version  uint64
	err      error
}

var _ topo.Conn = (*FakeConn)(nil)

// ListDir implements the Conn interface
func (f *FakeConn) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	panic("implement me")
}

// Create implements the Conn interface
func (f *FakeConn) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	f.getResultMap[filePath] = result{
		contents: contents,
		version:  1,
		err:      nil,
	}
	return memorytopo.NodeVersion(1), nil
}

// Update implements the Conn interface
func (f *FakeConn) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	if version == nil {
		_, err := f.Create(ctx, filePath, contents)
		if err != nil {
			return nil, err
		}
	}
	res, isPresent := f.getResultMap[filePath]
	if !isPresent {
		return nil, topo.NewError(topo.NoNode, filePath)
	}
	res.contents = contents
	return memorytopo.NodeVersion(res.version), res.err
}

// Get implements the Conn interface
func (f *FakeConn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	res, isPresent := f.getResultMap[filePath]
	if !isPresent {
		return nil, nil, topo.NewError(topo.NoNode, filePath)
	}
	return res.contents, memorytopo.NodeVersion(res.version), res.err
}

// Delete implements the Conn interface
func (f *FakeConn) Delete(ctx context.Context, filePath string, version topo.Version) error {
	panic("implement me")
}

// Lock implements the Conn interface
func (f *FakeConn) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	panic("implement me")
}

// Watch implements the Conn interface
func (f *FakeConn) Watch(ctx context.Context, filePath string) (current *topo.WatchData, changes <-chan *topo.WatchData, cancel topo.CancelFunc) {
	panic("implement me")
}

// NewMasterParticipation implements the Conn interface
func (f *FakeConn) NewMasterParticipation(name, id string) (topo.MasterParticipation, error) {
	panic("implement me")
}

// Close implements the Conn interface
func (f *FakeConn) Close() {
	panic("implement me")
}

// NewFakeTopoServer creates a new fake topo server
func NewFakeTopoServer(factory *FakeFactory) *topo.Server {
	ts, err := topo.NewWithFactory(factory, "" /*serverAddress*/, "" /*root*/)
	if err != nil {
		log.Exitf("topo.NewWithFactory() failed: %v", err)
	}
	for cell := range factory.cells {
		if err := ts.CreateCellInfo(context.Background(), cell, &topodatapb.CellInfo{}); err != nil {
			log.Exitf("ts.CreateCellInfo(%v) failed: %v", cell, err)
		}
	}
	return ts
}
