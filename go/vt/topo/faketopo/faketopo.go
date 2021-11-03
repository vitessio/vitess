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

	"vitess.io/vitess/go/vt/topo"
)

// FakeFactory implements the Factory interface. This is supposed to be used only for testing
type FakeFactory struct {
}

var _ topo.Factory = (*FakeFactory)(nil)

// HasGlobalReadOnlyCell implements the Factory interface
func (f FakeFactory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	return false
}

// Create implements the Factory interface
// It creates a fake connection which is supposed to be used only for testing
func (f FakeFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	panic("implement me")
}

// FakeConn implements the Conn interface. It is used only for testing
type FakeConn struct {
}

var _ topo.Conn = (*FakeConn)(nil)

// ListDir implements the Conn interface
func (f *FakeConn) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	panic("implement me")
}

// Create implements the Conn interface
func (f *FakeConn) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	panic("implement me")
}

// Update implements the Conn interface
func (f *FakeConn) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	panic("implement me")
}

// Get implements the Conn interface
func (f *FakeConn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	panic("implement me")
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

func NewFakeTopoServer() *topo.Server {
	factory := &FakeFactory{}
	server, _ := topo.NewWithFactory(factory, "" /*serverAddress*/, "" /*root*/)
	return server
}
