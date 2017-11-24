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

// Package faketopo contains utitlities for tests that have to interact with a
// Vitess topology.
package faketopo

import (
	"errors"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

var errNotImplemented = errors.New("Not implemented")

// FakeTopo is a topo.Server implementation that always returns errNotImplemented errors.
type FakeTopo struct{}

// Close is part of the topo.Server interface.
func (ft FakeTopo) Close() {}

// ListDir is part of the topo.Backend interface.
func (ft FakeTopo) ListDir(ctx context.Context, cell, dirPath string) ([]string, error) {
	return nil, errNotImplemented
}

// Create is part of the topo.Backend interface.
func (ft FakeTopo) Create(ctx context.Context, cell, filePath string, contents []byte) (topo.Version, error) {
	return nil, errNotImplemented
}

// Update is part of the topo.Backend interface.
func (ft FakeTopo) Update(ctx context.Context, cell, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	return nil, errNotImplemented
}

// Get is part of the topo.Backend interface.
func (ft FakeTopo) Get(ctx context.Context, cell, filePath string) ([]byte, topo.Version, error) {
	return nil, nil, errNotImplemented
}

// Delete is part of the topo.Backend interface.
func (ft FakeTopo) Delete(ctx context.Context, cell, filePath string, version topo.Version) error {
	return errNotImplemented
}

// Watch is part of the topo.Backend interface.
func (ft FakeTopo) Watch(ctx context.Context, cell string, path string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	return &topo.WatchData{
		Err: errNotImplemented,
	}, nil, nil
}

// Lock is part of the topo.Backend interface.
func (ft FakeTopo) Lock(ctx context.Context, cell, dirPath, contents string) (topo.LockDescriptor, error) {
	return nil, errNotImplemented
}

// NewMasterParticipation is part of the topo.Server interface.
func (ft FakeTopo) NewMasterParticipation(name, id string) (topo.MasterParticipation, error) {
	return nil, errNotImplemented
}

var _ topo.Impl = (*FakeTopo)(nil) // compile-time interface check
