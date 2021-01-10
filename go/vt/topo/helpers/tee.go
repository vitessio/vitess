/*
Copyright 2019 The Vitess Authors.

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

package helpers

import (
	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// TeeFactory is an implementation of topo.Factory that uses a primary
// underlying topo.Server for all changes, but also duplicates the
// changes to a secondary topo.Server. It also locks both topo servers
// when needed.  It is meant to be used during transitions from one
// topo.Server to another.
//
// - primary: we read everything from it, and write to it. We also create
//     MasterParticipation from it.
// - secondary: we write to it as well, but we usually don't fail.
// - we lock primary/secondary if reverseLockOrder is False,
// or secondary/primary if reverseLockOrder is True.
type TeeFactory struct {
	primary          *topo.Server
	secondary        *topo.Server
	reverseLockOrder bool
}

// HasGlobalReadOnlyCell is part of the topo.Factory interface.
func (f *TeeFactory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	return false
}

// Create is part of the topo.Factory interface.
func (f *TeeFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	ctx := context.Background()
	primaryConn, err := f.primary.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}
	secondaryConn, err := f.secondary.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	lockFirst := primaryConn
	lockSecond := secondaryConn
	if f.reverseLockOrder {
		lockFirst = secondaryConn
		lockSecond = primaryConn
	}

	return &TeeConn{
		primary:    primaryConn,
		secondary:  secondaryConn,
		lockFirst:  lockFirst,
		lockSecond: lockSecond,
	}, nil
}

// NewTee returns a new topo.Server object. It uses a TeeFactory.
func NewTee(primary, secondary *topo.Server, reverseLockOrder bool) (*topo.Server, error) {
	f := &TeeFactory{
		primary:          primary,
		secondary:        secondary,
		reverseLockOrder: reverseLockOrder,
	}
	return topo.NewWithFactory(f, "" /*serverAddress*/, "" /*root*/)
}

// TeeConn implements the topo.Conn interface.
type TeeConn struct {
	primary   topo.Conn
	secondary topo.Conn

	lockFirst  topo.Conn
	lockSecond topo.Conn
}

// Close is part of the topo.Conn interface.
func (c *TeeConn) Close() {
	c.primary.Close()
	c.secondary.Close()
}

// ListDir is part of the topo.Conn interface.
func (c *TeeConn) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	return c.primary.ListDir(ctx, dirPath, full)
}

// Create is part of the topo.Conn interface.
func (c *TeeConn) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	primaryVersion, err := c.primary.Create(ctx, filePath, contents)
	if err != nil {
		return nil, err
	}

	// This is critical enough that we want to fail. However, we support
	// an unconditional update if the file already exists.
	_, err = c.secondary.Create(ctx, filePath, contents)
	if topo.IsErrType(err, topo.NodeExists) {
		_, err = c.secondary.Update(ctx, filePath, contents, nil)
	}
	if err != nil {
		return nil, err
	}

	return primaryVersion, nil
}

// Update is part of the topo.Conn interface.
func (c *TeeConn) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	primaryVersion, err := c.primary.Update(ctx, filePath, contents, version)
	if err != nil {
		// Failed on primary, not updating secondary.
		return nil, err
	}

	// Always do an unconditional update on secondary.
	if _, err = c.secondary.Update(ctx, filePath, contents, nil); err != nil {
		log.Warningf("secondary.Update(%v,unconditonal) failed: %v", filePath, err)
	}
	return primaryVersion, nil
}

// Get is part of the topo.Conn interface.
func (c *TeeConn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	return c.primary.Get(ctx, filePath)
}

// Delete is part of the topo.Conn interface.
func (c *TeeConn) Delete(ctx context.Context, filePath string, version topo.Version) error {
	// If primary fails, no need to go further.
	if err := c.primary.Delete(ctx, filePath, version); err != nil {
		return err
	}

	// Always do an unconditonal delete on secondary.
	if err := c.secondary.Delete(ctx, filePath, nil); err != nil && !topo.IsErrType(err, topo.NoNode) {
		// Secondary didn't work, and the node wasn't gone already.
		log.Warningf("secondary.Delete(%v) failed: %v", filePath, err)
	}

	return nil
}

// Watch is part of the topo.Conn interface
func (c *TeeConn) Watch(ctx context.Context, filePath string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	return c.primary.Watch(ctx, filePath)
}

//
// Lock management.
//

// teeTopoLockDescriptor implements the topo.LockDescriptor interface.
type teeTopoLockDescriptor struct {
	c                    *TeeConn
	dirPath              string
	firstLockDescriptor  topo.LockDescriptor
	secondLockDescriptor topo.LockDescriptor
}

// Lock is part of the topo.Conn interface.
func (c *TeeConn) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// Lock lockFirst.
	fLD, err := c.lockFirst.Lock(ctx, dirPath, contents)
	if err != nil {
		return nil, err
	}

	// Lock lockSecond.
	sLD, err := c.lockSecond.Lock(ctx, dirPath, contents)
	if err != nil {
		if err := fLD.Unlock(ctx); err != nil {
			log.Warningf("Failed to unlock lockFirst after failed lockSecond lock for %v: %v", dirPath, err)
		}
		return nil, err
	}

	// Remember both locks in teeTopoLockDescriptor.
	return &teeTopoLockDescriptor{
		c:                    c,
		dirPath:              dirPath,
		firstLockDescriptor:  fLD,
		secondLockDescriptor: sLD,
	}, nil
}

// Check is part of the topo.LockDescriptor interface.
func (ld *teeTopoLockDescriptor) Check(ctx context.Context) error {
	if err := ld.firstLockDescriptor.Check(ctx); err != nil {
		return err
	}
	return ld.secondLockDescriptor.Check(ctx)
}

// Unlock is part of the topo.LockDescriptor interface.
func (ld *teeTopoLockDescriptor) Unlock(ctx context.Context) error {
	// Unlock lockSecond, then lockFirst.
	serr := ld.secondLockDescriptor.Unlock(ctx)
	ferr := ld.firstLockDescriptor.Unlock(ctx)

	if serr != nil {
		if ferr != nil {
			log.Warningf("First Unlock(%v) failed: %v", ld.dirPath, ferr)
		}
		return serr
	}
	return ferr
}

// NewMasterParticipation is part of the topo.Conn interface.
func (c *TeeConn) NewMasterParticipation(name, id string) (topo.MasterParticipation, error) {
	return c.primary.NewMasterParticipation(name, id)
}
