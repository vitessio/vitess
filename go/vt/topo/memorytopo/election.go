/*
Copyright 2017 Google Inc.

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

package memorytopo

import (
	"path"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// NewMasterParticipation is part of the topo.Server interface
func (c *Conn) NewMasterParticipation(name, id string) (topo.MasterParticipation, error) {
	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	// Make sure the global path exists.
	electionPath := path.Join(electionsPath, name)
	if n := c.factory.getOrCreatePath(c.cell, electionPath); n == nil {
		return nil, topo.ErrNoNode
	}

	return &cMasterParticipation{
		c:    c,
		name: name,
		id:   id,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}, nil
}

// cMasterParticipation implements topo.MasterParticipation.
//
// We use a directory (in global election path, with the name) with
// ephemeral files in it, that contains the id.  The oldest revision
// wins the election.
type cMasterParticipation struct {
	// c is our memorytopo connection
	c *Conn

	// name is the name of this MasterParticipation
	name string

	// id is the process's current id.
	id string

	// stop is a channel closed when Stop is called.
	stop chan struct{}

	// done is a channel closed when we're done processing the Stop
	done chan struct{}
}

// WaitForMastership is part of the topo.MasterParticipation interface.
func (mp *cMasterParticipation) WaitForMastership() (context.Context, error) {
	// If Stop was already called, mp.done is closed, so we are interrupted.
	select {
	case <-mp.done:
		return nil, topo.ErrInterrupted
	default:
	}

	electionPath := path.Join(electionsPath, mp.name)
	var ld topo.LockDescriptor

	// We use a cancelable context here. If stop is closed,
	// we just cancel that context.
	lockCtx, lockCancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-mp.stop:
			if ld != nil {
				if err := ld.Unlock(context.Background()); err != nil {
					log.Errorf("failed to unlock LockDescriptor %v: %v", electionPath, err)
				}
			}
			lockCancel()
			close(mp.done)
		}
	}()

	// Try to get the mastership, by getting a lock.
	var err error
	ld, err = mp.c.Lock(lockCtx, electionPath, mp.id)
	if err != nil {
		// It can be that we were interrupted.
		return nil, err
	}

	// We got the lock. Return the lockContext. If Stop() is called,
	// it will cancel the lockCtx, and cancel the returned context.
	return lockCtx, nil
}

// Stop is part of the topo.MasterParticipation interface
func (mp *cMasterParticipation) Stop() {
	close(mp.stop)
	<-mp.done
}

// GetCurrentMasterID is part of the topo.MasterParticipation interface
func (mp *cMasterParticipation) GetCurrentMasterID(ctx context.Context) (string, error) {
	electionPath := path.Join(electionsPath, mp.name)

	mp.c.factory.mu.Lock()
	defer mp.c.factory.mu.Unlock()

	n := mp.c.factory.nodeByPath(mp.c.cell, electionPath)
	if n == nil {
		return "", nil
	}

	return n.lockContents, nil
}
