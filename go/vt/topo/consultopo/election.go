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

package consultopo

import (
	"path"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/hashicorp/consul/api"
	"github.com/youtube/vitess/go/vt/topo"
)

// NewMasterParticipation is part of the topo.Server interface
func (s *Server) NewMasterParticipation(name, id string) (topo.MasterParticipation, error) {
	// Create the lock here.
	electionPath := path.Join(s.root, electionsPath, name)
	l, err := s.client.LockOpts(&api.LockOptions{
		Key:   electionPath,
		Value: []byte(id),
	})
	if err != nil {
		return nil, err
	}

	return &consulMasterParticipation{
		s:    s,
		lock: l,
		name: name,
		id:   id,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}, nil
}

// consulMasterParticipation implements topo.MasterParticipation.
//
// We use a key with name <global>/elections/<name> for the lock,
// that contains the id.
type consulMasterParticipation struct {
	// s is our parent consul topo Server
	s *Server

	// lock is the *api.Lock structure we're going to use.
	lock *api.Lock

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
func (mp *consulMasterParticipation) WaitForMastership() (context.Context, error) {
	// If Stop was already called, mp.done is closed, so we are interrupted.
	select {
	case <-mp.done:
		return nil, topo.ErrInterrupted
	default:
	}

	// Try to lock until mp.stop is closed.
	lost, err := mp.lock.Lock(mp.stop)
	if err != nil {
		// We can't lock. See if it was because we got canceled.
		select {
		case <-mp.stop:
			close(mp.done)
		default:
		}
		return nil, err
	}

	// We have the lock, keep mastership until we loose it.
	lockCtx, lockCancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-lost:
			// We lost the lock, nothing to do but lockCancel().
			lockCancel()
		case <-mp.stop:
			// Stop was called. We stop the context first,
			// so the running process is not thinking it
			// is the master any more, then we unlock.
			lockCancel()
			if err := mp.lock.Unlock(); err != nil {
				log.Errorf("master election(%v) Unlock failed: %v", mp.name, err)
			}
			close(mp.done)
		}
	}()

	return lockCtx, nil
}

// Stop is part of the topo.MasterParticipation interface
func (mp *consulMasterParticipation) Stop() {
	close(mp.stop)
	<-mp.done
}

// GetCurrentMasterID is part of the topo.MasterParticipation interface
func (mp *consulMasterParticipation) GetCurrentMasterID(ctx context.Context) (string, error) {
	electionPath := path.Join(mp.s.root, electionsPath, mp.name)
	pair, _, err := mp.s.kv.Get(electionPath, nil)
	if err != nil {
		return "", err
	}
	if pair == nil {
		return "", nil
	}
	return string(pair.Value), nil
}
