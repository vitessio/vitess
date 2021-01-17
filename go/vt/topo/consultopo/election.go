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

package consultopo

import (
	"path"

	"context"

	"github.com/hashicorp/consul/api"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// NewMasterParticipation is part of the topo.Server interface
func (s *Server) NewMasterParticipation(name, id string) (topo.MasterParticipation, error) {
	return &consulMasterParticipation{
		s:    s,
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

	electionPath := path.Join(mp.s.root, electionsPath, mp.name)
	l, err := mp.s.client.LockOpts(&api.LockOptions{
		Key:   electionPath,
		Value: []byte(mp.id),
	})
	if err != nil {
		return nil, err
	}

	// If Stop was already called, mp.done is closed, so we are interrupted.
	select {
	case <-mp.done:
		return nil, topo.NewError(topo.Interrupted, "mastership")
	default:
	}

	// Try to lock until mp.stop is closed.
	lost, err := l.Lock(mp.stop)
	if err != nil {
		// We can't lock. See if it was because we got canceled.
		select {
		case <-mp.stop:
			close(mp.done)
		default:
		}
		return nil, err
	}

	// We have the lock, keep mastership until we lose it.
	lockCtx, lockCancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-lost:
			lockCancel()
			// We could have lost the lock. Per consul API, explicitly call Unlock to make sure that session will not be renewed.
			if err := l.Unlock(); err != nil {
				log.Errorf("master election(%v) Unlock failed: %v", mp.name, err)
			}
		case <-mp.stop:
			// Stop was called. We stop the context first,
			// so the running process is not thinking it
			// is the master any more, then we unlock.
			lockCancel()
			if err := l.Unlock(); err != nil {
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
