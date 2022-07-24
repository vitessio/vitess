/*
Copyright 2020 The Vitess Authors.

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

package k8stopo

import (
	"path"

	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

const electionsPath = "elections"

// NewLeaderParticipation is part of the topo.Server interface
func (s *Server) NewLeaderParticipation(name, id string) (topo.LeaderParticipation, error) {
	return &kubernetesLeaderParticipation{
		s:    s,
		name: name,
		id:   id,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}, nil
}

// kubernetesLeaderParticipation implements topo.LeaderParticipation.
//
// We use a directory (in global election path, with the name) with
// ephemeral files in it, that contains the id.  The oldest revision
// wins the election.
type kubernetesLeaderParticipation struct {
	// s is our parent kubernetes topo Server
	s *Server

	// name is the name of this LeaderParticipation
	name string

	// id is the process's current id.
	id string

	// stop is a channel closed when Stop is called.
	stop chan struct{}

	// done is a channel closed when we're done processing the Stop
	done chan struct{}
}

func (mp *kubernetesLeaderParticipation) getElectionPath() string {
	return path.Join(mp.s.root, electionsPath, mp.name)
}

// WaitForLeadership is part of the topo.LeaderParticipation interface.
func (mp *kubernetesLeaderParticipation) WaitForLeadership() (context.Context, error) {
	// If Stop was already called, mp.done is closed, so we are interrupted.
	select {
	case <-mp.done:
		return nil, topo.NewError(topo.Interrupted, "Leadership")
	default:
	}

	electionPath := mp.getElectionPath()
	var ld topo.LockDescriptor

	// We use a cancelable context here. If stop is closed,
	// we just cancel that context.
	lockCtx, lockCancel := context.WithCancel(context.Background())
	go func() {
		<-mp.stop
		if ld != nil {
			if err := ld.Unlock(context.Background()); err != nil {
				log.Errorf("failed to unlock electionPath %v: %v", electionPath, err)
			}
		}
		lockCancel()
		close(mp.done)
	}()

	// Try to get the primaryship, by getting a lock.
	var err error
	ld, err = mp.s.lock(lockCtx, electionPath, mp.id, true)
	if err != nil {
		// It can be that we were interrupted.
		return nil, err
	}

	// We got the lock. Return the lockContext. If Stop() is called,
	// it will cancel the lockCtx, and cancel the returned context.
	return lockCtx, nil
}

// Stop is part of the topo.LeaderParticipation interface
func (mp *kubernetesLeaderParticipation) Stop() {
	close(mp.stop)
	<-mp.done
}

// GetCurrentLeaderID is part of the topo.LeaderParticipation interface
func (mp *kubernetesLeaderParticipation) GetCurrentLeaderID(ctx context.Context) (string, error) {
	id, _, err := mp.s.Get(ctx, mp.getElectionPath())
	if err != nil {
		// NoNode means nobody is the primary
		if topo.IsErrType(err, topo.NoNode) {
			return "", nil
		}
		return "", err
	}
	return string(id), nil
}
