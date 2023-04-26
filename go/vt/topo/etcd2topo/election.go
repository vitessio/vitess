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

package etcd2topo

import (
	"context"
	"path"

	clientv3 "go.etcd.io/etcd/client/v3"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// NewLeaderParticipation is part of the topo.Server interface
func (s *Server) NewLeaderParticipation(name, id string) (topo.LeaderParticipation, error) {
	return &etcdLeaderParticipation{
		s:    s,
		name: name,
		id:   id,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}, nil
}

// etcdLeaderParticipation implements topo.LeaderParticipation.
//
// We use a directory (in global election path, with the name) with
// ephemeral files in it, that contains the id.  The oldest revision
// wins the election.
type etcdLeaderParticipation struct {
	// s is our parent etcd topo Server
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

// WaitForLeadership is part of the topo.LeaderParticipation interface.
func (mp *etcdLeaderParticipation) WaitForLeadership() (context.Context, error) {
	// If Stop was already called, mp.done is closed, so we are interrupted.
	select {
	case <-mp.done:
		return nil, topo.NewError(topo.Interrupted, "Leadership")
	default:
	}

	electionPath := path.Join(electionsPath, mp.name)
	var ld topo.LockDescriptor

	// We use a cancelable context here. If stop is closed,
	// we just cancel that context.
	lockCtx, lockCancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-mp.s.running:
			return
		case <-mp.stop:
		}
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
	ld, err = mp.s.lock(lockCtx, electionPath, mp.id)
	if err != nil {
		// It can be that we were interrupted.
		return nil, err
	}

	// We got the lock. Return the lockContext. If Stop() is called,
	// it will cancel the lockCtx, and cancel the returned context.
	return lockCtx, nil
}

// Stop is part of the topo.LeaderParticipation interface
func (mp *etcdLeaderParticipation) Stop() {
	close(mp.stop)
	<-mp.done
}

// GetCurrentLeaderID is part of the topo.LeaderParticipation interface
func (mp *etcdLeaderParticipation) GetCurrentLeaderID(ctx context.Context) (string, error) {
	electionPath := path.Join(mp.s.root, electionsPath, mp.name)

	// Get the keys in the directory, older first.
	resp, err := mp.s.cli.Get(ctx, electionPath+"/",
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortAscend),
		clientv3.WithLimit(1))
	if err != nil {
		return "", convertError(err, electionPath)
	}
	if len(resp.Kvs) == 0 {
		// No key starts with this prefix, means nobody is the primary.
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

func (mp *etcdLeaderParticipation) WaitForNewLeader(ctx context.Context) (<-chan string, error) {
	electionPath := path.Join(mp.s.root, electionsPath, mp.name)

	notifications := make(chan string, 8)
	ctx, cancel := context.WithCancel(ctx)

	// Get the current leader
	initial, err := mp.s.cli.Get(ctx, electionPath+"/",
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortAscend),
		clientv3.WithLimit(1))
	if err != nil {
		cancel()
		return nil, err
	}

	if len(initial.Kvs) == 1 {
		leader := initial.Kvs[0].Value
		notifications <- string(leader)
	}

	// Create the Watcher.  We start watching from the response we
	// got, not from the file original version, as the server may
	// not have that much history.
	watcher := mp.s.cli.Watch(ctx, electionPath, clientv3.WithPrefix(), clientv3.WithRev(initial.Header.Revision))
	if watcher == nil {
		cancel()
		return nil, convertError(err, electionPath)
	}

	go func() {
		defer cancel()
		defer close(notifications)
		for {
			select {
			case <-mp.s.running:
				return
			case <-mp.done:
				return
			case <-ctx.Done():
				return
			case wresp, ok := <-watcher:
				if !ok || wresp.Canceled {
					return
				}

				currentLeader, err := mp.s.cli.Get(ctx, electionPath+"/",
					clientv3.WithPrefix(),
					clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortAscend),
					clientv3.WithLimit(1))
				if err != nil {
					continue
				}
				if len(currentLeader.Kvs) != 1 {
					continue
				}
				notifications <- string(currentLeader.Kvs[0].Value)
			}
		}
	}()

	return notifications, nil
}
