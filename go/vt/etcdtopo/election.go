// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"fmt"
	"path"
	"time"

	"github.com/coreos/go-etcd/etcd"
	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// NewMasterParticipation is part of the topo.Server interface
func (s *Server) NewMasterParticipation(name, id string) (topo.MasterParticipation, error) {
	return &etcdMasterParticipation{
		s:    s,
		name: name,
		id:   id,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}, nil
}

// etcdMasterParticipation implements topo.MasterParticipation.
//
// We create a single file and use etcd's compare&swap with TTLs.
// The file is in the global election path, is named after the name,
// and contains the id.
type etcdMasterParticipation struct {
	// s is our parent etcd topo Server
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
func (mp *etcdMasterParticipation) WaitForMastership() (context.Context, error) {
	electionPath := path.Join(electionDirPath, mp.name)

	for {
		// fast path if Stop was already called
		select {
		case <-mp.stop:
			close(mp.done)
			return nil, topo.ErrInterrupted
		default:
		}

		// We have to try to take the lock, until we either get it,
		// or stop is closed.
		// Create will fail if the lock file already exists.
		client := mp.s.getGlobal()
		resp, err := client.Create(electionPath, mp.id, uint64(*lockTTL/time.Second))
		if err == nil {
			if resp.Node == nil {
				return nil, ErrBadResponse
			}

			// We got the lock. Start a heartbeat goroutine.
			// Its purpose is to remove the lock when we're told
			// to stop.
			lockID, done := locks.add(client, resp.Node)
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				// wait until one of the two conditions
				select {
				case <-mp.stop:
					// we're told to stop, remove our lock
					locks.remove(lockID)
					close(mp.done)
				case err := <-done:
					// we lost the lock
					log.Warningf("Lost lock for %v: %v", mp.name, err)
				}
				cancel()
			}()

			return ctx, nil
		}

		// If it fails for any reason other than ErrNodeExists
		// (meaning the lock is already held), then just give up.
		if topoErr := convertError(err); topoErr != topo.ErrNodeExists {
			return nil, topoErr
		}
		etcdErr, ok := err.(*etcd.EtcdError)
		if !ok {
			return nil, fmt.Errorf("error from etcd client has wrong type: got %#v, want %T", err, etcdErr)
		}

		// The lock is already being held.
		// Wait for the lock file to be deleted, then try again.
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			// This go routine cancels out the context if Stop()
			// it called.
			select {
			case <-mp.stop:
				cancel()
			case <-ctx.Done():
			}
		}()
		err = waitForLock(ctx, client, electionPath, etcdErr.Index+1)
		cancel()
		if err != nil {
			// This can be topo.ErrInterrupted if we canceled the
			// context.
			if err == topo.ErrInterrupted {
				close(mp.done)
			}
			return nil, err
		}
	}
}

// Stop is part of the topo.MasterParticipation interface
func (mp *etcdMasterParticipation) Stop() {
	close(mp.stop)
	<-mp.done
}

// GetCurrentMasterID is part of the topo.MasterParticipation interface
func (mp *etcdMasterParticipation) GetCurrentMasterID(ctx context.Context) (string, error) {
	electionPath := path.Join(electionDirPath, mp.name)

	resp, err := mp.s.getGlobal().Get(electionPath, false /* sort */, false /* recursive */)
	if err != nil {
		err = convertError(err)
		if err == topo.ErrNoNode {
			return "", nil
		}
		return "", err
	}
	if resp.Node == nil {
		return "", ErrBadResponse
	}

	return string(resp.Node.Value), nil
}
