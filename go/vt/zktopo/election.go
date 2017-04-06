// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"fmt"
	"path"
	"sort"
	"time"

	log "github.com/golang/glog"
	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
)

/*
This file contains the master election code for zktopo.Server
*/

const (
	// GlobalElectionPath is the path used to store global
	// information for master participation in ZK. Exported for tests.
	GlobalElectionPath = "/zk/global/vt/election"
)

// NewMasterParticipation is part of the topo.Server interface
func (zkts *Server) NewMasterParticipation(name, id string) (topo.MasterParticipation, error) {
	electionPath := path.Join(GlobalElectionPath, name)

	// create the toplevel directory, OK if it exists already.
	_, err := zk.CreateRecursive(zkts.zconn, electionPath, nil, 0, zookeeper.WorldACL(zookeeper.PermAll))
	if err != nil && err != zookeeper.ErrNodeExists {
		return nil, convertError(err)
	}

	return &zkMasterParticipation{
		zkts: zkts,
		name: name,
		id:   []byte(id),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}, nil
}

// zkMasterParticipation implements topo.MasterParticipation.
//
// We use a directory with files created as sequence and ephemeral,
// see https://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection
// From the toplevel election directory, we'll have one sub-directory
// per name, with the sequence files in there. Each sequence file also contains
// the id.
type zkMasterParticipation struct {
	// zkts is our parent zk topo Server
	zkts *Server

	// name is the name of this MasterParticipation
	name string

	// id is the process's current id.
	id []byte

	// stop is a channel closed when stop is called.
	stop chan struct{}

	// done is a channel closed when the stop operation is done.
	done chan struct{}
}

// WaitForMastership is part of the topo.MasterParticipation interface.
func (mp *zkMasterParticipation) WaitForMastership() (context.Context, error) {
	electionPath := path.Join(GlobalElectionPath, mp.name)

	// fast path if Stop was already called
	select {
	case <-mp.stop:
		close(mp.done)
		return nil, topo.ErrInterrupted
	default:
	}

	// create the current proposal
	proposal, err := mp.zkts.zconn.Create(electionPath+"/", mp.id, zookeeper.FlagSequence|zookeeper.FlagEphemeral, zookeeper.WorldACL(zk.PermFile))
	if err != nil {
		return nil, fmt.Errorf("cannot create proposal file in %v: %v", electionPath, err)
	}

	// Wait until we are it, or we are interrupted. Using a
	// small-ish time out so it gets exercised faster (as opposed
	// to crashing after a day of use).
	for {
		err = zk.ObtainQueueLock(mp.zkts.zconn, proposal, 5*time.Minute, mp.stop)
		if err == nil {
			// we got the lock, move on
			break
		}
		if err == zk.ErrInterrupted {
			// mp.stop is closed, we should return
			close(mp.done)
			return nil, topo.ErrInterrupted
		}
		if err == zk.ErrTimeout {
			// we try again
			continue
		}
		// something else went wrong
		return nil, err
	}

	// we got the lock, create our background context
	ctx, cancel := context.WithCancel(context.Background())
	go mp.watchMastership(proposal, cancel)
	return ctx, nil
}

// watchMastership is the background go routine we run while we are the master.
// We will do two things:
// - watch for changes to the proposal file. If anything happens there,
//   it most likely means we lost the ZK session, so we want to stop
//   being the master.
// - wait for mp.stop.
func (mp *zkMasterParticipation) watchMastership(proposal string, cancel context.CancelFunc) {
	// any interruption of this routine means we're not master any more.
	defer cancel()

	// get to work watching our own proposal
	_, stats, events, err := mp.zkts.zconn.GetW(proposal)
	if err != nil {
		log.Warningf("Cannot watch proposal while being master, stopping: %v", err)
		return
	}

	select {
	case <-mp.stop:
		// we were asked to stop, we're done. Remove our node.
		log.Infof("Canceling leadership '%v' upon Stop.", mp.name)

		if err := mp.zkts.zconn.Delete(proposal, stats.Version); err != nil {
			log.Warningf("Error deleting our proposal %v: %v", proposal, err)
		}
		close(mp.done)

	case e := <-events:
		// something happened to our proposal, that can only be bad.
		log.Warningf("Watch on proposal triggered, canceling leadership '%v': %v", mp.name, e)
	}
}

// Stop is part of the topo.MasterParticipation interface
func (mp *zkMasterParticipation) Stop() {
	close(mp.stop)
	<-mp.done
}

// GetCurrentMasterID is part of the topo.MasterParticipation interface.
// We just read the smallest (first) node content, that is the id.
func (mp *zkMasterParticipation) GetCurrentMasterID(ctx context.Context) (string, error) {
	electionPath := path.Join(GlobalElectionPath, mp.name)

	for {
		children, _, err := mp.zkts.zconn.Children(electionPath)
		if err != nil {
			return "", convertError(err)
		}
		if len(children) == 0 {
			// no current master
			return "", nil
		}
		sort.Strings(children)

		childPath := path.Join(electionPath, children[0])
		data, _, err := mp.zkts.zconn.Get(childPath)
		if err != nil {
			if err == zookeeper.ErrNoNode {
				// master terminated in front of our own eyes,
				// try again
				continue
			}
			return "", convertError(err)
		}

		return string(data), nil
	}
}
