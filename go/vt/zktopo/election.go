// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"fmt"
	"path"
	"time"

	log "github.com/golang/glog"
	zookeeper "github.com/samuel/go-zookeeper/zk"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
	"golang.org/x/net/context"
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
	_, err := zk.CreateRecursive(zkts.zconn, electionPath, "", 0, zookeeper.WorldACL(zookeeper.PermAll))
	if err != nil && err != zookeeper.ErrNodeExists {
		return nil, convertError(err)
	}

	return &zkMasterParticipation{
		zkts:     zkts,
		name:     name,
		id:       id,
		shutdown: make(chan struct{}),
	}, nil
}

// zkMasterParticipation implements topo.MasterParticipation.
//
// We use a directory with files created as sequence and ephemeral,
// see https://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection
type zkMasterParticipation struct {
	// zkts is our parent zk topo Server
	zkts *Server

	// name is the name of this MasterParticipation
	name string

	// id is the process's current id.
	id string

	// shutdown is a channel closed when Shutdown is called.
	shutdown chan struct{}
}

// WaitForMaster is part of the topo.MasterParticipation interface.
func (mp *zkMasterParticipation) WaitForMaster() (context.Context, error) {
	electionPath := path.Join(GlobalElectionPath, mp.name)

	// fast path if Shutdown was already called
	select {
	case <-mp.shutdown:
		return nil, topo.ErrInterrupted
	default:
	}

	// create the current proposal
	proposal, err := mp.zkts.zconn.Create(electionPath+"/", mp.id, zookeeper.FlagSequence|zookeeper.FlagEphemeral, zookeeper.WorldACL(zk.PermFile))
	if err != nil {
		return nil, fmt.Errorf("cannot create proposal file in %v: %v", electionPath, err)
	}

	// wait until we are it, or we are interrupted
	for {
		err = zk.ObtainQueueLock(mp.zkts.zconn, proposal, 24*time.Hour, mp.shutdown)
		if err == nil {
			// we got the lock, move on
			break
		}
		if err == zk.ErrInterrupted {
			// mp.shutdown is closed, we should return
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
	go mp.whileMaster(proposal, cancel)
	return ctx, nil
}

// whileMaster is the background go routine we run while we are the master.
// We will do two things:
// - watch for changes to the proposal file. If anything happens there,
//   it most likely means we lost the ZK session, so we want to stop
//   being the master.
// - wait for mp.shutdown.
func (mp *zkMasterParticipation) whileMaster(proposal string, cancel context.CancelFunc) {
	// any interruption of this routine means we're not master any more.
	defer cancel()

	// get to work watching our own proposal
	_, stats, events, err := mp.zkts.zconn.GetW(proposal)
	if err != nil {
		log.Warningf("Cannot watch proposal whle being master, stopping: %v", err)
		return
	}

	select {
	case <-mp.shutdown:
		// we were asked to shutdown, we're done. Remove our node.
		log.Infof("Canceling leadership '%v' upon Shutdown.", mp.name)

		if err := mp.zkts.zconn.Delete(proposal, stats.Version); err != nil {
			log.Warningf("Error deleting our proposal %v: %v", proposal, err)
		}

	case e := <-events:
		// something happened to our proposal, that can only be bad.
		log.Warningf("Watch on proposal triggered, canceling leadership '%v': %v", mp.name, e)
	}
}

// Shutdown is part of the topo.MasterParticipation interface
func (mp *zkMasterParticipation) Shutdown() {
	close(mp.shutdown)
}

// GetCurrentMasterID is part of the topo.MasterParticipation interface
func (mp *zkMasterParticipation) GetCurrentMasterID() (string, error) {
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

		return data, nil
	}
}
