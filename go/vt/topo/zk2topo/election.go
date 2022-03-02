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

package zk2topo

import (
	"path"
	"sort"

	"context"

	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// This file contains the primary election code for zk2topo.Server.

// NewLeaderParticipation is part of the topo.Server interface.
// We use the full path: <root path>/election/<name>
func (zs *Server) NewLeaderParticipation(name, id string) (topo.LeaderParticipation, error) {
	ctx := context.TODO()

	zkPath := path.Join(zs.root, electionsPath, name)

	// Create the toplevel directory, OK if it exists already.
	// We will create the parent directory as well, but not more.
	if _, err := CreateRecursive(ctx, zs.conn, zkPath, nil, 0, zk.WorldACL(PermDirectory), 1); err != nil && err != zk.ErrNodeExists {
		return nil, convertError(err, zkPath)
	}

	result := &zkLeaderParticipation{
		zs:   zs,
		name: name,
		id:   []byte(id),
		done: make(chan struct{}),
	}
	result.stopCtx, result.stopCtxCancel = context.WithCancel(context.Background())
	return result, nil
}

// zkLeaderParticipation implements topo.LeaderParticipation.
//
// We use a directory with files created as sequence and ephemeral,
// see https://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection
// From the toplevel election directory, we'll have one sub-directory
// per name, with the sequence files in there. Each sequence file also contains
// the id.
type zkLeaderParticipation struct {
	// zs is our parent zk topo Server
	zs *Server

	// name is the name of this LeaderParticipation
	name string

	// id is the process's current id.
	id []byte

	// stopCtx is a context that is closed when Stop is called.
	stopCtx context.Context

	// stopCtxCancel is the cancel function to call to cancel stopCtx.
	stopCtxCancel context.CancelFunc

	// done is a channel closed when the stop operation is done.
	done chan struct{}
}

// WaitForLeadership is part of the topo.LeaderParticipation interface.
func (mp *zkLeaderParticipation) WaitForLeadership() (context.Context, error) {
	// If Stop was already called, mp.done is closed, so we are interrupted.
	select {
	case <-mp.done:
		return nil, topo.NewError(topo.Interrupted, "Leadership")
	default:
	}

	ctx := context.TODO()
	zkPath := path.Join(mp.zs.root, electionsPath, mp.name)

	// Fast path if Stop was already called.
	select {
	case <-mp.stopCtx.Done():
		close(mp.done)
		return nil, topo.NewError(topo.Interrupted, "Leadership")
	default:
	}

	// Create the current proposal.
	proposal, err := mp.zs.conn.Create(ctx, zkPath+"/", mp.id, zk.FlagSequence|zk.FlagEphemeral, zk.WorldACL(PermFile))
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot create proposal file in %v", zkPath)
	}

	// Wait until we are it, or we are interrupted. Using a
	// small-ish time out so it gets exercised faster (as opposed
	// to crashing after a day of use).
	err = obtainQueueLock(mp.stopCtx, mp.zs.conn, proposal)
	switch err {
	case nil:
		break
	case context.Canceled:
		close(mp.done)
		return nil, topo.NewError(topo.Interrupted, "Leadership")
	default:
		// something else went wrong
		return nil, err
	}

	// we got the lock, create our background context
	ctx, cancel := context.WithCancel(context.Background())
	go mp.watchLeadership(ctx, mp.zs.conn, proposal, cancel)
	return ctx, nil
}

// watchLeadership is the background go routine we run while we are the primary.
// We will do two things:
// - watch for changes to the proposal file. If anything happens there,
//   it most likely means we lost the ZK session, so we want to stop
//   being the primary.
// - wait for mp.stop.
func (mp *zkLeaderParticipation) watchLeadership(ctx context.Context, conn *ZkConn, proposal string, cancel context.CancelFunc) {
	// any interruption of this routine means we're not primary any more.
	defer cancel()

	// get to work watching our own proposal
	_, stats, events, err := conn.GetW(ctx, proposal)
	if err != nil {
		log.Warningf("Cannot watch proposal while being Leader, stopping: %v", err)
		return
	}

	select {
	case <-mp.stopCtx.Done():
		// we were asked to stop, we're done. Remove our node.
		log.Infof("Canceling leadership '%v' upon Stop.", mp.name)

		if err := conn.Delete(ctx, proposal, stats.Version); err != nil {
			log.Warningf("Error deleting our proposal %v: %v", proposal, err)
		}
		close(mp.done)

	case e := <-events:
		// something happened to our proposal, that can only be bad.
		log.Warningf("Watch on proposal triggered, canceling leadership '%v': %v", mp.name, e)
	}
}

// Stop is part of the topo.LeaderParticipation interface
func (mp *zkLeaderParticipation) Stop() {
	mp.stopCtxCancel()
	<-mp.done
}

// GetCurrentLeaderID is part of the topo.LeaderParticipation interface.
// We just read the smallest (first) node content, that is the id.
func (mp *zkLeaderParticipation) GetCurrentLeaderID(ctx context.Context) (string, error) {
	zkPath := path.Join(mp.zs.root, electionsPath, mp.name)

	for {
		children, _, err := mp.zs.conn.Children(ctx, zkPath)
		if err != nil {
			return "", convertError(err, zkPath)
		}
		if len(children) == 0 {
			// no current primary
			return "", nil
		}
		sort.Strings(children)

		childPath := path.Join(zkPath, children[0])
		data, _, err := mp.zs.conn.Get(ctx, childPath)
		if err != nil {
			if err == zk.ErrNoNode {
				// primary terminated in front of our own eyes,
				// try again
				continue
			}
			return "", convertError(err, zkPath)
		}

		return string(data), nil
	}
}
