package memorytopo

import (
	"path"

	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// NewLeaderParticipation is part of the topo.Server interface
func (c *Conn) NewLeaderParticipation(name, id string) (topo.LeaderParticipation, error) {
	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	// Make sure the global path exists.
	electionPath := path.Join(electionsPath, name)
	if n := c.factory.getOrCreatePath(c.cell, electionPath); n == nil {
		return nil, topo.NewError(topo.NoNode, electionPath)
	}

	return &cLeaderParticipation{
		c:    c,
		name: name,
		id:   id,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}, nil
}

// cLeaderParticipation implements topo.LeaderParticipation.
//
// We use a directory (in global election path, with the name) with
// ephemeral files in it, that contains the id.  The oldest revision
// wins the election.
type cLeaderParticipation struct {
	// c is our memorytopo connection
	c *Conn

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
func (mp *cLeaderParticipation) WaitForLeadership() (context.Context, error) {
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
		<-mp.stop
		if ld != nil {
			if err := ld.Unlock(context.Background()); err != nil {
				log.Errorf("failed to unlock LockDescriptor %v: %v", electionPath, err)
			}
		}
		lockCancel()
		close(mp.done)
	}()

	// Try to get the primaryship, by getting a lock.
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

// Stop is part of the topo.LeaderParticipation interface
func (mp *cLeaderParticipation) Stop() {
	close(mp.stop)
	<-mp.done
}

// GetCurrentLeaderID is part of the topo.LeaderParticipation interface
func (mp *cLeaderParticipation) GetCurrentLeaderID(ctx context.Context) (string, error) {
	electionPath := path.Join(electionsPath, mp.name)

	mp.c.factory.mu.Lock()
	defer mp.c.factory.mu.Unlock()

	n := mp.c.factory.nodeByPath(mp.c.cell, electionPath)
	if n == nil {
		return "", nil
	}

	return n.lockContents, nil
}
