package consultopo

import (
	"fmt"
	"path"

	log "github.com/golang/glog"
	"github.com/hashicorp/consul/api"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

func (s *Server) lock(ctx context.Context, c *cellClient, lockPath, contents string) (string, error) {
	// Build the lock structure.
	l, err := s.global.client.LockOpts(&api.LockOptions{
		Key:   lockPath,
		Value: []byte(contents),
	})
	if err != nil {
		return "", err
	}

	// Wait until we are the only ones in this client trying to
	// lock that path.
	c.mu.Lock()
	li, ok := c.locks[lockPath]
	for ok {
		// Unlock, wait for something to change.
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", convertError(ctx.Err())
		case <-li.done:
		}

		// The original locker is gone, try to get it again
		c.mu.Lock()
		li, ok = c.locks[lockPath]
	}
	li = &lockInstance{
		lock: l,
		done: make(chan struct{}),
	}
	c.locks[lockPath] = li
	c.mu.Unlock()

	// We are the only ones trying to lock now.
	// FIXME(alainjobart) We don't look at the 'lost' channel
	// returned here. We need to fix this in our code base, to add
	// APIs to make sure a lock is still held.
	_, err = l.Lock(ctx.Done())
	if err != nil {
		// Failed to lock, give up our slot in locks map.
		// Close the channel to unblock anyone else.
		c.mu.Lock()
		delete(c.locks, lockPath)
		c.mu.Unlock()
		close(li.done)

		return "", err
	}

	// We got the lock, we're good.
	return lockPath, nil
}

// unlock releases a lock acquired by lock() on the given directory.
// The string returned by lock() should be passed as the actionPath.
func (s *Server) unlock(ctx context.Context, c *cellClient, lockPath, actionPath string) error {
	// Sanity check.
	if lockPath != actionPath {
		return fmt.Errorf("unlock: actionPath doesn't match directory being unlocked: %q != %q", lockPath, actionPath)
	}

	c.mu.Lock()
	li, ok := c.locks[lockPath]
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("unlock: lock %v not held", lockPath)
	}

	// Try to unlock our lock. We will clean up our entry anyway.
	unlockErr := li.lock.Unlock()

	c.mu.Lock()
	delete(c.locks, lockPath)
	c.mu.Unlock()
	close(li.done)

	return unlockErr
}

// LockKeyspaceForAction implements topo.Server.
func (s *Server) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	// Check the keyspace exists first.
	keyspacePath := path.Join(keyspacesPath, keyspace, topo.KeyspaceFile)
	_, _, err := s.Get(ctx, topo.GlobalCell, keyspacePath)
	if err != nil {
		return "", err
	}

	return s.lock(ctx, s.global, path.Join(s.global.root, keyspacesPath, keyspace, locksFilename), contents)
}

// UnlockKeyspaceForAction implements topo.Server.
func (s *Server) UnlockKeyspaceForAction(ctx context.Context, keyspace, actionPath, results string) error {
	log.Infof("results of %v: %v", actionPath, results)
	return s.unlock(ctx, s.global, path.Join(s.global.root, keyspacesPath, keyspace, locksFilename), actionPath)
}

// LockShardForAction implements topo.Server.
func (s *Server) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	shardPath := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardFile)
	_, _, err := s.Get(ctx, topo.GlobalCell, shardPath)
	if err != nil {
		return "", err
	}

	return s.lock(ctx, s.global, path.Join(s.global.root, keyspacesPath, keyspace, shardsPath, shard, locksFilename), contents)
}

// UnlockShardForAction implements topo.Server.
func (s *Server) UnlockShardForAction(ctx context.Context, keyspace, shard, actionPath, results string) error {
	log.Infof("results of %v: %v", actionPath, results)
	return s.unlock(ctx, s.global, path.Join(s.global.root, keyspacesPath, keyspace, shardsPath, shard, locksFilename), actionPath)
}
