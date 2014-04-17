// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/fileutil"
	"launchpad.net/gozk/zookeeper"
)

var (
	// This error is returned by functions that wait for a result
	// when they are interrupted.
	ErrInterrupted = errors.New("zkutil: obtaining lock was interrupted")

	// This error is returned by functions that wait for a result
	// when the timeout value is reached.
	ErrTimeout = errors.New("zkutil: obtaining lock timed out")
)

const (
	// PERM_DIRECTORY are default permissions for a node.
	PERM_DIRECTORY = zookeeper.PERM_ADMIN | zookeeper.PERM_CREATE | zookeeper.PERM_DELETE | zookeeper.PERM_READ
	// PERM_FILE allows a zk node to emulate file behavior by disallowing child nodes.
	PERM_FILE = zookeeper.PERM_ADMIN | zookeeper.PERM_READ | zookeeper.PERM_WRITE
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// IsDirectory returns if this node should be treated as a directory.
func IsDirectory(aclv []zookeeper.ACL) bool {
	for _, acl := range aclv {
		if acl.Perms != PERM_DIRECTORY {
			return false
		}
	}
	return true
}

// Create a path and any pieces required, think mkdir -p.
// Intermediate znodes are always created empty.
func CreateRecursive(zconn Conn, zkPath, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	parts := strings.Split(zkPath, "/")
	if parts[1] != MagicPrefix {
		return "", fmt.Errorf("zkutil: non /%v path: %v", MagicPrefix, zkPath)
	}

	pathCreated, err = zconn.Create(zkPath, value, flags, aclv)
	if zookeeper.IsError(err, zookeeper.ZNONODE) {
		// Make sure that nodes are either "file" or "directory" to mirror file system
		// semantics.
		dirAclv := make([]zookeeper.ACL, len(aclv))
		for i, acl := range aclv {
			dirAclv[i] = acl
			dirAclv[i].Perms = PERM_DIRECTORY
		}
		_, err = CreateRecursive(zconn, path.Dir(zkPath), "", flags, dirAclv)
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			return "", err
		}
		pathCreated, err = zconn.Create(zkPath, value, flags, aclv)
	}
	return
}

func CreateOrUpdate(zconn Conn, zkPath, value string, flags int, aclv []zookeeper.ACL, recursive bool) (pathCreated string, err error) {
	if recursive {
		pathCreated, err = CreateRecursive(zconn, zkPath, value, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	} else {
		pathCreated, err = zconn.Create(zkPath, value, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	}
	if err != nil && zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		pathCreated = ""
		_, err = zconn.Set(zkPath, value, -1)
	}
	return
}

type pathItem struct {
	path string
	err  error
}

func ChildrenRecursive(zconn Conn, zkPath string) ([]string, error) {
	var err error
	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	pathList := make([]string, 0, 32)
	children, _, err := zconn.Children(zkPath)
	if err != nil {
		return nil, err
	}

	for _, child := range children {
		wg.Add(1)
		go func(child string) {
			childPath := path.Join(zkPath, child)
			rChildren, zkErr := ChildrenRecursive(zconn, childPath)
			if zkErr != nil {
				// If other processes are deleting nodes, we need to ignore
				// the missing nodes.
				if !zookeeper.IsError(zkErr, zookeeper.ZNONODE) {
					mutex.Lock()
					err = zkErr
					mutex.Unlock()
				}
			} else {
				mutex.Lock()
				pathList = append(pathList, child)
				for _, rChild := range rChildren {
					pathList = append(pathList, path.Join(child, rChild))
				}
				mutex.Unlock()
			}
			wg.Done()
		}(child)
	}

	wg.Wait()

	mutex.Lock()
	defer mutex.Unlock()
	if err != nil {
		return nil, err
	}
	return pathList, nil
}

// resolve paths like:
// /zk/nyc/vt/tablets/*/action
// /zk/global/vt/keyspaces/*/shards/*/action
// /zk/*/vt/tablets/*/action
// into real existing paths
//
// If you send paths that don't contain any wildcard and
// don't exist, this function will return an empty array.
func ResolveWildcards(zconn Conn, zkPaths []string) ([]string, error) {
	// check all the paths start with /zk/ before doing anything
	// time consuming
	// relax this in case we are not talking to a metaconn and
	// just want to talk to a specified instance.
	// for _, zkPath := range zkPaths {
	// 	if _, err := ZkCellFromZkPath(zkPath); err != nil {
	// 		return nil, err
	// 	}
	// }

	results := make([][]string, len(zkPaths))
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	var firstError error

	for i, zkPath := range zkPaths {
		wg.Add(1)
		parts := strings.Split(zkPath, "/")
		go func(i int) {
			defer wg.Done()
			subResult, err := resolveRecursive(zconn, parts, true)
			if err != nil {
				mu.Lock()
				if firstError != nil {
					log.Infof("Multiple error: %v", err)
				} else {
					firstError = err
				}
				mu.Unlock()
			} else {
				results[i] = subResult
			}
		}(i)
	}

	wg.Wait()
	if firstError != nil {
		return nil, firstError
	}

	result := make([]string, 0, 32)
	for i := 0; i < len(zkPaths); i++ {
		subResult := results[i]
		if subResult != nil {
			result = append(result, subResult...)
		}
	}

	return result, nil
}

func resolveRecursive(zconn Conn, parts []string, toplevel bool) ([]string, error) {
	for i, part := range parts {
		if fileutil.HasWildcard(part) {
			var children []string
			if i == 2 {
				children = ZkKnownCells(false)
			} else {
				zkParentPath := strings.Join(parts[:i], "/")
				var err error
				children, _, err = zconn.Children(zkParentPath)
				if err != nil {
					// we asked for something like
					// /zk/cell/aaa/* and
					// /zk/cell/aaa doesn't exist
					// -> return empty list, no error
					// (note we check both a regular zk
					// error and the error the test
					// produces)
					if zookeeper.IsError(err, zookeeper.ZNONODE) {
						return nil, nil
					}
					// otherwise we return the error
					return nil, err
				}
			}
			sort.Strings(children)

			results := make([][]string, len(children))
			wg := &sync.WaitGroup{}
			mu := &sync.Mutex{}
			var firstError error

			for j, child := range children {
				matched, err := path.Match(part, child)
				if err != nil {
					return nil, err
				}
				if matched {
					// we have a match!
					wg.Add(1)
					newParts := make([]string, len(parts))
					copy(newParts, parts)
					newParts[i] = child
					go func(j int) {
						defer wg.Done()
						subResult, err := resolveRecursive(zconn, newParts, false)
						if err != nil {
							mu.Lock()
							if firstError != nil {
								log.Infof("Multiple error: %v", err)
							} else {
								firstError = err
							}
							mu.Unlock()
						} else {
							results[j] = subResult
						}
					}(j)
				}
			}

			wg.Wait()
			if firstError != nil {
				return nil, firstError
			}

			result := make([]string, 0, 32)
			for j := 0; j < len(children); j++ {
				subResult := results[j]
				if subResult != nil {
					result = append(result, subResult...)
				}
			}

			// we found a part that is a wildcard, we
			// added the children already, we're done
			return result, nil
		}
	}

	// no part contains a wildcard, add the path if it exists, and done
	path := strings.Join(parts, "/")
	if toplevel {
		// for whatever the user typed at the toplevel, we don't
		// check it exists or not, we just return it
		return []string{path}, nil
	}

	// this is an expanded path, we need to check if it exists
	stat, err := zconn.Exists(path)
	if err != nil {
		return nil, err
	}
	if stat != nil {
		return []string{path}, nil
	}
	return nil, nil
}

func DeleteRecursive(zconn Conn, zkPath string, version int) error {
	// version: -1 delete any version of the node at path - only applies to the top node
	err := zconn.Delete(zkPath, version)
	if err == nil {
		return nil
	}
	if !zookeeper.IsError(err, zookeeper.ZNOTEMPTY) {
		return err
	}
	// Remove the ability for other nodes to get created while we are trying to delete.
	// Otherwise, you can enter a race condition, or get starved out from deleting.
	err = zconn.SetACL(zkPath, zookeeper.WorldACL(zookeeper.PERM_ADMIN|zookeeper.PERM_DELETE|zookeeper.PERM_READ), version)
	if err != nil {
		return err
	}
	children, _, err := zconn.Children(zkPath)
	if err != nil {
		return err
	}
	for _, child := range children {
		err := DeleteRecursive(zconn, path.Join(zkPath, child), -1)
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return fmt.Errorf("zkutil: recursive delete failed: %v", err)
		}
	}

	err = zconn.Delete(zkPath, version)
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNOTEMPTY) {
		err = fmt.Errorf("zkutil: nodes getting recreated underneath delete (app race condition): %v", zkPath)
	}
	return err
}

// The lexically lowest node is the lock holder - verify that this
// path holds the lock.  Call this queue-lock because the semantics are
// a hybrid.  Normal zookeeper locks make assumptions about sequential
// numbering that don't hold when the data in a lock is modified.
// if the provided 'interrupted' chan is closed, we'll just stop waiting
// and return an interruption error
func ObtainQueueLock(zconn Conn, zkPath string, wait time.Duration, interrupted chan struct{}) error {
	queueNode := path.Dir(zkPath)
	lockNode := path.Base(zkPath)

	timer := time.NewTimer(wait)
trylock:
	children, _, err := zconn.Children(queueNode)
	if err != nil {
		return fmt.Errorf("zkutil: trylock failed %v", err)
	}
	sort.Strings(children)
	if len(children) > 0 {
		if children[0] == lockNode {
			return nil
		}
		if wait > 0 {
			prevLock := ""
			for i := 1; i < len(children); i++ {
				if children[i] == lockNode {
					prevLock = children[i-1]
					break
				}
			}
			if prevLock == "" {
				return fmt.Errorf("zkutil: no previous queue node found: %v", zkPath)
			}

			zkPrevLock := path.Join(queueNode, prevLock)
			stat, watch, err := zconn.ExistsW(zkPrevLock)
			if err != nil {
				return fmt.Errorf("zkutil: unable to watch queued node %v %v", zkPrevLock, err)
			}
			if stat == nil {
				goto trylock
			}
			select {
			case <-timer.C:
				break
			case <-interrupted:
				return ErrInterrupted
			case <-watch:
				// The precise event doesn't matter - try to read again regardless.
				goto trylock
			}
		}
		return ErrTimeout
	}
	return fmt.Errorf("zkutil: empty queue node: %v", queueNode)
}

// Close the release channel when you want to clean up nicely.
func CreatePidNode(zconn Conn, zkPath string, contents string, done chan struct{}) error {
	// On the first try, assume the cluster is up and running, that will
	// help hunt down any config issues present at startup
	if _, err := zconn.Create(zkPath, contents, zookeeper.EPHEMERAL, zookeeper.WorldACL(PERM_FILE)); err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			err = zconn.Delete(zkPath, -1)
		}
		if err != nil {
			return fmt.Errorf("zkutil: failed deleting pid node: %v: %v", zkPath, err)
		}
		_, err = zconn.Create(zkPath, contents, zookeeper.EPHEMERAL, zookeeper.WorldACL(PERM_FILE))
		if err != nil {
			return fmt.Errorf("zkutil: failed creating pid node: %v: %v", zkPath, err)
		}
	}

	go func() {
		for {
			_, _, watch, err := zconn.GetW(zkPath)
			if err != nil {
				if zookeeper.IsError(err, zookeeper.ZNONODE) {
					_, err = zconn.Create(zkPath, contents, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
					if err != nil {
						log.Warningf("failed recreating pid node: %v: %v", zkPath, err)
					} else {
						log.Infof("recreated pid node: %v", zkPath)
						continue
					}
				} else {
					log.Warningf("failed reading pid node: %v", err)
				}
			} else {
				select {
				case event := <-watch:
					if event.Ok() && event.Type == zookeeper.EVENT_DELETED {
						// Most likely another process has started up. However,
						// there is a chance that an ephemeral node is deleted by
						// the session expiring, yet that same session gets a watch
						// notification. This seems like buggy behavior, but rather
						// than race too hard on the node, just wait a bit and see
						// if the situation resolves itself.
						log.Warningf("pid deleted: %v", zkPath)
					} else {
						log.Infof("pid node event: %v", event)
					}
					// break here and wait for a bit before attempting
				case <-done:
					log.Infof("pid watcher stopped on done: %v", zkPath)
					return
				}
			}
			select {
			// No one likes a thundering herd, least of all zookeeper.
			case <-time.After(5*time.Second + time.Duration(rand.Int63n(55e9))):
			case <-done:
				log.Infof("pid watcher stopped on done: %v", zkPath)
				return
			}
		}
	}()

	return nil
}

// ZLocker is an interface for a lock that can fail.
type ZLocker interface {
	Lock() error
	LockWithTimeout(wait time.Duration) error
	Unlock() error
	Interrupt()
}

// Experiment with a little bit of abstraction.
// FIMXE(msolo) This object may need a mutex to ensure it can be shared
// across goroutines.
type zMutex struct {
	mu          sync.Mutex
	zconn       Conn
	path        string // Path under which we try to create lock nodes.
	contents    string
	interrupted chan struct{}
	name        string // The name of the specific lock node we created.
	ephemeral   bool
}

// CreateMutex initializes an unaquired mutex. A mutex is released only
// by Unlock. You can clean up a mutex with delete, but you should be
// careful doing so.
func CreateMutex(zconn Conn, zkPath string) ZLocker {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err) // should never happen
	}
	pid := os.Getpid()
	contents := fmt.Sprintf(`{"hostname": "%v", "pid": %v}`, hostname, pid)

	return &zMutex{zconn: zconn, path: zkPath, contents: contents, interrupted: make(chan struct{})}
}

// Interrupt releases a lock that's held.
func (zm *zMutex) Interrupt() {
	select {
	case zm.interrupted <- struct{}{}:
	default:
		log.Warningf("zmutex interrupt blocked")
	}
}

// Lock returns nil when the lock is acquired.
func (zm *zMutex) Lock() error {
	return zm.LockWithTimeout(365 * 24 * time.Hour)
}

// LockWithTimeout returns nil when the lock is acquired. A lock is
// held if the file exists and you are the creator. Setting the wait
// to zero makes this a nonblocking lock check.
//
// FIXME(msolo) Disallow non-super users from removing the lock?
func (zm *zMutex) LockWithTimeout(wait time.Duration) (err error) {
	timer := time.NewTimer(wait)
	defer func() {
		if panicErr := recover(); panicErr != nil || err != nil {
			zm.deleteLock()
		}
	}()
	// Ensure the rendezvous node is here.
	// FIXME(msolo) Assuming locks are contended, it will be cheaper to assume this just
	// exists.
	_, err = CreateRecursive(zm.zconn, zm.path, "", 0, zookeeper.WorldACL(PERM_DIRECTORY))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}

	lockPrefix := path.Join(zm.path, "lock-")
	zflags := zookeeper.SEQUENCE
	if zm.ephemeral {
		zflags = zflags | zookeeper.EPHEMERAL
	}

createlock:
	lockCreated, err := zm.zconn.Create(lockPrefix, zm.contents, zflags, zookeeper.WorldACL(PERM_FILE))
	if err != nil {
		return err
	}
	name := path.Base(lockCreated)
	zm.mu.Lock()
	zm.name = name
	zm.mu.Unlock()

trylock:
	children, _, err := zm.zconn.Children(zm.path)
	if err != nil {
		return fmt.Errorf("zkutil: trylock failed %v", err)
	}
	sort.Strings(children)
	if len(children) == 0 {
		return fmt.Errorf("zkutil: empty lock: %v", zm.path)
	}

	if children[0] == name {
		// We are the lock owner.
		return nil
	}

	// This is the degenerate case of a nonblocking lock check. It's not optimal, but
	// also probably not worth optimizing.
	if wait == 0 {
		return ErrTimeout
	}
	prevLock := ""
	for i := 1; i < len(children); i++ {
		if children[i] == name {
			prevLock = children[i-1]
			break
		}
	}
	if prevLock == "" {
		// This is an interesting case. The node disappeared
		// underneath us, probably due to a session loss. We can
		// recreate the lock node (with a new sequence number) and
		// keep trying.
		log.Warningf("zkutil: no lock node found: %v/%v", zm.path, zm.name)
		goto createlock
	}

	zkPrevLock := path.Join(zm.path, prevLock)
	stat, watch, err := zm.zconn.ExistsW(zkPrevLock)
	if err != nil {
		// FIXME(msolo) Should this be a retry?
		return fmt.Errorf("zkutil: unable to watch previous lock node %v %v", zkPrevLock, err)
	}
	if stat == nil {
		goto trylock
	}
	select {
	case <-timer.C:
		return ErrTimeout
	case <-zm.interrupted:
		return ErrInterrupted
	case event := <-watch:
		log.Infof("zkutil: lock event: %v", event)
		// The precise event doesn't matter - try to read again regardless.
		goto trylock
	}
	panic("unexpected")
}

// Unlock returns nil if the lock was successfully
// released. Otherwise, it is most likely a zookeeper related error.
func (zm *zMutex) Unlock() error {
	return zm.deleteLock()
}

func (zm *zMutex) deleteLock() error {
	zm.mu.Lock()
	zpath := path.Join(zm.path, zm.name)
	zm.mu.Unlock()

	err := zm.zconn.Delete(zpath, -1)
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
		return err
	}
	return nil
}

// ZElector stores basic state for running an election.
type ZElector struct {
	*zMutex
	path   string
	leader string
}

func (ze *ZElector) isLeader() bool {
	return ze.leader == ze.name
}

type electionEvent struct {
	Event int
	Err   error
}

type backoffDelay struct {
	min   time.Duration
	max   time.Duration
	delay time.Duration
}

func newBackoffDelay(min, max time.Duration) *backoffDelay {
	return &backoffDelay{min, max, min}
}

func (bd *backoffDelay) NextDelay() time.Duration {
	delay := bd.delay
	bd.delay = 2 * bd.delay
	if bd.delay > bd.max {
		bd.delay = bd.max
	}
	return delay
}

func (bd *backoffDelay) Reset() {
	bd.delay = bd.min
}

// ElectorTask is the interface for a task that runs essentially
// forever or until something bad happens.  If a task must be stopped,
// it should be handled promptly - no second notification will be
// sent.
type ElectorTask interface {
	Run() error
	Stop()
	// Return true if interrupted, false if it died of natural causes.
	// An interrupted task indicates that the election should stop.
	Interrupted() bool
}

// CreateElection returns an initialized elector. An election is
// really a cycle of events. You are flip-flopping between leader and
// candidate. It's better to think of this as a stream of events that
// one needs to react to.
func CreateElection(zconn Conn, zkPath string) ZElector {
	zm := CreateMutex(zconn, path.Join(zkPath, "candidates")).(*zMutex)
	zm.ephemeral = true
	return ZElector{zMutex: zm, path: zkPath}
}

// RunTask returns nil when the underlyingtask ends or the error it
// generated.
func (ze *ZElector) RunTask(task ElectorTask) error {
	delay := newBackoffDelay(100*time.Millisecond, 1*time.Minute)
	leaderPath := path.Join(ze.path, "leader")
	for {
		_, err := CreateRecursive(ze.zconn, leaderPath, "", 0, zookeeper.WorldACL(PERM_FILE))
		if err == nil || zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			break
		}
		log.Warningf("election leader create failed: %v", err)
		time.Sleep(delay.NextDelay())
	}

	for {
		err := ze.Lock()
		if err != nil {
			log.Warningf("election lock failed: %v", err)
			if err == ErrInterrupted {
				return ErrInterrupted
			}
			continue
		}
		// Confirm your win and deliver acceptance speech. This notifies
		// listeners who will have been watching the leader node for
		// changes.
		_, err = ze.zconn.Set(leaderPath, ze.contents, -1)
		if err != nil {
			log.Warningf("election promotion failed: %v", err)
			continue
		}

		log.Infof("election promote leader %v", leaderPath)
		taskErrChan := make(chan error)
		go func() {
			taskErrChan <- task.Run()
		}()

	watchLeader:
		// Watch the leader so we can get notified if something goes wrong.
		data, _, watch, err := ze.zconn.GetW(leaderPath)
		if err != nil {
			log.Warningf("election unable to watch leader node %v %v", leaderPath, err)
			// FIXME(msolo) Add delay
			goto watchLeader
		}

		if data != ze.contents {
			log.Warningf("election unable to promote leader")
			task.Stop()
			// We won the election, but we didn't become the leader. How is that possible?
			// (see Bush v. Gore for some inspiration)
			// It means:
			//   1. Someone isn't playing by the election rules (a bad actor).
			//      Hard to detect - let's assume we don't have this problem. :)
			//   2. We lost our connection somehow and the ephemeral lock was cleared,
			//      allowing someone else to win the election.
			continue
		}

		// This is where we start our target process and watch for its failure.
	waitForEvent:
		select {
		case <-ze.interrupted:
			log.Infof("election interrupted - stop child process")
			task.Stop()
			// Once the process dies from the signal, this will all tear down.
			goto waitForEvent
		case taskErr := <-taskErrChan:
			// If our code fails, unlock to trigger an election.
			log.Infof("election child process ended: %v", taskErr)
			ze.Unlock()
			if task.Interrupted() {
				log.Warningf("election child process interrupted - stepping down")
				return ErrInterrupted
			}
			continue
		case zevent := <-watch:
			// We had a zookeeper connection hiccup.  We have a few choices,
			// but it depends on the constraints and the events.
			//
			// If we get SESSION_EXPIRED our connection loss triggered an
			// election that we won't have won and the thus the lock was
			// automatically freed. We have no choice but to start over.
			if zevent.State == zookeeper.STATE_EXPIRED_SESSION {
				log.Warningf("election leader watch expired")
				task.Stop()
				continue
			}

			// Otherwise, we had an intermittent issue or something touched
			// the node. Either we lost our position or someone broke
			// protocol and touched the leader node.  We just reconnect and
			// revalidate. In the meantime, assume we are still the leader
			// until we determine otherwise.
			//
			// On a reconnect we will be able to see the leader
			// information. If we still hold the position, great. If not, we
			// kill the associated process.
			//
			// On a leader node change, we need to perform the same
			// validation. It's possible an election completes without the
			// old leader realizing he is out of touch.
			log.Warningf("election leader watch event %v", zevent)
			goto watchLeader
		}
	}
	panic("unreachable")
}
