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

package topo

import (
	"context"
	"sort"
)

// Conn defines the interface that must be implemented by topology
// plug-ins to be used with Vitess.
//
// Zookeeper is a good example of an implementation, as defined in
// go/vt/topo/zk2topo.
//
// This API is very generic, and key/value store oriented.  We use
// regular paths for object names, and we can list all immediate
// children of a path. All paths sent through this API are relative
// paths, from the root directory of the cell.
//
// The Conn objects are created by the Factory implementations.
type Conn interface {
	//
	// Directory support
	//

	// ListDir returns the entries in a directory.  The returned
	// list should be sorted by entry.Name.
	// If there are no files under the provided path, returns ErrNoNode.
	// dirPath is a path relative to the root directory of the cell.
	// If full is set, we want all the fields in DirEntry to be filled in.
	// If full is not set, only Name will be used. This is intended for
	// implementations where getting more than the names is more expensive,
	// as in most cases only the names are needed.
	ListDir(ctx context.Context, dirPath string, full bool) ([]DirEntry, error)

	//
	// File support
	// if version == nil, then it’s an unconditional update / delete.
	//

	// Create creates the initial version of a file.
	// Returns ErrNodeExists if the file exists.
	// filePath is a path relative to the root directory of the cell.
	Create(ctx context.Context, filePath string, contents []byte) (Version, error)

	// Update updates the file with the provided filename with the
	// new content.
	// If version is nil, it is an unconditional update
	// (which is then the same as a Create is the file doesn't exist).
	// filePath is a path relative to the root directory of the cell.
	// It returns the new Version of the file after update.
	// Returns ErrBadVersion if the provided version is not current.
	Update(ctx context.Context, filePath string, contents []byte, version Version) (Version, error)

	// Get returns the content and version of a file.
	// filePath is a path relative to the root directory of the cell.
	// Can return ErrNoNode if the file doesn't exist.
	Get(ctx context.Context, filePath string) ([]byte, Version, error)

	// List returns KV pairs, along with metadata like the version, for
	// entries where the key contains the specified prefix.
	// filePathPrefix is a path relative to the root directory of the cell.
	// Can return ErrNoNode if there are no matches.
	List(ctx context.Context, filePathPrefix string) ([]KVInfo, error)

	// Delete deletes the provided file.
	// If version is nil, it is an unconditional delete.
	// If the last entry of a directory is deleted, using ListDir
	// on its parent directory should not return the directory.
	// For instance, when deleting /keyspaces/aaa/Keyspace, and if
	// there is no other file in /keyspaces/aaa, then aaa should not
	// appear any more when listing /keyspaces.
	// filePath is a path relative to the root directory of the cell.
	//
	// Delete will never be called on a directory.
	// Returns ErrNodeExists if the file doesn't exist.
	// Returns ErrBadVersion if the provided version is not current.
	Delete(ctx context.Context, filePath string, version Version) error

	//
	// Locks
	//

	// Lock takes a lock on the given directory.
	// It does not prevent any modification to any file in the topology.
	// It just prevents two concurrent processes (wherever they are)
	// to run concurrently. It is used for instance to make sure only
	// one reparent operation is running on a Shard at a given time.
	// dirPath is the directory associated with a resource, for instance
	// a Keyspace or a Shard. It is not a file location.
	// (this means the implementation can for instance create a
	// file in this directory to materialize the lock).
	// contents describes the lock holder and purpose, but has no other
	// meaning, so it can be used as a lock file contents, for instance.
	// Returns ErrNoNode if the directory doesn't exist (meaning
	//   there is no existing file under that directory).
	// Returns ErrTimeout if ctx expires.
	// Returns ErrInterrupted if ctx is canceled.
	Lock(ctx context.Context, dirPath, contents string) (LockDescriptor, error)

	// TryLock takes lock on the given directory with a fail-fast approach.
	// It is similar to `Lock` but the difference is it attempts to acquire the lock
	// if it is likely to succeed. If there is already a lock on given path, then unlike `Lock`
	// instead of waiting and blocking the client it returns with `Lock already exists` error.
	// With current implementation it may not be able to fail-fast for some scenarios.
	// For example there is a possibility that a thread checks for lock for a given path
	// but by the time it acquires the lock, some other thread has already acquired it,
	// in this case the client will block until the other caller releases the lock or the
	// client call times out (just like standard `Lock' implementation). In short the lock checking
	// and acquiring is not under the same mutex in current implementation of `TryLock`.
	TryLock(ctx context.Context, dirPath, contents string) (LockDescriptor, error)

	//
	// Watches
	//

	// Watch starts watching a file in the provided cell.  It
	// returns the current value, a 'changes' channel to read the
	// changes from, and an error.
	// If the initial read fails, or the file doesn't
	// exist, an error is returned.
	//
	// To stop the watch, cancel the provided context.
	// This will eventually result in a final WatchData result with Err =
	// ErrInterrupted. It should be safe to cancel the context
	// multiple times, or after the Watch already errored out.
	//
	// The 'changes' channel may return a record with Err != nil.
	// In that case, the channel will also be closed right after
	// that record.  In any case, 'changes' has to be drained of
	// all events, even when 'stop' is closed.
	//
	// Note the 'changes' channel can return twice the same
	// Version/Contents (for instance, if the watch is interrupted
	// and restarted within the Conn implementation).
	// Similarly, the 'changes' channel may skip versions / changes
	// (that is, if value goes [A, B, C, D, E, F], the watch may only
	// receive [A, B, F]). This should only happen for rapidly
	// changing values though. Usually, the initial value will come
	// back right away. And a stable value (that hasn't changed for
	// a while) should be seen shortly.
	//
	// The Watch call is not guaranteed to return exactly up to
	// date data right away. For instance, if a file is created
	// and saved, and then a watch is set on that file, it may
	// return ErrNoNode (as the underlying configuration service
	// may use asynchronous caches that are not up to date
	// yet). The only guarantee is that the watch data will
	// eventually converge. Vitess doesn't explicitly depend on the data
	// being correct quickly, as long as it eventually gets there.
	//
	// filePath is a path relative to the root directory of the cell.
	Watch(ctx context.Context, filePath string) (current *WatchData, changes <-chan *WatchData, err error)

	// WatchRecursive starts watching a file prefix in the provided cell. It
	// returns all the current values for existing files with the given
	// prefix, a 'changes' channel  to read the changes from and an error.
	//
	// The provided context should be canceled when stopping WatchRecursive().
	// This API is different from Watch() and Watch() will be changed
	// to match this API as well in the future.
	//
	// Canceling will eventually result in a final WatchDataRecursive result with Err =
	// ErrInterrupted.
	//
	// The 'changes' channel may return a record with Err != nil.
	// In that case, the channel will also be closed right after
	// that record.  In any case, 'changes' has to be drained of
	// all events, even when 'stop' is closed.
	//
	// Note the 'changes' channel can return twice the same
	// Version/Contents (for instance, if the watch is interrupted
	// and restarted within the Conn implementation).
	// Similarly, the 'changes' channel may skip versions / changes
	// (that is, if value goes [A, B, C, D, E, F], the watch may only
	// receive [A, B, F]). This should only happen for rapidly
	// changing values though. Usually, the initial value will come
	// back right away. And a stable value (that hasn't changed for
	// a while) should be seen shortly.
	//
	// The WatchRecursive call is not guaranteed to return exactly up to
	// date data right away. For instance, if a file is created
	// and saved, and then a watch is set on that file, it may
	// return ErrNoNode (as the underlying configuration service
	// may use asynchronous caches that are not up to date
	// yet). The only guarantee is that the watch data will
	// eventually converge. Vitess doesn't explicitly depend on the data
	// being correct quickly, as long as it eventually gets there.
	//
	// path is a path relative to the root directory of the cell.
	WatchRecursive(ctx context.Context, path string) ([]*WatchDataRecursive, <-chan *WatchDataRecursive, error)

	//
	// Leader election methods. This is meant to have a small
	// number of processes elect a primary within a group. The
	// backend storage for this can either be the global topo
	// server, or a resilient quorum of individual cells, to
	// reduce the load / dependency on the global topo server.
	//

	// NewLeaderParticipation creates a LeaderParticipation
	// object, used to become the Leader in an election for the
	// provided group name.  Id is the name of the local process,
	// passing in the hostname:port of the current process as id
	// is the common usage. Id must be unique for each process
	// calling this, for a given name. Calling this function does
	// not make the current process a candidate for the election.
	NewLeaderParticipation(name, id string) (LeaderParticipation, error)

	// Close closes the connection to the server.
	Close()
}

// DirEntryType is the type of an entry in a directory.
type DirEntryType int

const (
	// TypeDirectory describes a directory.
	TypeDirectory DirEntryType = iota

	// TypeFile describes a file.
	TypeFile
)

// DirEntry is an entry in a directory, as returned by ListDir.
type DirEntry struct {
	// Name is the name of the entry.
	// Always filled in.
	Name string

	// Type is the DirEntryType of the entry.
	// Only filled in if full is true.
	Type DirEntryType

	// Ephemeral is set if the directory / file only contains
	// data that was not set by the file API, like lock files
	// or primary-election related files.
	// Only filled in if full is true.
	Ephemeral bool
}

// DirEntriesToStringArray is a helper method to extract the names
// from an []DirEntry
func DirEntriesToStringArray(entries []DirEntry) []string {
	result := make([]string, len(entries))
	for i, e := range entries {
		result[i] = e.Name
	}
	return result
}

// dirEntries is used for sorting.
type dirEntries []DirEntry

func (e dirEntries) Len() int           { return len(e) }
func (e dirEntries) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e dirEntries) Less(i, j int) bool { return e[i].Name < e[j].Name }

// DirEntriesSortByName sorts a slice of DirEntry objects by Name.
func DirEntriesSortByName(entries []DirEntry) {
	sort.Sort(dirEntries(entries))
}

// Version is an interface that describes a file version.
type Version interface {
	// String returns a text representation of the version.
	String() string
}

// LockDescriptor is an interface that describes a lock.
// It will be returned by Lock().
type LockDescriptor interface {
	// Check returns an error if the lock was lost.
	// Some topology implementations use a keep-alive mechanism, and
	// sometimes it fails. The users of the lock are responsible for
	// checking on it when convenient.
	Check(ctx context.Context) error

	// Unlock releases the lock.
	Unlock(ctx context.Context) error
}

// CancelFunc is returned by the Watch method.
type CancelFunc func()

// WatchData is the structure returned by the Watch() API.
// It can contain:
// a) an error in Err if the call failed (or if the watch was terminated).
// b) the current or new version of the data.
type WatchData struct {
	// Contents has the bytes that were stored by Create
	// or Update.
	Contents []byte

	// Version contains an opaque representation of the Version
	// of that file.
	Version Version

	// Err is set the same way for both the 'current' value
	// returned by Watch, or the values read on the 'changes'
	// channel. It can be:
	// - nil, then Contents and Version are set.
	// - ErrNoNode if the file doesn't exist.
	// - ErrInterrupted if 'cancel' was called.
	// - any other platform-specific error.
	Err error
}

// WatchDataRecursive is the structure returned by the WatchRecursive() API.
// It contains the same data as WatchData, but additionally also the specific
// path of the entry that the recursive watch applies to, since an entire
// file prefix can be watched.
type WatchDataRecursive struct {
	// Path is the path that has changed
	Path string

	WatchData
}

// KVInfo is a structure that contains a generic key/value pair from
// the topo server, along with important metadata about it.
// This should be used to provide multiple entries in List like calls
// that return N KVs based on a key prefix, so that you don't lose
// information or context you would otherwise have when using Get for
// a single key.
type KVInfo struct {
	Key     []byte
	Value   []byte
	Version Version // version - used to prevent stomping concurrent writes
}

// LeaderParticipation is the object returned by NewLeaderParticipation.
// Sample usage:
//
// mp := server.NewLeaderParticipation("vtctld", "hostname:8080")
// job := NewJob()
//
//	go func() {
//	  for {
//	    ctx, err := mp.WaitForLeadership()
//	    switch err {
//	    case nil:
//	      job.RunUntilContextDone(ctx)
//	    case topo.ErrInterrupted:
//	      return
//	    default:
//	      log.Errorf("Got error while waiting for primary, will retry in 5s: %v", err)
//	      time.Sleep(5 * time.Second)
//	    }
//	  }
//	}()
//
//	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
//	  if job.Running() {
//	    job.WriteStatus(w, r)
//	  } else {
//	    http.Redirect(w, r, mp.GetCurrentLeaderID(context.Background()), http.StatusFound)
//	  }
//	})
//
//	servenv.OnTermSync(func() {
//	  mp.Stop()
//	})
type LeaderParticipation interface {
	// WaitForLeadership makes the current process a candidate
	// for election, and waits until this process is the primary.
	// After we become the primary, we may lose primaryship. In that case,
	// the returned context will be canceled. If Stop was called,
	// WaitForLeadership will return nil, ErrInterrupted.
	WaitForLeadership() (context.Context, error)

	// Stop is called when we don't want to participate in the
	// primary election any more. Typically, that is when the
	// hosting process is terminating.  We will relinquish
	// primaryship at that point, if we had it. Stop should
	// not return until everything has been done.
	// The LeaderParticipation object should be discarded
	// after Stop has been called. Any call to WaitForLeadership
	// after Stop() will return nil, ErrInterrupted.
	// If WaitForLeadership() was running, it will return
	// nil, ErrInterrupted as soon as possible.
	Stop()

	// GetCurrentLeaderID returns the current primary id.
	// This may not work after Stop has been called.
	GetCurrentLeaderID(ctx context.Context) (string, error)

	// WaitForNewLeader allows for nodes to wait until a leadership
	// election cycle completes and to get subsequent updates of
	// leadership changes. This way logic that needs to know if leadership
	// changes also if we're not the leader ourselves doesn't need to
	// poll for leadership status.
	//
	// For topo implementation that have this, it can be used more
	// efficiently than needing a busy wait loop.
	WaitForNewLeader(ctx context.Context) (<-chan string, error)
}
