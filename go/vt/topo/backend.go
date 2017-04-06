package topo

import (
	"golang.org/x/net/context"
)

const (
	// GlobalCell is the name of the global cell.  It is special
	// as it contains the global topology, and references the other cells.
	GlobalCell = "global"
)

// Backend defines the interface that must be implemented by topology
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
// FIXME(alainjobart) add all parts of the API, implement them all for
// all our current systems, and convert the higher levels to talk to
// this API. This is a long-term project.
type Backend interface {
	//
	// Directory support
	//

	// ListDir returns the entries in a directory.  The returned
	// list should be sorted (by sort.Strings for instance).
	// If there are no files under the provided path, returns ErrNoNode.
	// dirPath is a path relative to the root directory of the cell.
	ListDir(ctx context.Context, cell, dirPath string) ([]string, error)

	//
	// File support
	// if version == nil, then it’s an unconditional update / delete.
	//

	// Create creates the initial version of a file.
	// Returns ErrNodeExists if the file exists.
	// filePath is a path relative to the root directory of the cell.
	Create(ctx context.Context, cell, filePath string, contents []byte) (Version, error)

	// Update updates the file with the provided filename with the
	// new content.
	// If version is nil, it is an unconditional update
	// (which is then the same as a Create is the file doesn't exist).
	// filePath is a path relative to the root directory of the cell.
	// It returns the new Version of the file after update.
	// Returns ErrBadVersion if the provided version is not current.
	Update(ctx context.Context, cell, filePath string, contents []byte, version Version) (Version, error)

	// Get returns the content and version of a file.
	// filePath is a path relative to the root directory of the cell.
	// Can return ErrNoNode if the file doesn't exist.
	Get(ctx context.Context, cell, filePath string) ([]byte, Version, error)

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
	Delete(ctx context.Context, cell, filePath string, version Version) error

	//
	// Locks
	//

	//	NYI: Lock(ctx context.Context, cell string, dirPath string) (LockDescriptor, error)
	//	NYI: Unlock(ctx context.Context, descriptor LockDescriptor) error

	//
	// Watches
	//

	// Watch starts watching a file in the provided cell.  It
	// returns the current value, a 'changes' channel to read the
	// changes from, and a 'cancel' function to call to stop the
	// watch.  If the initial read fails, or the file doesn't
	// exist, current.Err is set, and 'changes'/'cancel' are nil.
	// Otherwise current.Err is nil, and current.Contents /
	// current.Version are accurate. The provided context is only
	// used to setup the current watch, and not after Watch()
	// returns.
	//
	// To stop the watch, just call the returned 'cancel' function.
	// This will eventually result in a final WatchData result with Err =
	// ErrInterrupted. It should be safe to call the 'cancel' function
	// multiple times, or after the Watch already errored out.
	//
	// The 'changes' channel may return a record with Err != nil.
	// In that case, the channel will also be closed right after
	// that record.  In any case, 'changes' has to be drained of
	// all events, even when 'stop' is closed.
	//
	// Note the 'changes' channel can return twice the same
	// Version/Contents (for instance, if the watch is interrupted
	// and restarted within the Backend implementation).
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
	Watch(ctx context.Context, cell, filePath string) (current *WatchData, changes <-chan *WatchData, cancel CancelFunc)

	//
	// Master election methods. This is meant to have a small
	// number of processes elect a master within a group. The
	// backend storage for this can either be the global topo
	// server, or a resilient quorum of individual cells, to
	// reduce the load / dependency on the global topo server.
	//

	// NewMasterParticipation creates a MasterParticipation
	// object, used to become the Master in an election for the
	// provided group name.  Id is the name of the local process,
	// passing in the hostname:port of the current process as id
	// is the common usage. Id must be unique for each process
	// calling this, for a given name. Calling this function does
	// not make the current process a candidate for the election.
	NewMasterParticipation(name, id string) (MasterParticipation, error)
}

// Version is an interface that describes a file version.
type Version interface {
	// String returns a text representation of the version.
	String() string
}

// LockDescriptor is an interface that describes a lock.
type LockDescriptor interface {
	// String returns a text representation of the lock.
	String() string
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

// MasterParticipation is the object returned by NewMasterParticipation.
// Sample usage:
//
// mp := server.NewMasterParticipation("vtctld", "hostname:8080")
// job := NewJob()
// go func() {
//   for {
//     ctx, err := mp.WaitForMastership()
//     switch err {
//     case nil:
//       job.RunUntilContextDone(ctx)
//     case topo.ErrInterrupted:
//       return
//     default:
//       log.Errorf("Got error while waiting for master, will retry in 5s: %v", err)
//       time.Sleep(5 * time.Second)
//     }
//   }
// }()
//
// http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
//   if job.Running() {
//     job.WriteStatus(w, r)
//   } else {
//     http.Redirect(w, r, mp.GetCurrentMasterID(context.Background()), http.StatusFound)
//   }
// })
//
// servenv.OnTermSync(func() {
//   mp.Stop()
// })
type MasterParticipation interface {
	// WaitForMastership makes the current process a candidate
	// for election, and waits until this process is the master.
	// After we become the master, we may lose mastership. In that case,
	// the returned context will be canceled. If Stop was called,
	// WaitForMastership will return nil, ErrInterrupted.
	WaitForMastership() (context.Context, error)

	// Stop is called when we don't want to participate in the
	// master election any more. Typically, that is when the
	// hosting process is terminating.  We will relinquish
	// mastership at that point, if we had it. Stop should
	// not return until everything has been done.
	// The MasterParticipation object should be discarded
	// after Stop has been called. Any call to WaitForMastership
	// after Stop() will return nil, ErrInterrupted.
	// If WaitForMastership() was running, it will return
	// nil, ErrInterrupted as soon as possible.
	Stop()

	// GetCurrentMasterID returns the current master id.
	// This may not work after Stop has been called.
	GetCurrentMasterID(ctx context.Context) (string, error)
}
