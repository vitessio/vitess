package topo

import "golang.org/x/net/context"

// Backend defines the interface that must be implemented by topology
// plug-ins to be used with Vitess.
//
// Zookeeper is a good example of an implementation, as defined in
// go/vt/zktopo.
//
// This API is very generic, and file oriented.
//
// FIXME(alainjobart) add all parts of the API, implement them all for
// all our current systems, and convert the higher levels to talk to
// this API. This is a long-term project.
type Backend interface {
	// Directory support: NYI
	//	MkDir(ctx context.Context, cell, dirPath string) error
	//	RmDir(ctx context.Context, cell, dirPath string) error
	//	ListDir(ctx context.Context, cell, dirPath string) ([]string, error)

	// File support: NYI
	// if version == nil, then it’s an unconditional update / delete.
	//	Create(ctx context.Context, cell, filePath string, contents []byte) error
	//	Update(ctx context.Context, cell, filePath string, contents []byte, version Version) (Version, error)
	//	Get(ctx context.Context, cell, filePath string) ([]byte, Version, error)
	//	Delete(ctx context.Context, cell, filePath string, version Version) error

	// Locks: NYI
	//	Lock(ctx context.Context, cell string, dirPath string) (LockDescriptor, error)
	//	Unlock(ctx context.Context, descriptor LockDescriptor) error

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
	Watch(ctx context.Context, cell, filePath string) (current *WatchData, changes <-chan *WatchData, cancel CancelFunc)
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
