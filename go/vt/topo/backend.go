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
	//	MkDir(ctx context.Context, cell string, path string) error
	//	RmDir(ctx context.Context, cell string, path string) error
	//	ListDir(ctx context.Context, cell string, path string) ([]string, error)

	// File support: NYI
	// if version == nil, then it’s an unconditional update / delete.
	//	Create(ctx context.Context, cell string, path string, contents []byte) error
	//	Update(ctx context.Context, cell string, path string, contents []byte, version Version) (Version, error)
	//	Get(ctx context.Context, cell string, path string) ([]byte, Version, error)
	//	Delete(ctx context.Context, cell string, path string, version Version)

	// Locks: NYI
	//	Lock(ctx context.Context, cell string, dirPath string) (LockDescriptor, error)
	//	Unlock(ctx context.Context, descriptor LockDescriptor) error

	// Watch starts watching a file in the provided cell.  It
	// returns the current value, as well as a channel to read the
	// changes from.  If the initial read fails, or the file
	// doesn't exist, current.Err is set, and 'changes' is nil.
	// Otherwise current.Err is nil, and current.Contents /
	// current.Version are accurate.
	//
	// The 'changes' channel may return a record with Err != nil.
	// In that case, the channel will also be closed right after
	// that record.  In any case, 'changes' has to be drained of
	// all events, even when the Context is canceled.
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
	// To stop the watch, just cancel the context.
	Watch(ctx context.Context, cell string, path string) (current *WatchData, changes <-chan *WatchData)
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
	// - context.Err() if context.Done() is closed (then the value
	//   will be context.DeadlineExceeded or context.Interrupted).
	// - any other platform-specific error.
	Err error
}
