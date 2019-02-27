package pools

import (
	"context"
	"errors"
	"os"
	"time"
)

type Pool interface {
	Close()
	Get(context.Context) (Resource, error)
	Put(Resource)

	SetCapacity(int, bool) error
	SetIdleTimeout(duration time.Duration)

	Capacity() int
	IdleTimeout() time.Duration
	MaxCap() int
	MinActive() int
	Active() int
	Available() int
	InUse() int

	WaitTime() time.Duration
	WaitCount() int64
	IdleClosed() int64

	StatsJSON() string
}

var (
	// ErrClosed is returned if ResourcePool is used when it's closed.
	ErrClosed = errors.New("resource pool is closed")

	// ErrTimeout is returned if a resource get times out.
	ErrTimeout = errors.New("resource pool timed out")

	// ErrFull is returned if a put is placed when the pool at capacity.
	ErrFull = errors.New("resource pool is full")

	// ErrPutBeforeGet is caused when there was a put called before a get.
	ErrPutBeforeGet = errors.New("a put was placed before get in the resource pool")

	// errNeedToQueue is used internally as a state informing that the caller needs to wait.
	errNeedToQueue = errors.New("need to queue")
)

// CreateFactory is a function that can be used to create a resource.
type CreateFactory func() (Resource, error)

// Resource defines the interface that every resource must provide.
// Thread synchronization between Close() and IsClosed()
// is the responsibility of the caller.
type Resource interface {
	Close()
}

type resourceWrapper struct {
	resource Resource
	timeUsed time.Time
}

type Impl int

const (
	ResourceImpl Impl = iota
	FastImpl
)

func New(poolImpl Impl, f CreateFactory, cap, maxCap int, idleTimeout time.Duration, minActive int) Pool {
	// Have an environment override so that several of the tests
	// outside this package don't need to be written twice.
	// e.g. VT_EXPERIMENTAL_FAST_POOL=1 make unit_test
	if os.Getenv("VT_EXPERIMENTAL_FAST_POOL") != "" {
		poolImpl = FastImpl
	}

	switch poolImpl {
	case ResourceImpl:
		return NewResourcePool(f, cap, maxCap, idleTimeout)
	case FastImpl:
		return NewFastPool(f, cap, maxCap, idleTimeout, minActive)
	}

	return nil
}
