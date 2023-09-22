package smartconnpool

import (
	"context"
	"sync"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// ErrTimeout is returned if a resource get times out.
	ErrTimeout = vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, "resource pool timed out")

	// ErrCtxTimeout is returned if a ctx is already expired by the time the resource pool is used
	ErrCtxTimeout = vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "resource pool context already expired")
)

type waiter[C Connection] struct {
	Setting *Setting
	Conn    *Pooled[C]
	Context context.Context
	Sema    semaphore
	Age     uint32
}

type waitlist[C Connection] struct {
	nodes sync.Pool
	mu    sync.Mutex
	list  List[waiter[C]]
}

func (wl *waitlist[C]) waitForConn(ctx context.Context, setting *Setting) (*Pooled[C], error) {
	elem := wl.nodes.Get().(*Element[waiter[C]])
	elem.Value = waiter[C]{Setting: setting, Conn: nil, Context: ctx}
	elem.Value.Sema.init()

	wl.mu.Lock()
	wl.list.PushBackValue(elem)
	wl.mu.Unlock()

	elem.Value.Sema.wait()

	conn := elem.Value.Conn
	wl.nodes.Put(elem)

	return conn, ctx.Err()
}

func (wl *waitlist[C]) expire(force bool) (waiting int) {
	if wl.list.len.Load() == 0 {
		return 0
	}

	var expired []*Element[waiter[C]]

	wl.mu.Lock()
	for e := wl.list.Front(); e != nil; e = e.Next() {
		if force || e.Value.Context.Err() != nil {
			wl.list.Remove(e)
			expired = append(expired, e)
			continue
		}
		waiting++
	}
	wl.mu.Unlock()

	for _, e := range expired {
		e.Value.Sema.notify(false)
	}
	return waiting
}

func (wl *waitlist[D]) tryReturnConn(conn *Pooled[D]) bool {
	if wl.list.len.Load() == 0 {
		return false
	}
	return wl.tryReturnConnSlow(conn)
}

func (wl *waitlist[D]) tryReturnConnSlow(conn *Pooled[D]) bool {
	const maxAge = 8
	var (
		target      *Element[waiter[D]]
		connSetting = conn.Conn.Setting()
	)

	wl.mu.Lock()
	target = wl.list.Front()
	for e := target; e != nil; e = e.Next() {
		if e.Value.Age > maxAge || e.Value.Setting == connSetting {
			target = e
			break
		}
		e.Value.Age++
	}
	if target != nil {
		wl.list.Remove(target)
	}
	wl.mu.Unlock()

	if target == nil {
		return false
	}

	target.Value.Conn = conn
	target.Value.Sema.notify(true)
	return true
}

func (wl *waitlist[C]) init() {
	wl.nodes.New = func() any {
		return &Element[waiter[C]]{}
	}
	wl.list.Init()
}

func (wl *waitlist[C]) waiting() int {
	return int(wl.list.len.Load())
}
