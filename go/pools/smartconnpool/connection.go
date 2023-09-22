package smartconnpool

import (
	"context"
	"sync/atomic"
	"time"
)

type Connection interface {
	ApplySetting(ctx context.Context, setting *Setting) error
	ResetSetting(ctx context.Context) error
	Setting() *Setting

	IsClosed() bool
	Close()
}

type Pooled[C Connection] struct {
	next        atomic.Pointer[Pooled[C]]
	timeCreated time.Time
	timeUsed    time.Time
	pool        *ConnPool[C]

	Conn C
}

func (dbc *Pooled[C]) Close() {
	dbc.Conn.Close()
}

func (dbc *Pooled[C]) Recycle() {
	switch {
	case dbc.pool == nil:
		dbc.Conn.Close()
	case dbc.Conn.IsClosed():
		dbc.pool.Put(nil)
	default:
		dbc.pool.Put(dbc)
	}
}

func (dbc *Pooled[C]) Taint() {
	if dbc.pool == nil {
		return
	}
	dbc.pool.Put(nil)
	dbc.pool = nil
}
