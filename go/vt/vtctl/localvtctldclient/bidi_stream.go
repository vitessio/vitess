package localvtctldclient

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type bidiStream struct {
	m          sync.RWMutex
	ctx        context.Context
	errch      chan error
	sendClosed bool
}

func newBidiStream(ctx context.Context) *bidiStream { // nolint (TODO:@ajm188) this will be used in a future PR, and the codegen will produce invalid code for streaming rpcs without this
	return &bidiStream{
		ctx:   ctx,
		errch: make(chan error, 1),
	}
}

func (bs *bidiStream) close(err error) {
	if err == nil {
		err = io.EOF
	}

	bs.m.Lock()
	defer bs.m.Unlock()

	bs.sendClosed = true
	bs.errch <- err
}

var (
	_ grpc.ClientStream = (*bidiStream)(nil)
	_ grpc.ServerStream = (*bidiStream)(nil)
)

// client and server methods

func (bs *bidiStream) Context() context.Context    { return bs.ctx }
func (bs *bidiStream) RecvMsg(m interface{}) error { return nil }
func (bs *bidiStream) SendMsg(m interface{}) error { return nil }

// client methods

func (bs *bidiStream) Header() (metadata.MD, error) { return nil, nil }
func (bs *bidiStream) Trailer() metadata.MD         { return nil }
func (bs *bidiStream) CloseSend() error             { return nil }

// server methods

func (bs *bidiStream) SendHeader(md metadata.MD) error { return nil }
func (bs *bidiStream) SetHeader(md metadata.MD) error  { return nil }
func (bs *bidiStream) SetTrailer(md metadata.MD)       {}
