/*
Copyright 2021 The Vitess Authors.

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

package grpcshim

import (
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ErrStreamClosed is the error types embedding BidiStream should return when
// their Send method is called and IsClosed returns true.
var ErrStreamClosed = errors.New("stream closed for sending")

// BidiStream is a shim struct implementing both the grpc.ClientStream and
// grpc.ServerStream interfaces. It can be embedded into other types that need
// all of those methods to satisfy the compiler, but are only interested in the
// parameterized Send/Recv methods typically called by gRPC streaming servers
// and clients. For example, in the localvtctldclient:
//
//	type backupStreamAdapter struct {
//		*grpcshim.BidiStream
//		ch chan *vtctldatapb.BackupResponse
//	}
//
//	func (stream *backupStreamAdapter) Recv() (*vtctldatapb.BackupResponse, error) {
//		select {
//		case <-stream.Context().Done():
//			return nil, stream.Context().Err()
//		case <-stream.Closed():
//			// Stream has been closed for future sends. If there are messages that
//			// have already been sent, receive them until there are no more. After
//			// all sent messages have been received, Recv will return the CloseErr.
//			select {
//			case msg := <-stream.ch:
//				return msg, nil
//			default:
//				return nil, stream.CloseErr()
//			}
//		case err := <-stream.ErrCh:
//			return nil, err
//		case msg := <-stream.ch:
//			return msg, nil
//		}
//	}
//
//	func (stream *backupStreamAdapter) Send(msg *vtctldatapb.BackupResponse) error {
//		select {
//		case <-stream.Context().Done():
//			return stream.Context().Err()
//		case <-stream.Closed():
//			return grpcshim.ErrStreamClosed
//		case stream.ch <- msg:
//			return nil
//		}
//	}
//
//	// Backup is part of the vtctlservicepb.VtctldClient interface.
//	func (client *localVtctldClient) Backup(ctx context.Context, in *vtctldatapb.BackupRequest, opts ...grpc.CallOption) (vtctlservicepb.Vtctld_BackupClient, error) {
//		stream := &backupStreamAdapter{
//			BidiStream: grpcshim.NewBidiStream(ctx),
//			ch:         make(chan *vtctldatapb.BackupResponse, 1),
//		}
//		go func() {
//			err := client.s.Backup(in, stream)
//			stream.CloseWithError(err)
//		}()
//		return stream, nil
//	}
type BidiStream struct {
	// ErrCh receives errors mid-stream, and should be selected on with the
	// same priority as stream.ch and stream/send contexts' cancellations in
	// Recv methods.
	ErrCh chan error

	ctx              context.Context
	sendClosedCtx    context.Context
	sendClosedCancel context.CancelFunc

	m        sync.Mutex
	closeErr error
}

// NewBidiStream returns a BidiStream ready for embedded use. The provided ctx
// will be used for the stream context, and types embedding BidiStream should
// check context cancellation/expiriation in their respective Recv and Send
// methods.
//
// See the documentation on BidiStream for example usage.
func NewBidiStream(ctx context.Context) *BidiStream {
	sendClosedCtx, sendClosedCancel := context.WithCancel(context.Background())
	return &BidiStream{
		ErrCh:            make(chan error, 1),
		ctx:              ctx,
		sendClosedCtx:    sendClosedCtx,
		sendClosedCancel: sendClosedCancel,
	}
}

// Closed returns a channel which will be itself be closed when the stream has
// been closed for sending.
func (bs *BidiStream) Closed() <-chan struct{} {
	return bs.sendClosedCtx.Done()
}

// IsClosed returns true if the stream has been closed for sending.
//
// It is a conveince function for attempting to select on the channel returned
// by bs.Closed().
func (bs *BidiStream) IsClosed() bool {
	select {
	case <-bs.Closed():
		return true
	default:
		return false
	}
}

// CloseWithError closes the stream for future sends, and sets the error
// returned by bs.CloseErr(). If the passed err is nil, io.EOF is set instead.
// If the stream is already closed, this is a no-op.
func (bs *BidiStream) CloseWithError(err error) {
	bs.m.Lock()
	defer bs.m.Unlock()

	if bs.IsClosed() {
		return
	}

	if err == nil {
		err = io.EOF
	}

	bs.closeErr = err
	bs.sendClosedCancel()
}

// CloseErr returns the error set by CloseWithError, or nil if the stream is
// not closed.
func (bs *BidiStream) CloseErr() error {
	bs.m.Lock()
	defer bs.m.Unlock()
	return bs.closeErr
}

var (
	_ grpc.ClientStream = (*BidiStream)(nil)
	_ grpc.ServerStream = (*BidiStream)(nil)
)

// client and server methods

func (bs *BidiStream) Context() context.Context { return bs.ctx }
func (bs *BidiStream) RecvMsg(m any) error      { return nil }
func (bs *BidiStream) SendMsg(m any) error      { return nil }

// client methods

func (bs *BidiStream) Header() (metadata.MD, error) { return nil, nil }
func (bs *BidiStream) Trailer() metadata.MD         { return nil }
func (bs *BidiStream) CloseSend() error             { return nil }

// server methods

func (bs *BidiStream) SendHeader(md metadata.MD) error { return nil }
func (bs *BidiStream) SetHeader(md metadata.MD) error  { return nil }
func (bs *BidiStream) SetTrailer(md metadata.MD)       {}
