// Package pbrpc implements a ClientCodec and ServerCodec
// for the rpc package using proto.
package pbrpc

import (
	"io"
	"net"
	"sync"

	"code.google.com/p/goprotobuf/proto"
	rpc "github.com/youtube/vitess/go/rpcplus"
)

// NewClientCodec returns a new rpc.ClientCodec using JSON-RPC on conn.
func NewClientCodec(conn io.ReadWriteCloser) rpc.ClientCodec {
	return &pbClientCodec{rwc: conn}
}

// NewClient returns a new rpc.Client to handle requests to the
// set of services at the other end of the connection.
func NewClient(conn io.ReadWriteCloser) *rpc.Client {
	return rpc.NewClientWithCodec(NewClientCodec(conn))
}

// Dial connects to a Protobuf-RPC server at the specified network address.
func Dial(network, address string) (*rpc.Client, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return NewClient(conn), err
}

type pbClientCodec struct {
	mu  sync.Mutex
	rwc io.ReadWriteCloser
}

func (c *pbClientCodec) WriteRequest(r *rpc.Request, body interface{}) (err error) {
	// Use a mutex to guarantee the header/body are written in the correct order.
	c.mu.Lock()
	defer c.mu.Unlock()

	// This is protobuf, of course we copy it.
	pbr := &Request{ServiceMethod: &r.ServiceMethod, Seq: &r.Seq}
	data, err := proto.Marshal(pbr)
	if err != nil {
		return
	}
	_, err = WriteNetString(c.rwc, data)
	if err != nil {
		return
	}

	// Of course this is a protobuf! Trust me or detonate the program.
	data, err = proto.Marshal(body.(proto.Message))
	if err != nil {
		return
	}
	_, err = WriteNetString(c.rwc, data)
	if err != nil {
		return
	}

	if flusher, ok := c.rwc.(Flusher); ok {
		err = flusher.Flush()
	}
	return
}

func (c *pbClientCodec) ReadResponseHeader(r *rpc.Response) error {
	data, err := ReadNetString(c.rwc)
	if err != nil {
		return err
	}
	rtmp := new(Response)
	err = proto.Unmarshal(data, rtmp)
	if err != nil {
		return err
	}
	r.ServiceMethod = *rtmp.ServiceMethod
	r.Seq = *rtmp.Seq
	r.Error = *rtmp.Error
	return nil
}

func (c *pbClientCodec) ReadResponseBody(body interface{}) error {
	data, err := ReadNetString(c.rwc)
	if err != nil {
		return err
	}
	if body != nil {
		return proto.Unmarshal(data, body.(proto.Message))
	}
	return nil
}

func (c *pbClientCodec) Close() error {
	return c.rwc.Close()
}
