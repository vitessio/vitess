package pbrpc

import (
	"io"
	"sync"

	"code.google.com/p/goprotobuf/proto"
	rpc "github.com/youtube/vitess/go/rpcplus"
)

type pbServerCodec struct {
	mu  sync.Mutex
	rwc io.ReadWriteCloser
}

// NewServerCodec returns a new ServerCodec.
func NewServerCodec(rwc io.ReadWriteCloser) rpc.ServerCodec {
	return &pbServerCodec{rwc: rwc}
}

// ReadRequestHeader reads a Request.
func (c *pbServerCodec) ReadRequestHeader(r *rpc.Request) error {
	data, err := ReadNetString(c.rwc)
	if err != nil {
		return err
	}
	rtmp := new(Request)
	err = proto.Unmarshal(data, rtmp)
	if err != nil {
		return err
	}
	r.ServiceMethod = *rtmp.ServiceMethod
	r.Seq = *rtmp.Seq
	return nil
}

// ReadRequestBody reads a body structure from the codec.
func (c *pbServerCodec) ReadRequestBody(body interface{}) error {
	data, err := ReadNetString(c.rwc)
	if err != nil {
		return err
	}
	if body != nil {
		return proto.Unmarshal(data, body.(proto.Message))
	}
	return nil
}

type flusher interface {
	Flush() error
}

// WriteResponse writes a response on the codec.
func (c *pbServerCodec) WriteResponse(r *rpc.Response, body interface{}, last bool) (err error) {
	// Use a mutex to guarantee the header/body are written in the correct order.
	c.mu.Lock()
	defer c.mu.Unlock()
	rtmp := &Response{ServiceMethod: &r.ServiceMethod, Seq: &r.Seq, Error: &r.Error}
	data, err := proto.Marshal(rtmp)
	if err != nil {
		return
	}
	_, err = WriteNetString(c.rwc, data)
	if err != nil {
		return
	}

	if pb, ok := body.(proto.Message); ok {
		data, err = proto.Marshal(pb)
		if err != nil {
			return
		}
	} else {
		data = nil
	}
	_, err = WriteNetString(c.rwc, data)
	if err != nil {
		return
	}

	if flusher, ok := c.rwc.(flusher); ok {
		err = flusher.Flush()
	}
	return
}

// Close the underlying connection.
func (c *pbServerCodec) Close() error {
	return c.rwc.Close()
}
