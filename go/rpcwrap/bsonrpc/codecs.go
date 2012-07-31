// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bsonrpc

import (
	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/bytes2"
	"code.google.com/p/vitess/go/rpcwrap"
	"io"
	"net/rpc"
)

const (
	codecName = "bson"
)

type ClientCodec struct {
	rwc io.ReadWriteCloser
}

func NewClientCodec(conn io.ReadWriteCloser) rpc.ClientCodec {
	return &ClientCodec{conn}
}

const DefaultBufferSize = 4096

func (self *ClientCodec) WriteRequest(r *rpc.Request, body interface{}) error {
	buf := bytes2.NewChunkedWriter(DefaultBufferSize)
	if err := bson.MarshalToBuffer(buf, &RequestBson{r}); err != nil {
		return err
	}
	if err := bson.MarshalToBuffer(buf, body); err != nil {
		return err
	}
	_, err := buf.WriteTo(self.rwc)
	return err
}

func (self *ClientCodec) ReadResponseHeader(r *rpc.Response) error {
	return bson.UnmarshalFromStream(self.rwc, &ResponseBson{r})
}

func (self *ClientCodec) ReadResponseBody(body interface{}) error {
	return bson.UnmarshalFromStream(self.rwc, body)
}

func (self *ClientCodec) Close() error {
	return self.rwc.Close()
}

type ServerCodec struct {
	rwc io.ReadWriteCloser
	cw  *bytes2.ChunkedWriter
}

func NewServerCodec(conn io.ReadWriteCloser) rpc.ServerCodec {
	return &ServerCodec{conn, bytes2.NewChunkedWriter(DefaultBufferSize)}
}

func (self *ServerCodec) ReadRequestHeader(r *rpc.Request) error {
	return bson.UnmarshalFromStream(self.rwc, &RequestBson{r})
}

func (self *ServerCodec) ReadRequestBody(body interface{}) error {
	return bson.UnmarshalFromStream(self.rwc, body)
}

func (self *ServerCodec) WriteResponse(r *rpc.Response, body interface{}) error {
	if err := bson.MarshalToBuffer(self.cw, &ResponseBson{r}); err != nil {
		return err
	}
	if err := bson.MarshalToBuffer(self.cw, body); err != nil {
		return err
	}
	_, err := self.cw.WriteTo(self.rwc)
	self.cw.Reset()
	return err
}

func (self *ServerCodec) Close() error {
	return self.rwc.Close()
}

func DialHTTP(network, address string) (*rpc.Client, error) {
	return rpcwrap.DialHTTP(network, address, codecName, NewClientCodec)
}

func ServeRPC() {
	rpcwrap.ServeRPC(codecName, NewServerCodec)
}

func ServeAuthRPC() {
	rpcwrap.ServeAuthRPC(codecName, NewServerCodec)
}

func ServeHTTP() {
	rpcwrap.ServeHTTP(codecName, NewServerCodec)
}
