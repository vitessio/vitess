// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bsonrpc

import (
	"io"
	"time"

	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/bytes2"
	rpc "code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap"
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

func (cc *ClientCodec) WriteRequest(r *rpc.Request, body interface{}) error {
	buf := bytes2.NewChunkedWriter(DefaultBufferSize)
	if err := bson.MarshalToBuffer(buf, &RequestBson{r}); err != nil {
		return err
	}
	if err := bson.MarshalToBuffer(buf, body); err != nil {
		return err
	}
	_, err := buf.WriteTo(cc.rwc)
	return err
}

func (cc *ClientCodec) ReadResponseHeader(r *rpc.Response) error {
	return bson.UnmarshalFromStream(cc.rwc, &ResponseBson{r})
}

func (cc *ClientCodec) ReadResponseBody(body interface{}) error {
	return bson.UnmarshalFromStream(cc.rwc, body)
}

func (cc *ClientCodec) Close() error {
	return cc.rwc.Close()
}

type ServerCodec struct {
	rwc io.ReadWriteCloser
	cw  *bytes2.ChunkedWriter
}

func NewServerCodec(conn io.ReadWriteCloser) rpc.ServerCodec {
	return &ServerCodec{conn, bytes2.NewChunkedWriter(DefaultBufferSize)}
}

func (sc *ServerCodec) ReadRequestHeader(r *rpc.Request) error {
	return bson.UnmarshalFromStream(sc.rwc, &RequestBson{r})
}

func (sc *ServerCodec) ReadRequestBody(body interface{}) error {
	return bson.UnmarshalFromStream(sc.rwc, body)
}

func (sc *ServerCodec) WriteResponse(r *rpc.Response, body interface{}, last bool) error {
	if err := bson.MarshalToBuffer(sc.cw, &ResponseBson{r}); err != nil {
		return err
	}
	if err := bson.MarshalToBuffer(sc.cw, body); err != nil {
		return err
	}
	_, err := sc.cw.WriteTo(sc.rwc)
	sc.cw.Reset()
	return err
}

func (sc *ServerCodec) Close() error {
	return sc.rwc.Close()
}

func DialHTTP(network, address string, connectTimeout time.Duration) (*rpc.Client, error) {
	return rpcwrap.DialHTTP(network, address, codecName, NewClientCodec, connectTimeout)
}

func DialAuthHTTP(network, address, user, password string, connectTimeout time.Duration) (*rpc.Client, error) {
	return rpcwrap.DialAuthHTTP(network, address, user, password, codecName, NewClientCodec, connectTimeout)
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
