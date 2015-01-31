// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bsonrpc provides codecs for bsonrpc communication
package bsonrpc

import (
	"crypto/tls"
	"io"
	"net/http"
	"time"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap"
)

const (
	codecName = "bson"
)

// ClientCodec holds required parameters for providing a client codec for
// bsonrpc
type ClientCodec struct {
	rwc io.ReadWriteCloser
}

// NewClientCodec creates a new client codec for bsonrpc communication
func NewClientCodec(conn io.ReadWriteCloser) rpc.ClientCodec {
	return &ClientCodec{conn}
}

// DefaultBufferSize holds the default value for buffer size
const DefaultBufferSize = 4096

// WriteRequest sends the request to the server
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

// ReadResponseHeader reads the header of server response
func (cc *ClientCodec) ReadResponseHeader(r *rpc.Response) error {
	return bson.UnmarshalFromStream(cc.rwc, &ResponseBson{r})
}

// ReadResponseBody reads the body of server response
func (cc *ClientCodec) ReadResponseBody(body interface{}) error {
	return bson.UnmarshalFromStream(cc.rwc, body)
}

// Close closes the codec
func (cc *ClientCodec) Close() error {
	return cc.rwc.Close()
}

// ServerCodec holds required parameters for providing a server codec for
// bsonrpc
type ServerCodec struct {
	rwc io.ReadWriteCloser
	cw  *bytes2.ChunkedWriter
}

// NewServerCodec creates a new server codec for bsonrpc communication
func NewServerCodec(conn io.ReadWriteCloser) rpc.ServerCodec {
	return &ServerCodec{conn, bytes2.NewChunkedWriter(DefaultBufferSize)}
}

// ReadRequestHeader reads the header of the request
func (sc *ServerCodec) ReadRequestHeader(r *rpc.Request) error {
	return bson.UnmarshalFromStream(sc.rwc, &RequestBson{r})
}

// ReadRequestBody reads the body of the request
func (sc *ServerCodec) ReadRequestBody(body interface{}) error {
	return bson.UnmarshalFromStream(sc.rwc, body)
}

// WriteResponse send the response of the request to the client
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

// Close closes the codec
func (sc *ServerCodec) Close() error {
	return sc.rwc.Close()
}

// DialHTTP dials a HTTP endpoint with bsonrpc codec
func DialHTTP(network, address string, connectTimeout time.Duration, config *tls.Config) (*rpc.Client, error) {
	return rpcwrap.DialHTTP(network, address, codecName, NewClientCodec, connectTimeout, config)
}

// DialAuthHTTP dials a HTTP endpoint with bsonrpc codec as authentication enabled
func DialAuthHTTP(network, address, user, password string, connectTimeout time.Duration, config *tls.Config) (*rpc.Client, error) {
	return rpcwrap.DialAuthHTTP(network, address, user, password, codecName, NewClientCodec, connectTimeout, config)
}

// ServeRPC serves bsonrpc codec with the default rpc server
func ServeRPC() {
	rpcwrap.ServeRPC(codecName, NewServerCodec)
}

// ServeAuthRPC serves bsonrpc codec with the default authentication enabled rpc
// server
func ServeAuthRPC() {
	rpcwrap.ServeAuthRPC(codecName, NewServerCodec)
}

// ServeCustomRPC serves bsonrpc codec with a custom rpc server
func ServeCustomRPC(handler *http.ServeMux, server *rpc.Server, useAuth bool) {
	rpcwrap.ServeCustomRPC(handler, server, useAuth, codecName, NewServerCodec)
}
