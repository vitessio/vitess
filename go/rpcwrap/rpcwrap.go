// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package rpcwrap provides wrappers for rpcplus package
package rpcwrap

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/stats"
)

const (
	connected = "200 Connected to Go RPC"
)

var (
	connCount    = stats.NewInt("connection-count")
	connAccepted = stats.NewInt("connection-accepted")
)

// ClientCodecFactory holds pattern for other client codec factories
type ClientCodecFactory func(conn io.ReadWriteCloser) rpc.ClientCodec

// BufferedConnection holds connection data for codecs
type BufferedConnection struct {
	isClosed bool
	*bufio.Reader
	io.WriteCloser
}

// NewBufferedConnection creates a new Buffered Connection
func NewBufferedConnection(conn io.ReadWriteCloser) *BufferedConnection {
	connCount.Add(1)
	connAccepted.Add(1)
	return &BufferedConnection{false, bufio.NewReader(conn), conn}
}

// Close closes the buffered connection
// FIXME(sougou/szopa): Find a better way to track connection count.
func (bc *BufferedConnection) Close() error {
	if !bc.isClosed {
		bc.isClosed = true
		connCount.Add(-1)
	}
	return bc.WriteCloser.Close()
}

// DialHTTP connects to a go HTTP RPC server using the specified codec.
// use 0 as connectTimeout for no timeout
func DialHTTP(network, address, codecName string, cFactory ClientCodecFactory, connectTimeout time.Duration) (*rpc.Client, error) {
	return dialHTTP(network, address, codecName, cFactory, connectTimeout)
}

func dialHTTP(network, address, codecName string, cFactory ClientCodecFactory, connectTimeout time.Duration) (*rpc.Client, error) {
	var err error
	var conn net.Conn
	if connectTimeout != 0 {
		conn, err = net.DialTimeout(network, address, connectTimeout)
	} else {
		conn, err = net.Dial(network, address)
	}
	if err != nil {
		return nil, err
	}

	_, err = io.WriteString(conn, "CONNECT "+GetRpcPath(codecName)+" HTTP/1.0\n\n")
	if err != nil {
		return nil, err
	}

	// Require successful HTTP response
	// before switching to RPC protocol.
	buffered := NewBufferedConnection(conn)
	resp, err := http.ReadResponse(buffered.Reader, &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return rpc.NewClientWithCodec(cFactory(buffered)), nil
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	conn.Close()
	return nil, &net.OpError{Op: "dial-http", Net: network + " " + address, Addr: nil, Err: err}
}

// ServerCodecFactory holds pattern for other server codec factories
type ServerCodecFactory func(conn io.ReadWriteCloser) rpc.ServerCodec

// ServeRPC handles rpc requests using the hijack scheme of rpc
func ServeRPC(codecName string, cFactory ServerCodecFactory) {
	http.Handle(GetRpcPath(codecName), &rpcHandler{cFactory, rpc.DefaultServer})
}

// ServeCustomRPC serves the given rpc requests with the provided ServeMux
func ServeCustomRPC(handler *http.ServeMux, server *rpc.Server, codecName string, cFactory ServerCodecFactory) {
	handler.Handle(GetRpcPath(codecName), &rpcHandler{cFactory, server})
}

// rpcHandler handles rpc queries for a 'CONNECT' method.
type rpcHandler struct {
	cFactory ServerCodecFactory
	server   *rpc.Server
}

// ServeHTTP implements http.Handler's ServeHTTP
func (h *rpcHandler) ServeHTTP(c http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		c.Header().Set("Content-Type", "text/plain; charset=utf-8")
		c.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(c, "405 must CONNECT\n")
		return
	}
	conn, _, err := c.(http.Hijacker).Hijack()
	if err != nil {
		log.Errorf("rpc hijacking %s: %v", req.RemoteAddr, err)
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	codec := h.cFactory(NewBufferedConnection(conn))
	ctx := proto.NewContext(req.RemoteAddr)
	h.server.ServeCodecWithContext(ctx, codec)
}

// GetRpcPath returns the toplevel path used for serving RPCs over HTTP
func GetRpcPath(codecName string) string {
	return "/_" + codecName + "_rpc_"
}

// ServeHTTPRPC serves the given http rpc requests with the provided ServeMux
func ServeHTTPRPC(
	handler *http.ServeMux,
	server *rpc.Server,
	codecName string,
	cFactory ServerCodecFactory,
	contextCreator func(*http.Request) context.Context) {

	handler.Handle(
		GetRpcPath(codecName),
		&httpRPCHandler{
			cFactory:       cFactory,
			server:         server,
			contextCreator: contextCreator,
		},
	)
}

// httpRPCHandler handles rpc queries for a all types of HTTP requests, does not
// maintain a persistent connection.
type httpRPCHandler struct {
	cFactory ServerCodecFactory
	server   *rpc.Server
	// contextCreator creates an application specific context, while creating
	// the context it should not read the request body nor write anything to
	// headers
	contextCreator func(*http.Request) context.Context
}

// ServeHTTP implements http.Handler's ServeHTTP
func (h *httpRPCHandler) ServeHTTP(c http.ResponseWriter, req *http.Request) {
	codec := h.cFactory(&httpReadWriteCloser{rw: c, req: req})

	var ctx context.Context

	if h.contextCreator != nil {
		ctx = h.contextCreator(req)
	} else {
		ctx = proto.NewContext(req.RemoteAddr)
	}

	h.server.ServeRequestWithContext(
		ctx,
		codec,
	)

	codec.Close()
}

// httpReadWriteCloser wraps http.ResponseWriter and http.Request, with the help
// of those, implements ReadWriteCloser interface
type httpReadWriteCloser struct {
	rw  http.ResponseWriter
	req *http.Request
}

// Read implements Reader interface
func (i *httpReadWriteCloser) Read(p []byte) (n int, err error) {
	return i.req.Body.Read(p)
}

// Write implements Writer interface
func (i *httpReadWriteCloser) Write(p []byte) (n int, err error) {
	return i.rw.Write(p)
}

// Close implements Closer interface
func (i *httpReadWriteCloser) Close() error {
	return i.req.Body.Close()
}
