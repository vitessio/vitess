// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcwrap

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"

	"code.google.com/p/vitess/go/relog"
	rpc "code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap/auth"
)

const (
	connected = "200 Connected to Go RPC"
)

type ClientCodecFactory func(conn io.ReadWriteCloser) rpc.ClientCodec

type BufferedConnection struct {
	*bufio.Reader
	io.WriteCloser
}

func NewBufferedConnection(conn io.ReadWriteCloser) *BufferedConnection {
	return &BufferedConnection{bufio.NewReader(conn), conn}
}

// DialHTTP connects to a go HTTP RPC server using the specified codec.
func DialHTTP(network, address, codecName string, cFactory ClientCodecFactory) (*rpc.Client, error) {
	var err error
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	io.WriteString(conn, "CONNECT "+GetRpcPath(codecName)+" HTTP/1.0\n\n")

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
	return nil, &net.OpError{"dial-http", network + " " + address, nil, err}
}

type ServerCodecFactory func(conn io.ReadWriteCloser) rpc.ServerCodec

// ServeRPC handles rpc requests using the hijack scheme of rpc
func ServeRPC(codecName string, cFactory ServerCodecFactory) {
	http.Handle(GetRpcPath(codecName), &rpcHandler{cFactory, false})
}

// ServeRPC handles rpc requests using the hijack scheme of rpc
func ServeAuthRPC(codecName string, cFactory ServerCodecFactory) {
	http.Handle(GetAuthRpcPath(codecName), &rpcHandler{cFactory, true})
}

// ServeHTTP handles rpc requests in HTTP compliant POST form
func ServeHTTP(codecName string, cFactory ServerCodecFactory) {
	http.Handle(GetHttpPath(codecName), &httpHandler{cFactory})
}

// AuthenticatedServer is an rpc.Server instance that serves
// authenticated calls.
var AuthenticatedServer = rpc.NewServer()

// RegisterAuthenticated registers a receiver with the authenticated
// rpc server.
func RegisterAuthenticated(rcvr interface{}) error {
	// TODO(szopa): This should be removed after the transition
	// period, when all the clients know about authentication.
	if err := rpc.Register(rcvr); err != nil {
		return err
	}
	return AuthenticatedServer.Register(rcvr)
}

// ServeCodec calls ServeCodec for the appropriate server
// (authenticated or default).
func (h *rpcHandler) ServeCodec(c rpc.ServerCodec) {
	if h.useAuth {
		AuthenticatedServer.ServeCodec(c)
	} else {
		rpc.ServeCodec(c)
	}
}

type rpcHandler struct {
	cFactory ServerCodecFactory
	useAuth  bool
}

func (h *rpcHandler) ServeHTTP(c http.ResponseWriter, req *http.Request) {
	conn, _, err := c.(http.Hijacker).Hijack()
	if err != nil {
		relog.Error("rpc hijacking %s: %v", req.RemoteAddr, err)
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	codec := h.cFactory(NewBufferedConnection(conn))

	if h.useAuth {
		if authenticated, err := auth.Authenticate(codec); !authenticated {
			if err != nil {
				relog.Error("authentication erred at %s: %v", req.RemoteAddr, err)
			}
			codec.Close()
			return
		}
	}

	h.ServeCodec(codec)
}

func GetRpcPath(codecName string) string {
	return "/_" + codecName + "_rpc_"
}

func GetAuthRpcPath(codecName string) string {
	return GetRpcPath(codecName) + "/auth"
}

type httpHandler struct {
	cFactory ServerCodecFactory
}

func (self *httpHandler) ServeHTTP(c http.ResponseWriter, req *http.Request) {
	conn := &httpConnectionBroker{c, req.Body}
	codec := self.cFactory(conn)
	if err := rpc.ServeRequest(codec); err != nil {
		relog.Error("rpcwrap: %v", err)
	}
}

// Emulate a read/write connection for the server codec
type httpConnectionBroker struct {
	http.ResponseWriter
	io.Reader
}

func (self *httpConnectionBroker) Close() error {
	return nil
}

func GetHttpPath(codecName string) string {
	return "/_" + codecName + "_http_"
}
