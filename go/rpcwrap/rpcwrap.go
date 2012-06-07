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
	"net/rpc"

	"code.google.com/p/vitess/go/relog"
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
	http.Handle(GetRpcPath(codecName), &rpcHandler{cFactory})
}

// ServeHTTP handles rpc requests in HTTP compliant POST form
func ServeHTTP(codecName string, cFactory ServerCodecFactory) {
	http.Handle(GetHttpPath(codecName), &httpHandler{cFactory})
}

type rpcHandler struct {
	cFactory ServerCodecFactory
}

func (self *rpcHandler) ServeHTTP(c http.ResponseWriter, req *http.Request) {
	conn, _, err := c.(http.Hijacker).Hijack()
	if err != nil {
		relog.Error("rpc hijacking %s: %v", req.RemoteAddr, err)
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	rpc.ServeCodec(self.cFactory(NewBufferedConnection(conn)))
}

func GetRpcPath(codecName string) string {
	return "/_" + codecName + "_rpc_"
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
