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
	"time"

	"code.google.com/p/vitess/go/relog"
	rpc "code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap/auth"
	"code.google.com/p/vitess/go/rpcwrap/proto"
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
// use 0 as connectTimeout for no timeout
func DialHTTP(network, address, codecName string, cFactory ClientCodecFactory, connectTimeout time.Duration) (*rpc.Client, error) {
	return dialHTTP(network, address, codecName, cFactory, false, connectTimeout)
}

// DialAuthHTTP connects to an authenticated go HTTP RPC server using
// the specified codec and credentials.
// use 0 as connectTimeout for no timeout
func DialAuthHTTP(network, address, user, password, codecName string, cFactory ClientCodecFactory, connectTimeout time.Duration) (conn *rpc.Client, err error) {
	if conn, err = dialHTTP(network, address, codecName, cFactory, true, connectTimeout); err != nil {
		return
	}
	reply := new(auth.GetNewChallengeReply)
	if err = conn.Call("AuthenticatorCRAMMD5.GetNewChallenge", "", reply); err != nil {
		return
	}
	proof := auth.CRAMMD5GetExpected(user, password, reply.Challenge)

	if err = conn.Call(
		"AuthenticatorCRAMMD5.Authenticate",
		auth.AuthenticateRequest{Proof: proof}, new(auth.AuthenticateReply)); err != nil {
		return
	}
	return
}

func dialHTTP(network, address, codecName string, cFactory ClientCodecFactory, auth bool, connectTimeout time.Duration) (*rpc.Client, error) {
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

	io.WriteString(conn, "CONNECT "+GetRpcPath(codecName, auth)+" HTTP/1.0\n\n")

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
	http.Handle(GetRpcPath(codecName, false), &rpcHandler{cFactory, false})
}

// ServeRPC handles rpc requests using the hijack scheme of rpc
func ServeAuthRPC(codecName string, cFactory ServerCodecFactory) {
	http.Handle(GetRpcPath(codecName, true), &rpcHandler{cFactory, true})
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
func (h *rpcHandler) ServeCodecWithContext(c rpc.ServerCodec, context *proto.Context) {
	if h.useAuth {
		AuthenticatedServer.ServeCodecWithContext(c, context)
	} else {
		rpc.ServeCodecWithContext(c, context)
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
	context := &proto.Context{RemoteAddr: req.RemoteAddr}
	if h.useAuth {
		if authenticated, err := auth.Authenticate(codec, context); !authenticated {
			if err != nil {
				relog.Error("authentication erred at %s: %v", req.RemoteAddr, err)
			}
			codec.Close()
			return
		}
	}
	h.ServeCodecWithContext(codec, context)
}

func GetRpcPath(codecName string, auth bool) string {
	path := "/_" + codecName + "_rpc_"
	if auth {
		path += "/auth"
	}
	return path
}

type httpHandler struct {
	cFactory ServerCodecFactory
}

func (hh *httpHandler) ServeHTTP(c http.ResponseWriter, req *http.Request) {
	conn := &httpConnectionBroker{c, req.Body}
	codec := hh.cFactory(conn)
	if err := rpc.ServeRequestWithContext(codec, &proto.Context{RemoteAddr: req.RemoteAddr}); err != nil {
		relog.Error("rpcwrap: %v", err)
	}
}

// Emulate a read/write connection for the server codec
type httpConnectionBroker struct {
	http.ResponseWriter
	io.Reader
}

func (*httpConnectionBroker) Close() error {
	return nil
}

func GetHttpPath(codecName string) string {
	return "/_" + codecName + "_http_"
}
